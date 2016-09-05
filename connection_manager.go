// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package storage

import (
	"errors"
	"fmt"
	"sync"

	"github.com/m3db/m3x/close"
	"github.com/m3db/m3x/log"

	"github.com/facebookgo/clock"
	"github.com/golang/protobuf/proto"
)

var (
	errClosed          = errors.New("closed")
	errTypeUnsupported = errors.New("unsupported storage type")
)

// ConnectionManagerOptions are creation time options for the ConnectionManager
type ConnectionManagerOptions interface {
	// Logger is the logger to use
	Logger(log xlog.Logger) ConnectionManagerOptions
	GetLogger() xlog.Logger

	// ClusterMappingProvider provides information about active clusters
	ClusterMappingProvider(p ClusterMappingProvider) ConnectionManagerOptions
	GetClusterMappingProvider() ClusterMappingProvider

	// Drivers are the set of drivers used by this ConnectionManager
	Drivers(d []Driver) ConnectionManagerOptions
	GetDrivers() []Driver
}

// NewConnectionManagerOptions returns new empty ConnectionManagerOptions
func NewConnectionManagerOptions() ConnectionManagerOptions { return connectionManagerOptions{} }

// The ConnectionManager manages connections to tsdb clusters, creating them as
// needed and keeping them up to date with configuration changes
type ConnectionManager interface {
	xclose.Closer

	// GetConnection retrieves the connection to the given database and cluster
	GetConnection(database, cluster string) (Connection, error)
}

func NewConnectionManager(opts ConnectionManagerOptions) (ConnectionManager, error) {
	if opts == nil {
		opts = NewConnectionManagerOptions()
	}

	log := opts.GetLogger()
	if log == nil {
		log = xlog.NullLogger
	}

	d := opts.GetDrivers()
	if len(d) == 0 {
		panic("at least one driver must be registered")
	}
	drivers := make(map[string]Driver, len(d))
	for _, driver := range d {
		drivers[driver.Type().Name()] = driver
	}

	p := opts.GetClusterMappingProvider()
	if p == nil {
		panic("ClusterMappingProvider must be specified")
	}

	return &connectionManager{
		log:      log,
		conns:    make(map[string]Connection),
		pending:  make(map[string]<-chan struct{}),
		watches:  make(map[string]ClusterWatch),
		drivers:  drivers,
		provider: p,
	}, nil
}

type connectionManager struct {
	sync.RWMutex

	log      xlog.Logger
	conns    map[string]Connection
	pending  map[string]<-chan struct{}
	watches  map[string]ClusterWatch
	drivers  map[string]Driver
	provider ClusterMappingProvider
	closed   bool
	wg       sync.WaitGroup
}

// GetConnection retrieves a connection to the given database and cluster
func (cm *connectionManager) GetConnection(database, cluster string) (Connection, error) {
	cm.log.Debugf("retrieving connection for %s:%s", database, cluster)
	for {
		if conn, err := cm.getExistingConnection(database, cluster); conn != nil || err != nil {
			return conn, err
		}

		if conn, err := cm.tryCreateConnection(database, cluster); conn != nil || err != nil {
			return conn, err
		}
	}
}

// Close closes the connection manager
func (cm *connectionManager) Close() error {
	cm.Lock()
	if cm.closed {
		cm.Unlock()
		return nil
	}

	cm.log.Infof("closing connection manager")

	// Mark ourselves as closed
	cm.closed = true

	// Close all watches.  The watch goroutine will take care of cleaning up
	// connections and the watch map entry
	for _, cw := range cm.watches {
		cw.Close()
	}

	// Wait for watch goroutines to exit
	cm.Unlock()
	cm.wg.Wait()
	return nil
}

// getExistingConnection attmepts to return an existing open connection to the database cluster
func (cm *connectionManager) getExistingConnection(database, cluster string) (Connection, error) {
	key := fmtConnKey(database, cluster)

	cm.RLock()
	defer cm.RUnlock()

	// If we're closed, bail out
	if cm.closed {
		return nil, errClosed
	}

	// If there is already a connection, return it
	if c := cm.conns[key]; c != nil {
		return c, nil
	}

	// No connection, no error - try to create it
	return nil, nil
}

// tryCreateConnection tries to create a new connection to the database cluster
func (cm *connectionManager) tryCreateConnection(database, cluster string) (Connection, error) {
	key := fmtConnKey(database, cluster)

	cm.log.Infof("attempting to create connection to %s:%s", database, cluster)

	cm.Lock()

	// If someone closed us while we were upgrading the lock, bail out
	if cm.closed {
		return nil, errClosed
	}

	// If someone else created it while we were upgrading the lock, return the
	// new connection
	if c := cm.conns[key]; c != nil {
		cm.Unlock()
		return c, nil
	}

	// If someone is in the process of creating the connection, wait until they
	// are done and try to retrieve it again
	if p := cm.pending[key]; p != nil {
		cm.Unlock()
		<-p
		return nil, nil
	}

	// Mark that we are creating the connection, so that we can release the lock
	// and readers for other connections pass through while this connection is
	// being established, without risking that we might create the connection
	// more than once
	p := make(chan struct{})
	cm.pending[key] = p
	cm.Unlock()

	// Regardless of whether we succeed or fail, release anyone waiting
	// for us to be done with the attempt to create
	defer func() {
		cm.Lock()
		delete(cm.pending, key)
		cm.Unlock()
		close(p)
	}()

	// Get the current cluster configuration and watch for changes
	cw, err := cm.provider.WatchCluster(database, cluster)
	if err != nil {
		cm.log.Errorf("failed to watch cluster config for %s:%s: %v", database, cluster, err)
		return nil, err
	}
	<-cw.C()
	cl := cw.Get()

	// create the connection using the cluster configuration
	conn, err := cm.openConnection(cl)
	if err != nil {
		cw.Close()
		return nil, err
	}

	// register the connection and monitor the cluter for updates
	if err := cm.registerConnection(cl, cw, conn); err != nil {
		return nil, err
	}

	return conn, nil
}

// openConnection opens a connection to the given cluster
func (cm *connectionManager) openConnection(cl Cluster) (Connection, error) {
	d := cm.drivers[cl.Type().Name()]
	if d == nil {
		cm.log.Errorf("unsupported driver type %s for %s:%s",
			cl.Type().Name(), cl.Database(), cl.Name())
		return nil, errTypeUnsupported
	}

	cfg := proto.Clone(d.ConfigType())
	if err := cl.Config().Unmarshal(cfg); err != nil {
		cm.log.Errorf("failed to unmarshal config v%d for %s:%s: %v",
			cl.Config().Version(), cl.Database(), cl.Name(), err)
		return nil, err
	}

	return d.OpenConnection(cfg)
}

// registerConnection registers a newly opened connection to the given database cluster
func (cm *connectionManager) registerConnection(cl Cluster, cw ClusterWatch, conn Connection) error {
	key := fmtConnKey(cl.Database(), cl.Name())

	cm.Lock()
	defer cm.Unlock()

	if cm.closed {
		cw.Close()
		conn.Close()
		return errClosed
	}

	// Register the watch and connection
	cm.conns[key], cm.watches[key] = conn, cw

	// Spin up a goroutine to monitor the cluster for updates
	go cm.watchCluster(cl, cw, conn)
	cm.wg.Add(1)

	return nil
}

// watchCluster monitors the given cluster for config updates
func (cm *connectionManager) watchCluster(cl Cluster, cw ClusterWatch, conn Connection) {

	var (
		key = fmtConnKey(cl.Database(), cl.Name())
		d   = cm.drivers[cl.Type().Name()]
	)

	for range cw.C() {
		cl = cw.Get()
		cm.log.Infof("received update v%d for %s:%s", cl.Config().Version(), cl.Database(), cl.Name())

		cfg := proto.Clone(d.ConfigType())
		if err := cl.Config().Unmarshal(cfg); err != nil {
			// Don't bail out, just log and metric
			cm.log.Errorf("invalid config v%d for %s:%s: %v",
				cl.Config().Version(), cl.Database(), cl.Name(), err)
			continue
		}

		newConn, err := d.ReconfigureConnection(conn, cfg)
		if err != nil {
			cm.log.Errorf("could not reconfigure conn for %s:%d v%d: %v",
				cl.Database(), cl.Name(), cl.Config().Version(), err)
			continue
		}

		cm.Lock()
		cm.conns[key] = newConn
		cm.Unlock()
	}

	// Cleanup watch and connection, and let the closing goroutine know that we are done
	cm.log.Infof("cleaning up connection to %s:%s", cl.Database(), cl.Name())
	cm.Lock()
	conn = cm.conns[key]
	delete(cm.conns, key)
	delete(cm.watches, key)
	cm.Unlock()

	conn.Close()
	cm.wg.Done()
}

type connectionManagerOptions struct {
	log   xlog.Logger
	clock clock.Clock
	p     ClusterMappingProvider
	d     []Driver
}

func (opts connectionManagerOptions) GetLogger() xlog.Logger { return opts.log }
func (opts connectionManagerOptions) GetDrivers() []Driver   { return opts.d }
func (opts connectionManagerOptions) GetClusterMappingProvider() ClusterMappingProvider {
	return opts.p
}

func (opts connectionManagerOptions) Logger(log xlog.Logger) ConnectionManagerOptions {
	opts.log = log
	return opts
}

func (opts connectionManagerOptions) ClusterMappingProvider(p ClusterMappingProvider) ConnectionManagerOptions {
	opts.p = p
	return opts
}

func (opts connectionManagerOptions) Drivers(d []Driver) ConnectionManagerOptions {
	opts.d = d
	return opts
}

func fmtConnKey(database, cluster string) string { return fmt.Sprintf("%s:%s", database, cluster) }
