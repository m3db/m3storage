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

package conn

import (
	"errors"
	"sync"

	"github.com/m3db/m3storage/cluster"
	"github.com/m3db/m3x/close"
	"github.com/m3db/m3x/log"

	"github.com/facebookgo/clock"
	"github.com/golang/protobuf/proto"
)

var (
	errClosed                 = errors.New("closed")
	errStorageTypeUnsupported = errors.New("unsupported storage type")

	errManagerLogRequired      = errors.New("logger required")
	errManagerDriversRequired  = errors.New("at least 1 driver required")
	errManagerProviderRequired = errors.New("provider required")
)

// ManagerOptions are creation time options for the Manager
type ManagerOptions interface {
	// Logger is the logger to use
	Logger(log xlog.Logger) ManagerOptions
	GetLogger() xlog.Logger

	// Provider provides information about active cnames
	Provider(p cluster.Provider) ManagerOptions
	GetProvider() cluster.Provider

	// Drivers are the set of drivers used by this Manager
	Drivers(d []Driver) ManagerOptions
	GetDrivers() []Driver

	// Validate validates the options
	Validate() error
}

// NewManagerOptions returns new empty ManagerOptions
func NewManagerOptions() ManagerOptions {
	return managerOptions{
		log: xlog.NullLogger,
	}
}

// The Manager manages connections to tsdb cnames, creating them as
// needed and keeping them up to date with configuration changes
type Manager interface {
	xclose.Closer

	// GetConn retrieves the connection to the given dbname and cname
	GetConn(dbname, cname string) (Conn, error)
}

// NewManager creates a new connection manager based on the provided
// options
func NewManager(opts ManagerOptions) (Manager, error) {
	if opts == nil {
		opts = NewManagerOptions()
	}

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	drivers := make(map[string]Driver, len(opts.GetDrivers()))
	for _, driver := range opts.GetDrivers() {
		drivers[driver.Type().Name()] = driver
	}

	return &manager{
		log:      opts.GetLogger(),
		conns:    make(map[string]Conn),
		pending:  make(map[string]<-chan struct{}),
		watches:  make(map[string]cluster.Watch),
		drivers:  drivers,
		provider: opts.GetProvider(),
	}, nil
}

type manager struct {
	sync.RWMutex

	log      xlog.Logger
	conns    map[string]Conn
	pending  map[string]<-chan struct{}
	watches  map[string]cluster.Watch
	drivers  map[string]Driver
	provider cluster.Provider
	closed   bool
	wg       sync.WaitGroup
}

// GetConn retrieves a connection to the given dbname and cname
func (cm *manager) GetConn(dbname, cname string) (Conn, error) {
	cm.log.Debugf("retrieving connection for %s:%s", dbname, cname)
	for {
		if conn, err := cm.getExistingConn(dbname, cname); conn != nil || err != nil {
			return conn, err
		}

		if conn, err := cm.tryOpenConn(dbname, cname); conn != nil || err != nil {
			return conn, err
		}
	}
}

// Close closes the connection manager
func (cm *manager) Close() error {
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

// getExistingConn attmepts to return an existing open connection to the dbname cname
func (cm *manager) getExistingConn(dbname, cname string) (Conn, error) {
	key := cluster.FmtKey(dbname, cname)

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

// tryOpenConn tries to create a new connection to the dbname cname
func (cm *manager) tryOpenConn(dbname, cname string) (Conn, error) {
	key := cluster.FmtKey(dbname, cname)

	cm.log.Infof("attempting to create connection to %s:%s", dbname, cname)

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

	// Get the current cname configuration and watch for changes
	cw, err := cm.provider.WatchCluster(dbname, cname)
	if err != nil {
		cm.log.Errorf("failed to watch cname config for %s:%s: %v", dbname, cname, err)
		return nil, err
	}
	<-cw.C()
	cl := cw.Get()

	// create the connection using the cname configuration
	conn, err := cm.openConn(cl)
	if err != nil {
		cm.log.Errorf("failed to open connection for %s:%s: %v", dbname, cname, err)
		cw.Close()
		return nil, err
	}

	// register the connection and monitor the cluter for updates
	if err := cm.registerConn(cl, cw, conn); err != nil {
		return nil, err
	}

	return conn, nil
}

// openConn opens a connection to the given cname
func (cm *manager) openConn(cl cluster.Cluster) (Conn, error) {
	d := cm.drivers[cl.Type().Name()]
	if d == nil {
		cm.log.Errorf("unsupported driver type %s for %s:%s",
			cl.Type().Name(), cl.Database(), cl.Name())
		return nil, errStorageTypeUnsupported
	}

	cfg := proto.Clone(d.ConfigType())
	if err := cl.Config().Unmarshal(cfg); err != nil {
		cm.log.Errorf("failed to unmarshal config v%d for %s:%s: %v",
			cl.Config().Version(), cl.Database(), cl.Name(), err)
		return nil, err
	}

	return d.Open(cfg)
}

// registerConn registers a newly opened connection to the given dbname cname
func (cm *manager) registerConn(cl cluster.Cluster, cw cluster.Watch, conn Conn) error {
	key := cl.Key()

	cm.Lock()
	defer cm.Unlock()

	if cm.closed {
		cw.Close()
		conn.Close()
		return errClosed
	}

	// Register the watch and connection
	cm.conns[key], cm.watches[key] = conn, cw

	// Spin up a goroutine to monitor the cname for updates
	cm.wg.Add(1)
	go cm.watchCluster(cl, cw, conn)

	return nil
}

// watchCluster monitors the given cname for config updates
func (cm *manager) watchCluster(cl cluster.Cluster, cw cluster.Watch, conn Conn) {
	cm.log.Infof("watching %s:%s", cl.Database(), cl.Name())
	for range cw.C() {
		cm.reconfigureConn(cw.Get(), conn)
	}

	cm.cleanupConn(cl.Database(), cl.Name())
	cm.wg.Done()
}

// reconfigureConn reconfigures a connection with new cname configuration
func (cm *manager) reconfigureConn(cl cluster.Cluster, existing Conn) error {
	key := cl.Key()
	d := cm.drivers[cl.Type().Name()]
	if d == nil {
		cm.log.Errorf("received update v%d for %s:%s: unsupported storage type %s",
			cl.Config().Version(), cl.Database(), cl.Name(), cl.Type().Name())
		return errStorageTypeUnsupported
	}

	cm.log.Infof("received update v%d for %s:%s", cl.Config().Version(), cl.Database(), cl.Name())

	cfg := proto.Clone(d.ConfigType())
	if err := cl.Config().Unmarshal(cfg); err != nil {
		// Don't bail out, just log and metric
		cm.log.Errorf("invalid config v%d for %s:%s: %v",
			cl.Config().Version(), cl.Database(), cl.Name(), err)
		return err
	}

	newConn, err := d.Reconfigure(existing, cfg)
	if err != nil {
		cm.log.Errorf("could not reconfigure conn for %s:%d v%d: %v",
			cl.Database(), cl.Name(), cl.Config().Version(), err)
		return err
	}

	cm.Lock()
	cm.conns[key] = newConn
	cm.Unlock()
	return nil
}

// cleanupConn cleans up the connection to the given dbname and cname
func (cm *manager) cleanupConn(dbname, cname string) {
	cm.log.Infof("cleaning up connection to %s:%s", dbname, cname)

	key := cluster.FmtKey(dbname, cname)

	cm.Lock()
	conn := cm.conns[key]
	delete(cm.conns, key)
	delete(cm.watches, key)
	cm.Unlock()

	if conn != nil {
		conn.Close()
	}
}

type managerOptions struct {
	log   xlog.Logger
	clock clock.Clock
	p     cluster.Provider
	d     []Driver
}

func (opts managerOptions) Validate() error {
	if opts.log == nil {
		return errManagerLogRequired
	}

	if len(opts.d) == 0 {
		return errManagerDriversRequired
	}

	if opts.p == nil {
		return errManagerProviderRequired
	}

	return nil
}

func (opts managerOptions) GetLogger() xlog.Logger { return opts.log }
func (opts managerOptions) GetDrivers() []Driver   { return opts.d }
func (opts managerOptions) GetProvider() cluster.Provider {
	return opts.p
}

func (opts managerOptions) Logger(log xlog.Logger) ManagerOptions {
	opts.log = log
	return opts
}

func (opts managerOptions) Provider(p cluster.Provider) ManagerOptions {
	opts.p = p
	return opts
}

func (opts managerOptions) Drivers(d []Driver) ManagerOptions {
	opts.d = d
	return opts
}
