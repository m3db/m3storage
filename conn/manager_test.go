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
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3storage/cluster"
	"github.com/m3db/m3storage/generated/proto/configtest"
	"github.com/m3db/m3storage/retention"
	"github.com/m3db/m3storage/ts"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestNewManagerErrors(t *testing.T) {
	d, p, _, _ := initManagerTest(t, nil)

	tests := []struct {
		opts ManagerOptions
		err  error
	}{
		{NewManagerOptions().Logger(nil).Drivers([]Driver{d}).Provider(p),
			errManagerLogRequired},
		{NewManagerOptions().Provider(p),
			errManagerDriversRequired},
		{NewManagerOptions().Drivers([]Driver{d}),
			errManagerProviderRequired},
	}

	for _, test := range tests {
		m, err := NewManager(test.opts)
		require.Equal(t, test.err, err)
		require.Nil(t, m)
	}
}

func TestManagerGetConn(t *testing.T) {
	d, _, m, updateCh := initManagerTest(t, nil)

	updateCh <- cluster.NewCluster("c1", fakeStorageType, "wow",
		cluster.NewConfig(1, mustMarshal(&configtest.TestConfig{
			Hosts: []string{"h1", "h2", "h3"},
		})))

	d.unblockOpen()

	conn, err := m.GetConn("wow", "c1")
	require.NoError(t, err)
	fconn, ok := conn.(*managerTestConn)
	require.True(t, ok)
	require.Equal(t, []string{"h1", "h2", "h3"}, fconn.TestConfig.Hosts)
	require.Equal(t, uint32(1), d.opens)
}

func TestManagerConcurrentGetNewConn(t *testing.T) {
	d, _, m, updateCh := initManagerTest(t, nil)

	updateCh <- cluster.NewCluster("c1", fakeStorageType, "wow",
		cluster.NewConfig(1, mustMarshal(&configtest.TestConfig{
			Hosts: []string{"h1", "h2", "h3"},
		})))

	connCh := make(chan Conn, 5)
	errorCh := make(chan error, 5)
	var wg sync.WaitGroup

	requestConn := func() {
		defer wg.Done()

		conn, err := m.GetConn("wow", "c1")
		if err != nil {
			errorCh <- err
			return
		}

		connCh <- conn
	}

	// Spin up a goroutine in the background to get a connection.  This will
	// block at the driver.  As soon as the first goroutine enters the driver,
	// ask for another connection and then after a brief interval unblock the
	// driver

	wg.Add(1)
	go requestConn()

	d.waitForOpen()

	wg.Add(1)
	go requestConn()

	time.Sleep(time.Millisecond * 500)
	d.unblockOpen()

	// Both go-routines should return the same connection and no error, and the
	// driver should only have been called once
	wg.Wait()
	close(connCh)
	close(errorCh)

	if err := <-errorCh; err != nil {
		require.NoError(t, err)
	}

	conn1 := <-connCh
	conn2 := <-connCh

	require.NotNil(t, conn1)
	require.NotNil(t, conn2)
	require.Equal(t, conn1, conn2)
	require.Equal(t, uint32(1), d.opens)
}

func TestManagerGetConnWatchTimeout(t *testing.T) {
	d, _, m, _ := initManagerTest(t, NewManagerOptions().WatchTimeout(time.Millisecond*100))
	d.unblockOpen()

	conn, err := m.GetConn("wow", "c1")
	require.Equal(t, errTimeout, err)
	require.Nil(t, conn)
}

func TestManagerGetConnConcurrentRetrieveDifferentConnts(t *testing.T) {
	d, _, m, updateCh := initManagerTest(t, NewManagerOptions().WatchTimeout(time.Hour))
	d.unblockOpen()

	// In the background, request access to a cluster which doesn't yet have
	// config.  This will block
	connCh, errorCh := make(chan Conn, 5), make(chan error, 5)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := m.GetConn("wow", "c1")
		if err != nil {
			errorCh <- err
			return
		}

		connCh <- conn
	}()

	// Request access to a different connection on the same database.  This
	// should succeed even though there is a goroutine blocked on retrieving
	// another cluster
	updateCh <- cluster.NewCluster("c2", fakeStorageType, "wow",
		cluster.NewConfig(1, mustMarshal(&configtest.TestConfig{
			Hosts: []string{"h4"}})))

	conn1, err := m.GetConn("wow", "c2")
	require.NoError(t, err)
	require.NotNil(t, conn1)

	fconn1 := conn1.(*managerTestConn)
	require.Equal(t, []string{"h4"}, fconn1.TestConfig.Hosts)

	// Make the first cluster available. This will unblock the retrieval of
	// the first connection
	updateCh <- cluster.NewCluster("c1", fakeStorageType, "wow",
		cluster.NewConfig(1, mustMarshal(&configtest.TestConfig{
			Hosts: []string{"h1", "h2", "h3"}})))

	wg.Wait()
	close(connCh)
	close(errorCh)

	require.NoError(t, <-errorCh)
	conn2 := <-connCh
	require.NotNil(t, conn2)
	fconn2 := conn2.(*managerTestConn)
	require.Equal(t, []string{"h1", "h2", "h3"}, fconn2.TestConfig.Hosts)
}

func TestManagerGetConnDriverFails(t *testing.T) {
	errDriver := errors.New("driver failure")
	d, _, m, updateCh := initManagerTest(t, nil)

	updateCh <- cluster.NewCluster("c1", fakeStorageType, "wow",
		cluster.NewConfig(1, mustMarshal(&configtest.TestConfig{
			Hosts: []string{"h1", "h2", "h3"},
		})))

	d.failOpen(errDriver)
	d.unblockOpen()

	conn, err := m.GetConn("wow", "c1")
	require.Equal(t, errDriver, err)
	require.Nil(t, conn)
}

func TestManagerGetConnClosedManager(t *testing.T) {
	_, _, m, updateCh := initManagerTest(t, nil)

	updateCh <- cluster.NewCluster("c1", fakeStorageType, "wow",
		cluster.NewConfig(1, mustMarshal(&configtest.TestConfig{
			Hosts: []string{"h1", "h2", "h3"},
		})))

	m.Close()

	conn, err := m.GetConn("wow", "c1")
	require.Equal(t, errClosed, err)
	require.Nil(t, conn)
}

func TestManagerGetConnUnsupportedType(t *testing.T) {
	d, _, m, updateCh := initManagerTest(t, nil)

	updateCh <- cluster.NewCluster("c1", cluster.NewType("unknown-type"), "wow",
		cluster.NewConfig(1, mustMarshal(&configtest.TestConfig{
			Hosts: []string{"h1", "h2", "h3"},
		})))

	d.unblockOpen()

	conn, err := m.GetConn("wow", "c1")
	require.Equal(t, errStorageTypeUnsupported, err)
	require.Nil(t, conn)
}

func TestManagerGetManagerClosedWhileDriverWorking(t *testing.T) {
	d, _, m, updateCh := initManagerTest(t, nil)

	updateCh <- cluster.NewCluster("c1", fakeStorageType, "wow",
		cluster.NewConfig(1, mustMarshal(&configtest.TestConfig{
			Hosts: []string{"h1", "h2", "h3"},
		})))

	// Spin up a background goroutine to request the connection. This will
	// block in the driver until we're ready to let it run
	connCh := make(chan Conn, 5)
	errorCh := make(chan error, 5)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		conn, err := m.GetConn("wow", "c1")
		if err != nil {
			errorCh <- err
			return
		}

		connCh <- conn
	}()

	// Once the background goroutine has entered the driver, close the
	// connection manager and unblock the driver
	d.waitForOpen()
	m.Close()
	d.unblockOpen()

	// We should receive a closed error from the create
	wg.Wait()
	close(connCh)
	close(errorCh)

	require.Equal(t, errClosed, <-errorCh)
	require.Nil(t, <-connCh)
}

func TestManagerGetConnUnmarshalConfigError(t *testing.T) {
	_, _, m, updateCh := initManagerTest(t, nil)

	updateCh <- cluster.NewCluster("c1", fakeStorageType, "wow",
		cluster.NewConfig(1, []byte("invalid-proto")))

	conn, err := m.GetConn("wow", "c1")
	require.Error(t, err)
	require.Nil(t, conn)
}

func TestManagerReconfigureCluster(t *testing.T) {
	d, _, m, updateCh := initManagerTest(t, nil)

	updateCh <- cluster.NewCluster("c1", fakeStorageType, "wow",
		cluster.NewConfig(1, mustMarshal(&configtest.TestConfig{
			Hosts: []string{"h1", "h2", "h3"},
		})))

	// Get the initial connection
	d.unblockOpen()
	conn, err := m.GetConn("wow", "c1")
	require.NoError(t, err)

	fconn, ok := conn.(*managerTestConn)
	require.True(t, ok)
	require.Equal(t, []string{"h1", "h2", "h3"}, fconn.TestConfig.Hosts)

	// Update the connection, then wait a little bit for the watch goroutine to fire
	updateCh <- cluster.NewCluster("c1", fakeStorageType, "wow",
		cluster.NewConfig(2, mustMarshal(&configtest.TestConfig{
			Hosts: []string{"h4"},
		})))

	time.Sleep(time.Millisecond * 150)

	// Get the connection, should return the updated connection
	conn, err = m.GetConn("wow", "c1")
	require.NoError(t, err)

	fconn, ok = conn.(*managerTestConn)
	require.True(t, ok)
	require.Equal(t, []string{"h4"}, fconn.TestConfig.Hosts)

	// The driver should have open and reconfigure called appropriately
	require.Equal(t, uint32(1), d.opens)
	require.Equal(t, uint32(1), d.reconfigures)
}

func TestManagerReconfigureClusterDriverFails(t *testing.T) {
	d, _, m, updateCh := initManagerTest(t, nil)

	updateCh <- cluster.NewCluster("c1", fakeStorageType, "wow",
		cluster.NewConfig(1, mustMarshal(&configtest.TestConfig{
			Hosts: []string{"h1", "h2", "h3"},
		})))

	// Get the initial connection
	d.unblockOpen()
	conn, err := m.GetConn("wow", "c1")
	require.NoError(t, err)

	fconn, ok := conn.(*managerTestConn)
	require.True(t, ok)
	require.Equal(t, []string{"h1", "h2", "h3"}, fconn.TestConfig.Hosts)

	// Update the connection, then wait a little bit for the watch goroutine to fire
	d.failOpen(errors.New("bad driver"))
	updateCh <- cluster.NewCluster("c1", fakeStorageType, "wow",
		cluster.NewConfig(2, mustMarshal(&configtest.TestConfig{
			Hosts: []string{"h4"},
		})))

	time.Sleep(time.Millisecond * 150)

	// Get the connection, should return the old connection
	conn, err = m.GetConn("wow", "c1")
	require.NoError(t, err)

	fconn, ok = conn.(*managerTestConn)
	require.True(t, ok)
	require.Equal(t, []string{"h1", "h2", "h3"}, fconn.TestConfig.Hosts)

	// The driver should have open and reconfigure called appropriately
	require.Equal(t, uint32(1), d.opens)
	require.Equal(t, uint32(1), d.reconfigures)
}

func TestManagerReconfigureClusterUnmarshalError(t *testing.T) {
	d, _, m, updateCh := initManagerTest(t, nil)

	updateCh <- cluster.NewCluster("c1", fakeStorageType, "wow",
		cluster.NewConfig(1, mustMarshal(&configtest.TestConfig{
			Hosts: []string{"h1", "h2", "h3"},
		})))

	// Get the initial connection
	d.unblockOpen()
	conn, err := m.GetConn("wow", "c1")
	require.NoError(t, err)

	fconn, ok := conn.(*managerTestConn)
	require.True(t, ok)
	require.Equal(t, []string{"h1", "h2", "h3"}, fconn.TestConfig.Hosts)

	// Update the connection, then wait a little bit for the watch goroutine to fire
	updateCh <- cluster.NewCluster("c1", fakeStorageType, "wow",
		cluster.NewConfig(2, []byte("invalid-proto")))

	time.Sleep(time.Millisecond * 150)

	// Get the connection, should return the old connection
	conn, err = m.GetConn("wow", "c1")
	require.NoError(t, err)

	fconn, ok = conn.(*managerTestConn)
	require.True(t, ok)
	require.Equal(t, []string{"h1", "h2", "h3"}, fconn.TestConfig.Hosts)

	// The driver should have open called, but not reconfigure
	require.Equal(t, uint32(1), d.opens)
	require.Equal(t, uint32(0), d.reconfigures)
}

func TestManagerReconfigureClusterUnsupportedType(t *testing.T) {
	d, _, m, updateCh := initManagerTest(t, nil)

	updateCh <- cluster.NewCluster("c1", fakeStorageType, "wow",
		cluster.NewConfig(1, mustMarshal(&configtest.TestConfig{
			Hosts: []string{"h1", "h2", "h3"},
		})))

	// Get the initial connection
	d.unblockOpen()
	conn, err := m.GetConn("wow", "c1")
	require.NoError(t, err)

	fconn, ok := conn.(*managerTestConn)
	require.True(t, ok)
	require.Equal(t, []string{"h1", "h2", "h3"}, fconn.TestConfig.Hosts)

	// Update the connection, then wait a little bit for the watch goroutine to fire
	updateCh <- cluster.NewCluster("c1", cluster.NewType("unsupported"), "wow",
		cluster.NewConfig(2, mustMarshal(&configtest.TestConfig{
			Hosts: []string{"h1", "h2", "h3"},
		})))

	time.Sleep(time.Millisecond * 150)

	// Get the connection, should return the old connection
	conn, err = m.GetConn("wow", "c1")
	require.NoError(t, err)

	fconn, ok = conn.(*managerTestConn)
	require.True(t, ok)
	require.Equal(t, []string{"h1", "h2", "h3"}, fconn.TestConfig.Hosts)

	// The driver should have open called, but not reconfigure since the driver
	// could not be located
	require.Equal(t, uint32(1), d.opens)
	require.Equal(t, uint32(0), d.reconfigures)
}

func TestManagerCloseWithOpenConns(t *testing.T) {
	d, _, m, updateCh := initManagerTest(t, nil)

	d.unblockOpen()
	updateCh <- cluster.NewCluster("c1", fakeStorageType, "wow",
		cluster.NewConfig(1, mustMarshal(&configtest.TestConfig{
			Hosts: []string{"h1", "h2", "h3"},
		})))

	// Get a connection
	conn, err := m.GetConn("wow", "c1")
	require.NoError(t, err)

	fconn, ok := conn.(*managerTestConn)
	require.True(t, ok)
	require.Equal(t, []string{"h1", "h2", "h3"}, fconn.TestConfig.Hosts)

	// Close the connection manager.  This blocks until the watch goroutines
	// exit
	m.Close()

	// Confirm the connection is closed
	require.Equal(t, uint32(1), fconn.closed)

	// Close again, this shouldn't do anything since we're already closed
	m.Close()
	require.Equal(t, uint32(1), fconn.closed)
}

func initManagerTest(t *testing.T, opts ManagerOptions,
) (*managerTestDriver, cluster.Provider, Manager, chan cluster.Cluster) {
	d := newFakeDriver()

	updateCh := make(chan cluster.Cluster, 1)
	p, err := cluster.NewProvider(updateCh, cluster.NewProviderOptions())
	require.NoError(t, err)

	if opts == nil {
		opts = NewManagerOptions()
	}

	m, err := NewManager(opts.Provider(p).Drivers([]Driver{d}))
	require.NoError(t, err)

	return d, p, m, updateCh
}

var (
	fakeStorageType = cluster.NewType("fake")
)

type managerTestDriver struct {
	opens        uint32
	reconfigures uint32
	openCalled   chan struct{}
	openAllowed  chan struct{}
	fail         error
}

func newFakeDriver() *managerTestDriver {
	return &managerTestDriver{
		openAllowed: make(chan struct{}),
		openCalled:  make(chan struct{}, 10),
	}
}

func (d *managerTestDriver) failOpen(err error) {
	d.fail = err
}

func (d *managerTestDriver) waitForOpen() {
	<-d.openCalled
}

func (d *managerTestDriver) unblockOpen() {
	close(d.openAllowed)
}

func (d *managerTestDriver) ConfigType() proto.Message { return &configtest.TestConfig{} }
func (d *managerTestDriver) Type() cluster.Type        { return fakeStorageType }
func (d *managerTestDriver) Close() error              { return nil }
func (d *managerTestDriver) Open(cfg proto.Message) (Conn, error) {
	select {
	case d.openCalled <- struct{}{}:
	default:
	}
	<-d.openAllowed
	atomic.AddUint32(&d.opens, 1)

	if d.fail != nil {
		return nil, d.fail
	}

	return &managerTestConn{
		TestConfig: *(cfg.(*configtest.TestConfig)),
	}, nil
}

func (d *managerTestDriver) Reconfigure(conn Conn, cfg proto.Message) (Conn, error) {
	atomic.AddUint32(&d.reconfigures, 1)

	if d.fail != nil {
		return nil, d.fail
	}

	return &managerTestConn{
		TestConfig: *(cfg.(*configtest.TestConfig)),
	}, nil
}

type managerTestConn struct {
	configtest.TestConfig
	closed uint32
}

func (conn *managerTestConn) Read(id string, r retention.Resolution, start, end time.Time) (ts.SeriesIter, error) {
	return nil, nil
}

func (conn *managerTestConn) Write(id string, r retention.Resolution, t time.Time, v float64) error {
	return nil
}

func (conn *managerTestConn) Close() error {
	atomic.AddUint32(&conn.closed, 1)
	return nil
}

func mustMarshal(msg proto.Message) []byte {
	b, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}

	return b
}
