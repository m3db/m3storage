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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3storage/generated/proto/configtest"
	"github.com/m3db/m3storage/retention"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/watch"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestNewConnectionManagerErrors(t *testing.T) {
	d := newFakeDriver()
	p := newFakeProvider()

	tests := []struct {
		opts ConnectionManagerOptions
		err  error
	}{
		{NewConnectionManagerOptions().Logger(nil).Drivers([]Driver{d}).Provider(p),
			errConnectionManagerLogRequired},
		{NewConnectionManagerOptions().Provider(p),
			errConnectionManagerDriversRequired},
		{NewConnectionManagerOptions().Drivers([]Driver{d}),
			errConnectionManagerProviderRequired},
	}

	for _, test := range tests {
		m, err := NewConnectionManager(test.opts)
		require.Equal(t, test.err, err)
		require.Nil(t, m)
	}
}

func TestConnectionManagerGetConnection(t *testing.T) {
	d := newFakeDriver()
	p := newFakeProvider()
	m, err := NewConnectionManager(NewConnectionManagerOptions().
		Provider(p).
		Drivers([]Driver{d}))

	require.NoError(t, err)

	p.updateCluster(fakeCluster{
		database:    "wow",
		name:        "c1",
		storageType: fakeStorageType,
		config: fakeConfig{
			version: 1,
			bytes: mustMarshal(&configtest.TestConfig{
				Hosts: []string{"h1", "h2", "h3"},
			}),
		},
	})

	d.unblockOpen()

	conn, err := m.GetConnection("wow", "c1")
	require.NoError(t, err)
	fconn, ok := conn.(*fakeConnection)
	require.True(t, ok)
	require.Equal(t, []string{"h1", "h2", "h3"}, fconn.TestConfig.Hosts)
	require.Equal(t, uint32(1), d.opens)
}

func TestConnectionManagerConcurrentGetNewConnection(t *testing.T) {
	d := newFakeDriver()
	p := newFakeProvider()
	m, err := NewConnectionManager(NewConnectionManagerOptions().
		Provider(p).
		Drivers([]Driver{d}).
		Logger(xlog.SimpleLogger))

	require.NoError(t, err)

	p.updateCluster(fakeCluster{
		database:    "wow",
		name:        "c1",
		storageType: fakeStorageType,
		config: fakeConfig{
			version: 1,
			bytes: mustMarshal(&configtest.TestConfig{
				Hosts: []string{"h1", "h2", "h3"},
			}),
		},
	})

	connCh := make(chan Connection, 5)
	errorCh := make(chan error, 5)
	var wg sync.WaitGroup

	requestConn := func() {
		defer wg.Done()

		conn, err := m.GetConnection("wow", "c1")
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

func TestConnectionManagerGetConnectionWatchFails(t *testing.T) {
	d := newFakeDriver()
	p := newFakeProvider()
	m, err := NewConnectionManager(NewConnectionManagerOptions().
		Provider(p).
		Drivers([]Driver{d}).
		Logger(xlog.SimpleLogger))

	require.NoError(t, err)

	d.unblockOpen()

	conn, err := m.GetConnection("wow", "c1")
	require.Error(t, err)
	require.Nil(t, conn)
}

func TestConnectionManagerGetConnectionDriverFails(t *testing.T) {
	errDriver := errors.New("driver failure")

	d := newFakeDriver()
	p := newFakeProvider()
	m, err := NewConnectionManager(NewConnectionManagerOptions().
		Provider(p).
		Drivers([]Driver{d}))

	require.NoError(t, err)

	p.updateCluster(fakeCluster{
		database:    "wow",
		name:        "c1",
		storageType: fakeStorageType,
		config: fakeConfig{
			version: 1,
			bytes: mustMarshal(&configtest.TestConfig{
				Hosts: []string{"h1", "h2", "h3"},
			}),
		},
	})

	d.failOpen(errDriver)
	d.unblockOpen()

	conn, err := m.GetConnection("wow", "c1")
	require.Equal(t, errDriver, err)
	require.Nil(t, conn)
}

func TestConnectionManagerGetConnectionClosedManager(t *testing.T) {
	d := newFakeDriver()
	p := newFakeProvider()
	m, err := NewConnectionManager(NewConnectionManagerOptions().
		Provider(p).
		Drivers([]Driver{d}))

	require.NoError(t, err)

	p.updateCluster(fakeCluster{
		database:    "wow",
		name:        "c1",
		storageType: fakeStorageType,
		config: fakeConfig{
			version: 1,
			bytes: mustMarshal(&configtest.TestConfig{
				Hosts: []string{"h1", "h2", "h3"},
			}),
		},
	})

	m.Close()

	conn, err := m.GetConnection("wow", "c1")
	require.Equal(t, errClosed, err)
	require.Nil(t, conn)
}

func TestConnectionManagerGetConnectionUnsupportedType(t *testing.T) {
	d := newFakeDriver()
	p := newFakeProvider()
	m, err := NewConnectionManager(NewConnectionManagerOptions().
		Provider(p).
		Drivers([]Driver{d}))

	require.NoError(t, err)

	p.updateCluster(fakeCluster{
		database:    "wow",
		name:        "c1",
		storageType: simpleStorageType("unknown-type"),
		config: fakeConfig{
			version: 1,
			bytes: mustMarshal(&configtest.TestConfig{
				Hosts: []string{"h1", "h2", "h3"},
			}),
		},
	})
	d.unblockOpen()

	conn, err := m.GetConnection("wow", "c1")
	require.Equal(t, errTypeUnsupported, err)
	require.Nil(t, conn)
}

func TestConnectionManagerGetConnectionManagerClosedWhileDriverWorking(t *testing.T) {
	d := newFakeDriver()
	p := newFakeProvider()
	m, err := NewConnectionManager(NewConnectionManagerOptions().
		Provider(p).
		Drivers([]Driver{d}))

	require.NoError(t, err)

	p.updateCluster(fakeCluster{
		database:    "wow",
		name:        "c1",
		storageType: fakeStorageType,
		config: fakeConfig{
			version: 1,
			bytes: mustMarshal(&configtest.TestConfig{
				Hosts: []string{"h1", "h2", "h3"},
			}),
		},
	})

	// Spin up a background goroutine to request the connection. This will
	// block in the driver until we're ready to let it run
	connCh := make(chan Connection, 5)
	errorCh := make(chan error, 5)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		conn, err := m.GetConnection("wow", "c1")
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

func TestConnectionManagerGetConnectionUnmarshalConfigError(t *testing.T) {
	d := newFakeDriver()
	p := newFakeProvider()
	m, err := NewConnectionManager(NewConnectionManagerOptions().
		Provider(p).
		Drivers([]Driver{d}))

	require.NoError(t, err)

	p.updateCluster(fakeCluster{
		database:    "wow",
		name:        "c1",
		storageType: fakeStorageType,
		config: fakeConfig{
			version: 1,
			bytes:   []byte("invalid-proto"),
		},
	})

	conn, err := m.GetConnection("wow", "c1")
	require.Error(t, err)
	require.Nil(t, conn)
}

func TestConnectionManagerReconfigureCluster(t *testing.T) {
	d := newFakeDriver()
	p := newFakeProvider()
	m, err := NewConnectionManager(NewConnectionManagerOptions().
		Provider(p).
		Drivers([]Driver{d}).
		Logger(xlog.SimpleLogger))

	require.NoError(t, err)

	p.updateCluster(fakeCluster{
		database:    "wow",
		name:        "c1",
		storageType: fakeStorageType,
		config: fakeConfig{
			version: 1,
			bytes: mustMarshal(&configtest.TestConfig{
				Hosts: []string{"h1", "h2", "h3"},
			}),
		},
	})

	// Get the initial connection
	d.unblockOpen()
	conn, err := m.GetConnection("wow", "c1")
	require.NoError(t, err)

	fconn, ok := conn.(*fakeConnection)
	require.True(t, ok)
	require.Equal(t, []string{"h1", "h2", "h3"}, fconn.TestConfig.Hosts)

	// Update the connection, then wait a little bit for the watch goroutine to fire
	p.updateCluster(fakeCluster{
		database:    "wow",
		name:        "c1",
		storageType: fakeStorageType,
		config: fakeConfig{
			version: 2,
			bytes: mustMarshal(&configtest.TestConfig{
				Hosts: []string{"h4"},
			}),
		},
	})

	time.Sleep(time.Millisecond * 150)

	// Get the connection, should return the updated connection
	conn, err = m.GetConnection("wow", "c1")
	require.NoError(t, err)

	fconn, ok = conn.(*fakeConnection)
	require.True(t, ok)
	require.Equal(t, []string{"h4"}, fconn.TestConfig.Hosts)

	// The driver should have open and reconfigure called appropriately
	require.Equal(t, uint32(1), d.opens)
	require.Equal(t, uint32(1), d.reconfigures)
}

func TestConnectionManagerReconfigureClusterDriverFails(t *testing.T) {
	d := newFakeDriver()
	p := newFakeProvider()
	m, err := NewConnectionManager(NewConnectionManagerOptions().
		Provider(p).
		Drivers([]Driver{d}))

	require.NoError(t, err)

	p.updateCluster(fakeCluster{
		database:    "wow",
		name:        "c1",
		storageType: fakeStorageType,
		config: fakeConfig{
			version: 1,
			bytes: mustMarshal(&configtest.TestConfig{
				Hosts: []string{"h1", "h2", "h3"},
			}),
		},
	})

	// Get the initial connection
	d.unblockOpen()
	conn, err := m.GetConnection("wow", "c1")
	require.NoError(t, err)

	fconn, ok := conn.(*fakeConnection)
	require.True(t, ok)
	require.Equal(t, []string{"h1", "h2", "h3"}, fconn.TestConfig.Hosts)

	// Update the connection, then wait a little bit for the watch goroutine to fire
	d.failOpen(errors.New("bad driver"))
	p.updateCluster(fakeCluster{
		database:    "wow",
		name:        "c1",
		storageType: fakeStorageType,
		config: fakeConfig{
			version: 2,
			bytes: mustMarshal(&configtest.TestConfig{
				Hosts: []string{"h4"},
			}),
		},
	})

	time.Sleep(time.Millisecond * 150)

	// Get the connection, should return the old connection
	conn, err = m.GetConnection("wow", "c1")
	require.NoError(t, err)

	fconn, ok = conn.(*fakeConnection)
	require.True(t, ok)
	require.Equal(t, []string{"h1", "h2", "h3"}, fconn.TestConfig.Hosts)

	// The driver should have open and reconfigure called appropriately
	require.Equal(t, uint32(1), d.opens)
	require.Equal(t, uint32(1), d.reconfigures)

}

func TestConnectionManagerReconfigureClusterUnmarshalError(t *testing.T) {
	d := newFakeDriver()
	p := newFakeProvider()
	m, err := NewConnectionManager(NewConnectionManagerOptions().
		Provider(p).
		Drivers([]Driver{d}))

	require.NoError(t, err)

	p.updateCluster(fakeCluster{
		database:    "wow",
		name:        "c1",
		storageType: fakeStorageType,
		config: fakeConfig{
			version: 1,
			bytes: mustMarshal(&configtest.TestConfig{
				Hosts: []string{"h1", "h2", "h3"},
			}),
		},
	})

	// Get the initial connection
	d.unblockOpen()
	conn, err := m.GetConnection("wow", "c1")
	require.NoError(t, err)

	fconn, ok := conn.(*fakeConnection)
	require.True(t, ok)
	require.Equal(t, []string{"h1", "h2", "h3"}, fconn.TestConfig.Hosts)

	// Update the connection, then wait a little bit for the watch goroutine to fire
	p.updateCluster(fakeCluster{
		database:    "wow",
		name:        "c1",
		storageType: fakeStorageType,
		config: fakeConfig{
			version: 2,
			bytes:   []byte("not-proto"),
		},
	})

	time.Sleep(time.Millisecond * 150)

	// Get the connection, should return the old connection
	conn, err = m.GetConnection("wow", "c1")
	require.NoError(t, err)

	fconn, ok = conn.(*fakeConnection)
	require.True(t, ok)
	require.Equal(t, []string{"h1", "h2", "h3"}, fconn.TestConfig.Hosts)

	// The driver should have open called, but not reconfigure
	require.Equal(t, uint32(1), d.opens)
	require.Equal(t, uint32(0), d.reconfigures)
}

func TestConnectionManagerReconfigureClusterUnsupportedType(t *testing.T) {
	d := newFakeDriver()
	p := newFakeProvider()
	m, err := NewConnectionManager(NewConnectionManagerOptions().
		Provider(p).
		Drivers([]Driver{d}))

	require.NoError(t, err)

	p.updateCluster(fakeCluster{
		database:    "wow",
		name:        "c1",
		storageType: fakeStorageType,
		config: fakeConfig{
			version: 1,
			bytes: mustMarshal(&configtest.TestConfig{
				Hosts: []string{"h1", "h2", "h3"},
			}),
		},
	})

	// Get the initial connection
	d.unblockOpen()
	conn, err := m.GetConnection("wow", "c1")
	require.NoError(t, err)

	fconn, ok := conn.(*fakeConnection)
	require.True(t, ok)
	require.Equal(t, []string{"h1", "h2", "h3"}, fconn.TestConfig.Hosts)

	// Update the connection, then wait a little bit for the watch goroutine to fire
	p.updateCluster(fakeCluster{
		database:    "wow",
		name:        "c1",
		storageType: simpleStorageType("unsupported"),
		config: fakeConfig{
			version: 2,
			bytes: mustMarshal(&configtest.TestConfig{
				Hosts: []string{"h4"},
			}),
		},
	})

	time.Sleep(time.Millisecond * 150)

	// Get the connection, should return the old connection
	conn, err = m.GetConnection("wow", "c1")
	require.NoError(t, err)

	fconn, ok = conn.(*fakeConnection)
	require.True(t, ok)
	require.Equal(t, []string{"h1", "h2", "h3"}, fconn.TestConfig.Hosts)

	// The driver should have open called, but not reconfigure since the driver
	// could not be located
	require.Equal(t, uint32(1), d.opens)
	require.Equal(t, uint32(0), d.reconfigures)
}

func TestConnectionManagerCloseWithOpenConns(t *testing.T) {
	d := newFakeDriver()
	p := newFakeProvider()
	m, err := NewConnectionManager(NewConnectionManagerOptions().
		Provider(p).
		Drivers([]Driver{d}))

	require.NoError(t, err)

	d.unblockOpen()
	p.updateCluster(fakeCluster{
		database:    "wow",
		name:        "c1",
		storageType: fakeStorageType,
		config: fakeConfig{
			version: 1,
			bytes: mustMarshal(&configtest.TestConfig{
				Hosts: []string{"h1", "h2", "h3"},
			}),
		},
	})

	// Get a connection
	conn, err := m.GetConnection("wow", "c1")
	require.NoError(t, err)

	fconn, ok := conn.(*fakeConnection)
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

type simpleStorageType string

func (t simpleStorageType) Name() string { return string(t) }

const fakeStorageType simpleStorageType = "fake"

type fakeDriver struct {
	opens        uint32
	reconfigures uint32
	openCalled   chan struct{}
	openAllowed  chan struct{}
	fail         error
}

func newFakeDriver() *fakeDriver {
	return &fakeDriver{
		openAllowed: make(chan struct{}),
		openCalled:  make(chan struct{}, 10),
	}
}

func (d *fakeDriver) failOpen(err error) {
	d.fail = err
}

func (d *fakeDriver) waitForOpen() {
	<-d.openCalled
}

func (d *fakeDriver) unblockOpen() {
	close(d.openAllowed)
}

func (d *fakeDriver) ConfigType() proto.Message { return &configtest.TestConfig{} }
func (d *fakeDriver) Type() Type                { return fakeStorageType }
func (d *fakeDriver) Close() error              { return nil }
func (d *fakeDriver) OpenConnection(cfg proto.Message) (Connection, error) {
	select {
	case d.openCalled <- struct{}{}:
	default:
	}
	<-d.openAllowed
	atomic.AddUint32(&d.opens, 1)

	if d.fail != nil {
		return nil, d.fail
	}

	return &fakeConnection{
		TestConfig: *(cfg.(*configtest.TestConfig)),
	}, nil
}

func (d *fakeDriver) ReconfigureConnection(conn Connection, cfg proto.Message) (Connection, error) {
	atomic.AddUint32(&d.reconfigures, 1)

	if d.fail != nil {
		return nil, d.fail
	}

	return &fakeConnection{
		TestConfig: *(cfg.(*configtest.TestConfig)),
	}, nil
}

type fakeConnection struct {
	configtest.TestConfig
	closed uint32
}

func (conn *fakeConnection) Read(id string, r retention.Resolution, start, end time.Time) (SeriesIter, error) {
	return nil, nil
}

func (conn *fakeConnection) Write(id string, r retention.Resolution, t time.Time, v float64) error {
	return nil
}

func (conn *fakeConnection) Close() error {
	atomic.AddUint32(&conn.closed, 1)
	return nil
}

type fakeProvider struct {
	sync.Mutex
	clusters map[string]xwatch.Watchable
}

func newFakeProvider() *fakeProvider {
	return &fakeProvider{
		clusters: make(map[string]xwatch.Watchable),
	}
}

func (p *fakeProvider) updateCluster(c Cluster) {
	key := fmtConnKey(c.Database(), c.Name())
	p.Lock()
	if w := p.clusters[key]; w != nil {
		p.Unlock()
		w.Update(c)
		return
	}

	w := xwatch.NewWatchable()
	p.clusters[key] = w
	p.Unlock()

	w.Update(c)
}

func (p *fakeProvider) WatchCluster(database, cluster string) (ClusterWatch, error) {
	key := fmtConnKey(database, cluster)
	p.Lock()
	w := p.clusters[key]
	p.Unlock()

	if w == nil {
		return nil, errors.New("cluster not found")
	}

	_, watch, err := w.Watch()
	if err != nil {
		return nil, err
	}

	return NewClusterWatch(watch), nil
}

func (p *fakeProvider) QueryMappings(shard uint32, start, end time.Time) (MappingRuleIter, error) {
	return nil, nil
}

func (p *fakeProvider) Close() error { return nil }

type fakeCluster struct {
	database, name string
	storageType    Type
	config         fakeConfig
}

func (c fakeCluster) Name() string     { return c.name }
func (c fakeCluster) Database() string { return c.database }
func (c fakeCluster) Type() Type       { return c.storageType }
func (c fakeCluster) Config() Config   { return c.config }

type fakeConfig struct {
	bytes   []byte
	version int
}

func (cfg fakeConfig) Unmarshal(c proto.Message) error { return proto.Unmarshal(cfg.bytes, c) }
func (cfg fakeConfig) Version() int                    { return cfg.version }

func mustMarshal(msg proto.Message) []byte {
	b, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}

	return b
}
