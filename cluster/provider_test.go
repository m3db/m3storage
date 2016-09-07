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

package cluster

import (
	"testing"
	"time"

	"github.com/m3db/m3x/log"

	"github.com/stretchr/testify/require"
)

func TestProviderWatchClosed(t *testing.T) {
	ch := make(chan Cluster)
	p, err := NewProvider(ch, ProviderOptions{
		Logger: xlog.SimpleLogger,
	})
	require.NoError(t, err)

	// Close the provider
	require.NoError(t, p.Close())

	// Try to watch - should fail
	w, err := p.WatchCluster("foo", "c1")
	require.Equal(t, errClosed, err)
	require.Nil(t, w)
}

func TestProviderWatchUnknownCluster(t *testing.T) {
	ch := make(chan Cluster)
	p, err := NewProvider(ch, ProviderOptions{
		Logger: xlog.SimpleLogger,
	})
	require.NoError(t, err)

	// Register an initial watch
	w, err := p.WatchCluster("foo", "c1")
	require.NoError(t, err)

	// Send the initial value
	ch <- NewCluster("c1", NewType("m3db"), "foo", NewConfig(43, []byte("hello")))

	// Should trigger the watch
	<-w.C()

	c := w.Get()
	require.Equal(t, "c1", c.Name())
	require.Equal(t, "foo", c.Database())
	require.Equal(t, "m3db", c.Type().Name())
	require.Equal(t, 43, c.Config().Version())

	// Close the provider
	require.NoError(t, p.Close())
}

func TestProviderWatch(t *testing.T) {
	ch := make(chan Cluster)
	p, err := NewProvider(ch, ProviderOptions{
		Logger: xlog.SimpleLogger,
	})
	require.NoError(t, err)

	ch <- NewCluster("c1", NewType("m3db"), "foo", NewConfig(43, []byte("hello")))

	// Register an initial watch and make sure we get the first value
	w, err := p.WatchCluster("foo", "c1")
	require.NoError(t, err)
	<-w.C()

	c := w.Get()
	require.Equal(t, "c1", c.Name())
	require.Equal(t, "foo", c.Database())
	require.Equal(t, "m3db", c.Type().Name())
	require.Equal(t, 43, c.Config().Version())

	// Update the config
	ch <- NewCluster("c1", NewType("m3db"), "foo", NewConfig(45, []byte("hello2")))

	// Make sure the watch is triggered
	<-w.C()
	c = w.Get()
	require.Equal(t, "c1", c.Name())
	require.Equal(t, "foo", c.Database())
	require.Equal(t, "m3db", c.Type().Name())
	require.Equal(t, 45, c.Config().Version())

	// Update but with an older version, no watch should fire
	ch <- NewCluster("c1", NewType("m3db"), "foo", NewConfig(44, []byte("hello3")))

	updated := false
	select {
	case <-w.C():
		updated = true
	case <-time.After(time.Millisecond * 250):
	}
	require.False(t, updated)

	// Close the watch
	w.Close()
	<-w.C()

	// Close the provider
	require.NoError(t, p.Close())
}
