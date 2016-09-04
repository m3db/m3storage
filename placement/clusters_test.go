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

package placement

import (
	"testing"
	"time"

	"github.com/m3db/m3storage/generated/proto/configtest"
	"github.com/m3db/m3storage/generated/proto/schema"
	"github.com/m3db/m3x/log"

	"github.com/stretchr/testify/require"
)

func TestClusters_WatchNonExistent(t *testing.T) {
	clusters := newClusters(xlog.SimpleLogger)
	w, err := clusters.watch("c1")
	require.Equal(t, errClusterNotFound, err)
	require.Nil(t, w)
}

func TestClusters_Watch(t *testing.T) {
	clusters := newClusters(xlog.SimpleLogger)

	clusters.update(newCluster("foo", &schema.Cluster{
		Properties: &schema.ClusterProperties{
			Name: "c1",
			Type: "m3db",
		},
		Config: newTestConfigBytes(t, "h1", "h2", "h3"),
	}, 43))

	// Register an initial watch and make sure we get the first value
	w, err := clusters.watch("c1")
	require.NoError(t, err)
	<-w.C()

	c := w.Get()
	require.Equal(t, "c1", c.Name())
	require.Equal(t, "foo", c.Database())
	require.Equal(t, "m3db", c.Type().Name())
	require.Equal(t, 43, c.Config().Version())

	var cfg configtest.TestConfig
	require.NoError(t, c.Config().Unmarshal(&cfg))
	require.Equal(t, []string{"h1", "h2", "h3"}, cfg.Hosts)

	// Update the config
	clusters.update(newCluster("foo", &schema.Cluster{
		Properties: &schema.ClusterProperties{
			Name: "c1",
			Type: "m3db",
		},
		Config: newTestConfigBytes(t, "h1", "h2"),
	}, 45))

	// Make sure the watch is triggered
	<-w.C()
	c = w.Get()
	require.Equal(t, "c1", c.Name())
	require.Equal(t, "foo", c.Database())
	require.Equal(t, "m3db", c.Type().Name())
	require.Equal(t, 45, c.Config().Version())
	require.NoError(t, c.Config().Unmarshal(&cfg))
	require.Equal(t, []string{"h1", "h2"}, cfg.Hosts)

	// Update but with an older version, no watch should fire
	clusters.update(newCluster("foo", &schema.Cluster{
		Properties: &schema.ClusterProperties{
			Name: "c1",
			Type: "m3db",
		},
		Config: newTestConfigBytes(t, "h1", "h2"),
	}, 44))

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
}
