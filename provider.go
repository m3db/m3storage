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
	"time"

	"github.com/m3db/m3storage/mapping"
	"github.com/m3db/m3x/close"
	"github.com/m3db/m3x/watch"
)

// A ClusterWatch watches for config changes on a cluster
type ClusterWatch interface {
	xclose.Closer

	// C is the channel receiving notifications of config changes
	C() <-chan struct{}

	// Get returns the current state of the cluster
	Get() Cluster
}

// A MappingRuleIter is an iterator over Rules.  Allows provider to
// control how these are stored internally
type MappingRuleIter interface {
	xclose.Closer

	// Next moves to the next mapping, returning false if there are no more
	// mappings
	Next() bool

	// Current returns the current mapping
	Current() mapping.Rule
}

// A ClusterMappingProvider provides shard mapping rules and cluster watches
type ClusterMappingProvider interface {
	xclose.Closer

	// QueryMappings returns the active cluster mappings for the given query
	QueryMappings(shard uint32, start, end time.Time) (MappingRuleIter, error)

	// WatchCluster returns the config for a cluster as a watch that can
	// be used to listen for updates to that cluster.  Callers must wait
	// on the watch channel before attempting to access the Cluster
	WatchCluster(database, cluster string) (ClusterWatch, error)
}

// NewClusterWatch wraps a ClusterWatch around an existing Watch
func NewClusterWatch(w xwatch.Watch) ClusterWatch { return &clusterWatch{Watch: w} }

type clusterWatch struct {
	xwatch.Watch
}

func (w *clusterWatch) C() <-chan struct{} { return w.Watch.C() }

func (w *clusterWatch) Get() Cluster {
	if c := w.Watch.Get(); c != nil {
		return c.(Cluster)
	}

	return nil
}

func (w *clusterWatch) Close() error {
	w.Watch.Close()
	return nil
}
