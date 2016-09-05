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

	"github.com/m3db/m3x/close"
)

// A ClusterWatch watches for config changes on a cluster
type ClusterWatch interface {
	xclose.Closer

	// C is the channel receiving notifications of config changes
	C() <-chan struct{}

	// Get returns the current state of the cluster
	Get() Cluster
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
