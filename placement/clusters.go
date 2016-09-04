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
	"sync"

	"github.com/m3db/m3storage"
	"github.com/m3db/m3storage/generated/proto/schema"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/watch"

	"github.com/golang/protobuf/proto"
)

// clusters maintains information about active clusters, including watches
type clusters struct {
	sync.RWMutex
	v   map[string]int              // versions of each cluster
	w   map[string]xwatch.Watchable // watches for each cluster
	log xlog.Logger
}

// newClusters create a new set of clusters
func newClusters(log xlog.Logger) *clusters {
	return &clusters{
		v:   make(map[string]int),
		w:   make(map[string]xwatch.Watchable),
		log: log,
	}
}

// watchCluster watches the given named cluster
func (cc *clusters) watch(cname string) (storage.ClusterWatch, error) {
	cc.RLock()
	w, exists := cc.w[cname]
	cc.RUnlock()

	if !exists {
		return nil, errClusterNotFound
	}

	cc.log.Debugf("watching %s", cname)
	_, watch, err := w.Watch()
	if err != nil {
		return nil, err
	}

	return clusterWatch{watch}, nil
}

// update updates the given cluster and notifies watches
func (cc *clusters) update(c cluster) {
	cc.Lock()
	defer cc.Unlock()

	currentVersion, existing := cc.v[c.name]
	if !existing {
		// This is a new cluster - create a watch for it and set the initial value
		cc.log.Debugf("registering new cluster %s@%d", c.name, c.config.Version())
		w := xwatch.NewWatchable()
		cc.w[c.name], cc.v[c.name] = w, c.config.Version()
		w.Update(c)
		return
	}

	// This is an existing cluster and the version has updated, notify watches
	if currentVersion < c.config.Version() {
		cc.log.Debugf("update config for cluster %s from %d to %d", c.name, currentVersion, c.config.Version())
		cc.w[c.name].Update(c)
		cc.v[c.name] = c.config.Version()
	}
}

// cluster is a storage.Cluster based on placement data
type cluster struct {
	database, name string
	storageType    storage.Type
	config         clusterConfig
}

func newCluster(database string, c *schema.Cluster, version int) cluster {
	return cluster{
		database:    database,
		name:        c.Properties.Name,
		storageType: storageType{c.Properties.Type},
		config: clusterConfig{
			version: version,
			bytes:   c.Config,
		},
	}
}

func (c cluster) Name() string           { return c.name }
func (c cluster) Database() string       { return c.database }
func (c cluster) Type() storage.Type     { return c.storageType }
func (c cluster) Config() storage.Config { return c.config }

// clusterConfig is a storage.Config base on placement data
type clusterConfig struct {
	version int
	bytes   []byte
}

func (cfg clusterConfig) Version() int { return cfg.version }
func (cfg clusterConfig) Unmarshal(c proto.Message) error {
	return proto.Unmarshal(cfg.bytes, c)
}

// clusterWatch is a type-safe wrapper around xwatch.Watch
type clusterWatch struct {
	xwatch.Watch
}

func (cw clusterWatch) C() <-chan struct{}   { return cw.Watch.C() }
func (cw clusterWatch) Get() storage.Cluster { return cw.Watch.Get().(storage.Cluster) }
func (cw clusterWatch) Close() error {
	cw.Watch.Close()
	return nil
}

// storageType is a storage.Type based on placement data
type storageType struct {
	string
}

func (t storageType) Name() string { return t.string }
