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
	"sort"
	"sync"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3storage"
	"github.com/m3db/m3storage/generated/proto/schema"
	"github.com/m3db/m3x/log"

	"github.com/facebookgo/clock"
)

// ClusterMappingProviderOptions are options used to build a ClusterMappingProvider
type ClusterMappingProviderOptions interface {
	// Logger is the logger to use
	Logger(logger xlog.Logger) ClusterMappingProviderOptions
	GetLogger() xlog.Logger

	// Clock is the clock to use
	Clock(clock clock.Clock) ClusterMappingProviderOptions
	GetClock() clock.Clock
}

// NewClusterMappingProviderOptions returns new empty ClusterMappingProviderOptions
func NewClusterMappingProviderOptions() ClusterMappingProviderOptions {
	return clusterMappingProviderOptions{}
}

// clusterMappingProvider implements the ClusterMappingProvider interface on
// top of the placement data
type clusterMappingProvider struct {
	sync.RWMutex
	byRetention []*database          // ordered by retention period (shortest first)
	byName      map[string]*database // indexed by name

	watch  kv.ValueWatch
	closed chan struct{}
	log    xlog.Logger
	clock  clock.Clock
}

// NewClusterMappingProvider returns a new ClusterMappingProvider
// around a kv store and key
func NewClusterMappingProvider(key string, kvStore kv.Store, opts ClusterMappingProviderOptions,
) (storage.ClusterMappingProvider, error) {
	if opts == nil {
		opts = clusterMappingProviderOptions{}
	}

	c, log := opts.GetClock(), opts.GetLogger()
	if c == nil {
		c = clock.New()
	}

	if log == nil {
		log = xlog.NullLogger
	}

	watch, err := kvStore.Watch(key)
	if err != nil {
		return nil, err
	}

	prov := &clusterMappingProvider{
		byName: make(map[string]*database),
		log:    log,
		clock:  c,
		watch:  watch,
		closed: make(chan struct{}),
	}

	go prov.watchPlacementChanges()
	return prov, nil
}

// QueryMappings returns the mappings for a given shard and retention policy
func (mp *clusterMappingProvider) QueryMappings(
	shard uint32, start, end time.Time) (storage.ClusterMappingIter, error) {
	// Figure out how far back this query goes
	queryAgeInSecs := int32(mp.clock.Now().Sub(end) / time.Second)

	// Figure out which database mapping to use given the age of the datapoints being queried
	db := mp.findDatabase(queryAgeInSecs)
	if db == nil {
		// No database mappings - return an empty iterator
		return &mappingRuleIter{}, nil
	}

	// Figure out which active mapping contains that shard
	return &mappingRuleIter{
		prior: db.mappings.findActiveForShard(uint(shard)),
	}, nil
}

// WatchCluster retrieves a cluster configuration and watches for changes
func (mp *clusterMappingProvider) WatchCluster(database, cluster string) (storage.ClusterWatch, error) {
	mp.RLock()
	db := mp.byName[database]
	mp.RUnlock()

	if db == nil {
		return nil, errDatabaseNotFound
	}

	return db.clusters.watch(cluster)
}

// Close closes the provider and stops watching for placement changes
func (mp *clusterMappingProvider) Close() error {
	mp.watch.Close()
	<-mp.closed
	return nil
}

// findDatabaseMappings finds the database mappings that apply to the given retention
func (mp *clusterMappingProvider) findDatabase(retentionPeriodInSecs int32) *database {
	// NB(mmihic): We use copy-on-write for the database mappings, so it's safe to return
	// a reference without the
	mp.RLock()
	defer mp.RUnlock()

	if len(mp.byRetention) == 0 {
		return nil
	}

	for _, dbm := range mp.byRetention {
		// TODO(mmihic): Handle read cutover time and cutover complete time
		if retentionPeriodInSecs <= dbm.maxRetentionInSecs {
			return dbm
		}
	}

	// If we the retentionPeriod falls outside all database retention periods,
	// use the one with the longest period
	return mp.byRetention[len(mp.byRetention)-1]
}

// update updates the mappings based on new Placement information
func (mp *clusterMappingProvider) update(p *schema.Placement) error {
	// Avoid the need for upstream code to synchronize on the cluster
	// mappings by first making a copy, then updating the copy in place,
	// then swapping out the pointers atomically
	mp.RLock()
	byName := make(map[string]*database, len(mp.byName))
	byRetention := make([]*database, len(mp.byRetention))
	for n, db := range mp.byRetention {
		byRetention[n], byName[db.name] = db, db
	}
	mp.RUnlock()

	// Update the clone with the new settings
	for _, dbConfig := range p.Databases {
		if existing := byName[dbConfig.Properties.Name]; existing != nil {
			if err := existing.update(dbConfig); err != nil {
				return err
			}
			continue
		}

		// This is a new mapping - create and apply all rules
		db, err := newDatabase(dbConfig, mp.log)
		if err != nil {
			return err
		}

		byName[dbConfig.Properties.Name] = db
		byRetention = append(byRetention, db)
	}

	sort.Sort(databasesByMaxRetention(byRetention))

	// Swap out the pointers
	mp.Lock()
	mp.byRetention, mp.byName = byRetention, byName
	mp.Unlock()
	return nil
}

// watchPlacementChanges is a background goroutine that watches
// for placement changes
func (mp *clusterMappingProvider) watchPlacementChanges() {
	var placement schema.Placement
	for range mp.watch.C() {
		val := mp.watch.Get()
		if err := val.Unmarshal(&placement); err != nil {
			mp.log.Errorf("could not unmarshal placement data: %v", err)
			continue
		}

		mp.log.Infof("received placement version %d", val.Version())
		mp.update(&placement)
	}
	close(mp.closed)
}

// clusterMappingProviderOptions are options to a ClusterMappingProvider
type clusterMappingProviderOptions struct {
	log   xlog.Logger
	clock clock.Clock
}

func (opts clusterMappingProviderOptions) GetLogger() xlog.Logger { return opts.log }
func (opts clusterMappingProviderOptions) GetClock() clock.Clock  { return opts.clock }

func (opts clusterMappingProviderOptions) Logger(log xlog.Logger) ClusterMappingProviderOptions {
	opts.log = log
	return opts
}

func (opts clusterMappingProviderOptions) Clock(clock clock.Clock) ClusterMappingProviderOptions {
	opts.clock = clock
	return opts
}

// sort.Interface for sorting database by retention period
type databasesByMaxRetention []*database

func (dbs databasesByMaxRetention) Len() int      { return len(dbs) }
func (dbs databasesByMaxRetention) Swap(i, j int) { dbs[i], dbs[j] = dbs[j], dbs[i] }
func (dbs databasesByMaxRetention) Less(i, j int) bool {
	return dbs[i].maxRetentionInSecs < dbs[j].maxRetentionInSecs
}
