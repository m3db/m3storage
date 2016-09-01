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
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3storage"
	"github.com/m3db/m3storage/generated/proto/schema"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"

	"github.com/facebookgo/clock"
	"github.com/willf/bitset"
)

var (
	errInvalidTransition = errors.New("invalid shard transition")
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
	byRetention []*databaseMappings          // ordered by retention period (shortest first)
	byName      map[string]*databaseMappings // indexed by name

	watch  kv.ValueWatch
	closed chan struct{}
	log    xlog.Logger
	clock  clock.Clock
}

// NewClusterMappingProvider returns a new ClusterMappingProvider
// around a kv store and key
func NewClusterMappingProvider(
	key string,
	kvStore kv.Store,
	opts ClusterMappingProviderOptions,
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
		byName: make(map[string]*databaseMappings),
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
	shard uint32, start, end time.Time) storage.ClusterMappingIter {

	// Figure out how far back this query goes
	queryAgeInSecs := int32(mp.clock.Now().Sub(end) / time.Second)

	// Figure out which database mapping to use given the age of the datapoints being queried
	dbm := mp.findDatabaseMappings(queryAgeInSecs)
	if dbm == nil {
		return emptyClusterMappingIter{}
	}

	// Figure out which active mapping contains that shard
	return &clusterMappingIter{
		prior: dbm.findActiveForShard(uint(shard)),
	}
}

// Close closes the provider and stops watching for placement changes
func (mp *clusterMappingProvider) Close() error {
	mp.watch.Close()
	<-mp.closed
	return nil
}

// findDatabaseMappings finds the database mappings that apply to the given retention
func (mp *clusterMappingProvider) findDatabaseMappings(retentionPeriodInSecs int32) *databaseMappings {
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
	byName := make(map[string]*databaseMappings, len(mp.byName))
	byRetention := make([]*databaseMappings, len(mp.byRetention))
	for n, dbm := range mp.byRetention {
		dbm := dbm.clone()
		byRetention[n], byName[dbm.name] = dbm, dbm
	}
	mp.RUnlock()

	// Update the clone with the new settings
	for _, db := range p.Databases {
		if existing := byName[db.Properties.Name]; existing != nil {
			if err := existing.update(db); err != nil {
				return err
			}
			continue
		}

		// This is a new mapping - create and apply all rules
		dbm, err := newDatabaseMappings(db, mp.log)
		if err != nil {
			return err
		}

		byName[db.Properties.Name] = dbm
		byRetention = append(byRetention, dbm)
	}

	sort.Sort(databaseMappingsByMaxRetention(byRetention))

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

// databaseMappings are the mappings for a given database.
type databaseMappings struct {
	name               string
	version            int32
	maxRetentionInSecs int32
	active             []*activeClusterMapping
	log                xlog.Logger
}

// newDatabaseMappings returns a newly initialized set of databaseMappings
func newDatabaseMappings(db *schema.Database, log xlog.Logger) (*databaseMappings, error) {
	dbm := &databaseMappings{
		name:               db.Properties.Name,
		version:            db.Version,
		maxRetentionInSecs: db.Properties.MaxRetentionInSecs,
		log:                log,
	}

	for _, rules := range db.MappingRules {
		if err := dbm.applyRules(rules); err != nil {
			return nil, err
		}
	}

	return dbm, nil
}

// clone clones the database mappings
func (dbm *databaseMappings) clone() *databaseMappings {
	clone := &databaseMappings{
		name:               dbm.name,
		version:            dbm.version,
		maxRetentionInSecs: dbm.maxRetentionInSecs,
		log:                dbm.log,
		active:             make([]*activeClusterMapping, len(dbm.active)),
	}

	for n, m := range dbm.active {
		var prior *clusterMapping
		if m.prior != nil {
			prior = m.prior.clone()
		}

		clone.active[n] = &activeClusterMapping{
			clusterMapping: clusterMapping{
				cluster:          m.cluster,
				readCutoverTime:  m.readCutoverTime,
				writeCutoverTime: m.writeCutoverTime,
				prior:            prior,
			},
			shards: m.shards.Clone(),
		}
	}

	return clone
}

// update updates the database with new mapping information
func (dbm *databaseMappings) update(db *schema.Database) error {
	// Short circuit if we are already at the proper version
	if dbm.version == db.Version {
		return nil
	}

	// Apply new rules
	// NB(mmihic): Assumes mapping rule sets are sorted in ascending version order
	for _, rules := range db.MappingRules {
		if rules.ForVersion <= dbm.version {
			continue
		}

		if err := dbm.applyRules(rules); err != nil {
			return err
		}
	}

	dbm.version = db.Version
	return nil
}

// applyRules generates new mappings based on the given set of rules
func (dbm *databaseMappings) applyRules(rules *schema.ClusterMappingRuleSet) error {
	dbm.log.Infof("applying rule set %d for database %s", rules.ForVersion, dbm.name)

	dbm.log.Infof("applying %d transitions for database %s", len(rules.ShardTransitions), dbm.name)
	for _, t := range rules.ShardTransitions {
		ruleShards := bitset.From(t.Shards.Bits)

		// if there is no FromCluster, this is an initial assignment
		if t.FromCluster == "" {
			dbm.log.Infof("assigning %d shards to %s", ruleShards.Count(), t.ToCluster)
			dbm.active = append(dbm.active, &activeClusterMapping{
				shards: ruleShards,
				clusterMapping: clusterMapping{
					cluster:          t.ToCluster,
					readCutoverTime:  xtime.FromUnixMillis(t.ReadCutoverTime),
					writeCutoverTime: xtime.FromUnixMillis(t.WriteCutoverTime),
				},
			})
			continue
		}

		// transition from prior clusters
		for _, a := range dbm.active {
			shards := a.shards.Intersection(ruleShards)
			if shards.None() {
				continue
			}

			if a.cluster != t.FromCluster {
				// Gah! Something is broken
				dbm.log.Errorf(`expecting to transition %d shards from cluster %s to 
cluster %s but those shards are currently assigned to %s`,
					shards.Count(), t.FromCluster, t.ToCluster, a.cluster)
				return errInvalidTransition
			}

			// create a mapping that closes the currently active mapping, then create
			// a new active mapping pointing to the new cluster
			dbm.log.Infof("transitioning %d shards from cluster %s:%s to %s:%s",
				shards.Count(), dbm.name, t.FromCluster, dbm.name, t.ToCluster)
			dbm.active = append(dbm.active, &activeClusterMapping{
				shards: shards,
				clusterMapping: clusterMapping{
					cluster:          t.ToCluster,
					readCutoverTime:  xtime.FromUnixMillis(t.ReadCutoverTime),
					writeCutoverTime: xtime.FromUnixMillis(t.WriteCutoverTime),
					prior: &clusterMapping{
						cluster:          a.cluster,
						readCutoverTime:  a.readCutoverTime,
						writeCutoverTime: a.writeCutoverTime,
						cutoffTime:       xtime.FromUnixMillis(t.CutoverCompleteTime),
						prior:            a.prior,
					},
				},
			})

			// remove shards from the current active mapping, and delete that mapping
			// if it no longer has any assigned shards
			a.shards.InPlaceDifference(shards)

			// stop processing the transition if all the shards have been re-assigned
			ruleShards.InPlaceDifference(shards)
			if ruleShards.None() {
				break
			}
		}
	}

	dbm.gc()
	return nil
}

// gc garbage collects any mappings that no longer apply.
func (dbm *databaseMappings) gc() {
	// Garbage collect active mappings that are no longer used
	for n, a := range dbm.active {
		if a.shards.Any() {
			continue
		}

		dbm.active = append(dbm.active[:n], dbm.active[n+1:]...)
	}

	// TODO(mmihic): Garbage collect mappings that cutoff before the start of the
	// database max retention
}

// findActiveForShard finds the active mapping for a given shard
func (dbm *databaseMappings) findActiveForShard(shard uint) *clusterMapping {
	for _, active := range dbm.active {
		if active.shards.Test(shard) {
			return &active.clusterMapping
		}
	}

	return nil
}

// emptyClusterMappingIter is an empty iterator over cluster mappings
type emptyClusterMappingIter struct{}

func (iter emptyClusterMappingIter) Next() bool                      { return false }
func (iter emptyClusterMappingIter) Current() storage.ClusterMapping { return nil }

// clusterMappingIter is a shard specific iterator over cluster mappings
type clusterMappingIter struct {
	current, prior *clusterMapping
}

func (iter *clusterMappingIter) Next() bool {
	if iter.prior == nil {
		return false
	}

	iter.current, iter.prior = iter.prior, iter.prior.prior
	return true
}

func (iter *clusterMappingIter) Current() storage.ClusterMapping { return iter.current }

// clusterMapping is a cluster mapping, duh
type clusterMapping struct {
	cluster          string
	readCutoverTime  time.Time
	writeCutoverTime time.Time
	cutoffTime       time.Time
	prior            *clusterMapping
}

func (m clusterMapping) Cluster() string             { return m.cluster }
func (m clusterMapping) ReadCutoverTime() time.Time  { return m.readCutoverTime }
func (m clusterMapping) WriteCutoverTime() time.Time { return m.writeCutoverTime }
func (m clusterMapping) CutoffTime() time.Time       { return m.cutoffTime }
func (m clusterMapping) clone() *clusterMapping {
	var prior *clusterMapping
	if m.prior != nil {
		prior = m.prior.clone()
	}

	return &clusterMapping{
		cluster:          m.cluster,
		readCutoverTime:  m.readCutoverTime,
		writeCutoverTime: m.writeCutoverTime,
		cutoffTime:       m.cutoffTime,
		prior:            prior,
	}
}

// activeClusterMapping is a currently active cluster mapping
type activeClusterMapping struct {
	clusterMapping
	shards *bitset.BitSet
}

// sort.Interface for sorting databaseMappings by retention period
type databaseMappingsByMaxRetention []*databaseMappings

func (dbs databaseMappingsByMaxRetention) Len() int      { return len(dbs) }
func (dbs databaseMappingsByMaxRetention) Swap(i, j int) { dbs[i], dbs[j] = dbs[j], dbs[i] }
func (dbs databaseMappingsByMaxRetention) Less(i, j int) bool {
	return dbs[i].maxRetentionInSecs < dbs[j].maxRetentionInSecs
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
