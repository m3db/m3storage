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

	"github.com/m3db/m3storage"
	"github.com/m3db/m3storage/generated/proto/schema"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"

	"github.com/facebookgo/clock"
	"github.com/willf/bitset"
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

	log   xlog.Logger
	clock clock.Clock
}

// NewClusterMappingProvider returns a new ClusterMappingProvider
// around a kv store and key
// TODO(mmihic): Add in the kv store stuff
func NewClusterMappingProvider(opts ClusterMappingProviderOptions) (storage.ClusterMappingProvider, error) {
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

	return &clusterMappingProvider{
		byName: make(map[string]*databaseMappings),
		log:    log,
		clock:  c,
	}, nil
}

// MappingsForShard returns the mappings for a given shard and retention policy
func (mp *clusterMappingProvider) QueryMappings(
	shard uint32, start, end time.Time) storage.ClusterMappingIter {

	// Figure out how far back this query goes
	queryAgeInSecs := int32(mp.clock.Now().Sub(end) / time.Second)

	// Figure out which database mapping to use given the age of the datapoints being queried
	dbm := mp.findDatabaseMappings(queryAgeInSecs)
	if dbm == nil {
		return emptyClusterMappingIter{}
	}

	return &clusterMappingIter{
		mappings: dbm.mappings,
		shard:    uint(shard),
	}
}

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
func (mp *clusterMappingProvider) update(p *schema.Placement) {
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
			existing.update(db)
			continue
		}

		// This is a new mapping - create and apply all rules
		dbm := newDatabaseMappings(db, mp.log)
		byName[db.Properties.Name] = dbm
		byRetention = append(byRetention, dbm)
	}

	sort.Sort(databaseMappingsByMaxRetention(byRetention))

	// Swap out the pointers
	mp.Lock()
	mp.byRetention, mp.byName = byRetention, byName
	mp.Unlock()
}

// databaseMappings are the mappings for a given database.  databaseMappings is
// optimized for space efficiency assuming a reasonable level of density in
// shards to clusters, and a limited number of mapping rules.  If the number of
// mapping rules grows beyond 32 we might want to change the data structure to
// something threaded so that a given shard only needs to iterate through the
// mappings that it once belonged to.
type databaseMappings struct {
	name               string
	version            int32
	maxRetentionInSecs int32
	mappings           []*clusterMapping
	log                xlog.Logger
}

// newDatabaseMappings returns a newly initialized set of databaseMappings
func newDatabaseMappings(db *schema.Database, log xlog.Logger) *databaseMappings {
	dbm := &databaseMappings{
		name:               db.Properties.Name,
		version:            db.Version,
		maxRetentionInSecs: db.Properties.MaxRetentionInSecs,
		log:                log,
	}

	for _, rules := range db.MappingRules {
		dbm.applyRules(rules)
	}

	return dbm
}

// clone clones the database mappings
func (dbm *databaseMappings) clone() *databaseMappings {
	clone := &databaseMappings{
		name:               dbm.name,
		version:            dbm.version,
		maxRetentionInSecs: dbm.maxRetentionInSecs,
		log:                dbm.log,
		mappings:           make([]*clusterMapping, len(dbm.mappings)),
	}

	for n, m := range dbm.mappings {
		clone.mappings[n] = m.clone()
	}

	return clone
}

// update updates the database with new mapping information
func (dbm *databaseMappings) update(db *schema.Database) {
	// Short circuit if we are already at the proper version
	if dbm.version == db.Version {
		return
	}

	// Apply new rules
	// NB(mmihic): Assumes mapping rule sets are sorted in ascending version order
	for _, rules := range db.MappingRules {
		if rules.ForVersion <= dbm.version {
			continue
		}

		dbm.applyRules(rules)
	}

	dbm.version = db.Version
}

// applyRules generates new mappings based on the given set of rules
func (dbm *databaseMappings) applyRules(rules *schema.ClusterMappingRuleSet) {
	dbm.log.Infof("applying rule set %d for database %s", rules.ForVersion, dbm.name)

	dbm.log.Infof("applying %d cutoffs for database %s", len(rules.Cutoffs), dbm.name)
	for _, r := range rules.Cutoffs {
		ruleShards := bitset.From(r.Shards.Bits)
		for n, mapping := range dbm.mappings {
			if mapping.cluster != r.ClusterName {
				continue
			}

			if !mapping.cutoffTime.IsZero() {
				continue
			}

			shards := mapping.shards.Intersection(ruleShards)
			if shards.None() {
				continue
			}

			dbm.log.Infof("moved %d shards off cluster %s:%s", shards.Count(), r.ClusterName, dbm.name)

			dbm.mappings = append(dbm.mappings, &clusterMapping{
				cluster:          mapping.cluster,
				readCutoverTime:  mapping.readCutoverTime,
				writeCutoverTime: mapping.writeCutoverTime,
				cutoffTime:       xtime.FromUnixMillis(r.CutoffTime),
				shards:           ruleShards,
			})

			mapping.shards.InPlaceDifference(ruleShards)
			if mapping.shards.None() {
				// mapping is no longer used, so get rid of it
				dbm.mappings = append(dbm.mappings[:n-1], dbm.mappings[n:]...)
			}
		}
	}

	dbm.log.Infof("applying %d cutovers for %s", len(rules.Cutovers), dbm.name)
	for _, r := range rules.Cutovers {
		ruleShards := bitset.From(r.Shards.Bits)
		dbm.mappings = append(dbm.mappings, &clusterMapping{
			cluster:          r.ClusterName,
			readCutoverTime:  xtime.FromUnixMillis(r.ReadCutoverTime),
			writeCutoverTime: xtime.FromUnixMillis(r.WriteCutoverTime),
			shards:           ruleShards,
		})

		dbm.log.Infof("moved %d shards to cluster %s:%s", ruleShards.Count(), r.ClusterName, dbm.name)
	}

	sort.Sort(clusterMappingsByCutoff(dbm.mappings))
}

// emptyClusterMappingIter is an empty iterator over cluster mappings
type emptyClusterMappingIter struct{}

func (iter emptyClusterMappingIter) Next() bool                      { return false }
func (iter emptyClusterMappingIter) Current() storage.ClusterMapping { return nil }

// clusterMappingIter is a shard specific iterator over cluster mappings
type clusterMappingIter struct {
	shard         uint
	mappings      []*clusterMapping
	current, next int
}

func (iter *clusterMappingIter) Next() bool {
	if iter.next == 0 {
		// Find the first mapping that contains that shard
		iter.next = iter.advance(0)
	}

	if iter.next >= len(iter.mappings) {
		return false
	}

	iter.current = iter.next
	iter.next = iter.advance(iter.current + 1)
	return true
}

func (iter *clusterMappingIter) advance(pos int) int {
	for pos < len(iter.mappings) && !iter.mappings[pos].shards.Test(iter.shard) {
		pos++
	}

	return pos
}

func (iter *clusterMappingIter) Current() storage.ClusterMapping {
	if iter.current >= len(iter.mappings) {
		return nil
	}

	return iter.mappings[iter.current]
}

// clusterMapping is a cluster mapping for a set of shards
type clusterMapping struct {
	cluster          string
	readCutoverTime  time.Time
	writeCutoverTime time.Time
	cutoffTime       time.Time
	shards           *bitset.BitSet
}

func (m *clusterMapping) Cluster() string             { return m.cluster }
func (m *clusterMapping) ReadCutoverTime() time.Time  { return m.readCutoverTime }
func (m *clusterMapping) WriteCutoverTime() time.Time { return m.writeCutoverTime }
func (m *clusterMapping) CutoffTime() time.Time       { return m.cutoffTime }

func (m *clusterMapping) clone() *clusterMapping {
	return &clusterMapping{
		cluster:          m.cluster,
		readCutoverTime:  m.readCutoverTime,
		writeCutoverTime: m.writeCutoverTime,
		cutoffTime:       m.cutoffTime,
		shards:           m.shards.Clone(),
	}
}

// sort.Interface for sorting databaseMappings by retention period
type databaseMappingsByMaxRetention []*databaseMappings

func (dbs databaseMappingsByMaxRetention) Len() int      { return len(dbs) }
func (dbs databaseMappingsByMaxRetention) Swap(i, j int) { dbs[i], dbs[j] = dbs[j], dbs[i] }
func (dbs databaseMappingsByMaxRetention) Less(i, j int) bool {
	return dbs[i].maxRetentionInSecs < dbs[j].maxRetentionInSecs
}

// sort.Interface for sorting clusterMappings by cutoff time, latest cutoff first
type clusterMappingsByCutoff []*clusterMapping

func (m clusterMappingsByCutoff) Len() int      { return len(m) }
func (m clusterMappingsByCutoff) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
func (m clusterMappingsByCutoff) Less(i, j int) bool {
	if m[i].cutoffTime.IsZero() {
		return true
	}

	if m[j].cutoffTime.IsZero() {
		return false
	}

	return m[i].cutoffTime.After(m[j].cutoffTime)
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
