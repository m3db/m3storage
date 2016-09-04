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
	"time"

	"github.com/m3db/m3storage"
	"github.com/m3db/m3storage/generated/proto/schema"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"

	"github.com/willf/bitset"
)

var (
	errInvalidTransition = errors.New("invalid shard transition")
)

// databaseMapping are the mappings for a given database.
type databaseMapping struct {
	name               string
	version            int32
	maxRetentionInSecs int32
	active             []*activeClusterMapping
	log                xlog.Logger
}

// newDatabaseMapping returns a newly initialized set of databaseMapping
func newDatabaseMapping(db *schema.Database, log xlog.Logger) (*databaseMapping, error) {
	dbm := &databaseMapping{
		name:               db.Properties.Name,
		version:            db.Version,
		maxRetentionInSecs: db.Properties.MaxRetentionInSecs,
		log:                log,
	}

	sort.Sort(mappingRuleSetsByVersion(db.MappingRules))
	for _, rules := range db.MappingRules {
		if err := dbm.applyRules(rules); err != nil {
			return nil, err
		}
	}

	return dbm, nil
}

// clone clones the database mappings
func (dbm *databaseMapping) clone() *databaseMapping {
	clone := &databaseMapping{
		name:               dbm.name,
		version:            dbm.version,
		maxRetentionInSecs: dbm.maxRetentionInSecs,
		log:                dbm.log,
		active:             make([]*activeClusterMapping, len(dbm.active)),
	}

	for n, m := range dbm.active {
		clone.active[n] = m.clone()
	}

	return clone
}

// update updates the database with new mapping information
func (dbm *databaseMapping) update(db *schema.Database) error {
	// Short circuit if we are already at the proper version
	if dbm.version >= db.Version {
		return nil
	}

	// Apply new rules
	sort.Sort(mappingRuleSetsByVersion(db.MappingRules))
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
func (dbm *databaseMapping) applyRules(rules *schema.ClusterMappingRuleSet) error {
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
					database:         dbm.name,
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
				dbm.log.Errorf("expecting to transition %d shards from cluster %s to "+
					"cluster %s but those shards are currently assigned to %s",
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
					database:         dbm.name,
					cluster:          t.ToCluster,
					readCutoverTime:  xtime.FromUnixMillis(t.ReadCutoverTime),
					writeCutoverTime: xtime.FromUnixMillis(t.WriteCutoverTime),
					prior: &clusterMapping{
						database:         a.database,
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
func (dbm *databaseMapping) gc() {
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
func (dbm *databaseMapping) findActiveForShard(shard uint) *clusterMapping {
	for _, a := range dbm.active {
		if a.shards.Test(shard) {
			return &a.clusterMapping
		}
	}

	return nil
}

// clusterMappingIter is a shard specific iterator over cluster mappings
type clusterMappingIter struct {
	current, prior *clusterMapping
}

// Next advances the iterator, returning true if there is another mapping
func (iter *clusterMappingIter) Next() bool {
	if iter.prior == nil {
		return false
	}

	iter.current, iter.prior = iter.prior, iter.prior.prior
	return true
}

// Current returns the current mapping
func (iter *clusterMappingIter) Current() storage.ClusterMapping { return iter.current }

// Close closes the iterator
func (iter *clusterMappingIter) Close() error { return nil }

// clusterMapping is a cluster mapping
type clusterMapping struct {
	database, cluster                             string
	readCutoverTime, writeCutoverTime, cutoffTime time.Time
	prior                                         *clusterMapping
}

func (m clusterMapping) Database() string            { return m.database }
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
		database:         m.database,
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

func (m *activeClusterMapping) clone() *activeClusterMapping {
	return &activeClusterMapping{
		clusterMapping: *m.clusterMapping.clone(),
		shards:         m.shards.Clone(),
	}
}

// sort.Interface for sorting databaseMapping by retention period
type databaseMappingsByMaxRetention []*databaseMapping

func (dbs databaseMappingsByMaxRetention) Len() int      { return len(dbs) }
func (dbs databaseMappingsByMaxRetention) Swap(i, j int) { dbs[i], dbs[j] = dbs[j], dbs[i] }
func (dbs databaseMappingsByMaxRetention) Less(i, j int) bool {
	return dbs[i].maxRetentionInSecs < dbs[j].maxRetentionInSecs
}

// sort.Interface for sorting ClusterMappingRuleSets by version order
type mappingRuleSetsByVersion []*schema.ClusterMappingRuleSet

func (m mappingRuleSetsByVersion) Len() int      { return len(m) }
func (m mappingRuleSetsByVersion) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
func (m mappingRuleSetsByVersion) Less(i, j int) bool {
	return m[i].ForVersion < m[j].ForVersion
}
