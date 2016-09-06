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

package mapping

import (
	"errors"
	"sort"
	"time"

	"github.com/m3db/m3storage/generated/proto/schema"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"

	"github.com/willf/bitset"
)

var (
	errInvalidShardTransition = errors.New("invalid shard transition")
)

// databaseRules contain the rules for a database
type databaseRules struct {
	name               string
	version            int32
	maxRetentionInSecs int32
	active             []*activeRule
	log                xlog.Logger
}

// newDatabaseRules creates a new set of databaseRules
func newDatabaseRules(dbConfig *schema.Database, log xlog.Logger) (*databaseRules, error) {
	dbr := &databaseRules{
		name:    dbConfig.Properties.Name,
		version: -1, // apply all rules
		log:     log,
	}

	if err := dbr.applyNewTransitions(dbConfig.Version, dbConfig.MappingRules); err != nil {
		return nil, err
	}

	return dbr, nil
}

// clone clones a set of databaseRules
func (dbr *databaseRules) clone() *databaseRules {
	clone := &databaseRules{
		name:               dbr.name,
		version:            -1, // apply all rules
		maxRetentionInSecs: dbr.maxRetentionInSecs,
		active:             make([]*activeRule, len(dbr.active)),
		log:                dbr.log,
	}

	for n := range dbr.active {
		clone.active[n] = dbr.active[n].clone()
	}

	return clone
}

// applyNewTransitions applies unapplied shard transitions
func (dbr *databaseRules) applyNewTransitions(toVersion int32, rules []*schema.ClusterMappingRuleSet) error {
	// Short circuit if we are already at the proper version
	if dbr.version >= toVersion {
		return nil
	}

	// Apply new rules, also determining which cluster versions have changed
	sort.Sort(ruleSetsByVersion(rules))
	for _, r := range rules {
		if r.ForVersion <= dbr.version {
			continue
		}

		dbr.log.Infof("applying transitions for %s v%d", dbr.name, r.ForVersion)
		if err := dbr.applyTransitions(r.ShardTransitions); err != nil {
			return err
		}
	}

	return nil
}

// applyTransitions applies a set of shard transitions
func (dbr *databaseRules) applyTransitions(transitions []*schema.ShardTransitionRule) error {
	dbr.log.Infof("applying %d transitions for database %s", len(transitions), dbr.name)
	for _, t := range transitions {
		ruleShards := bitset.From(t.Shards.Bits)

		// if there is no FromCluster, this is an initial assignment
		if t.FromCluster == "" {
			dbr.log.Infof("assigning %d shards to %s", ruleShards.Count(), t.ToCluster)
			dbr.active = append(dbr.active, &activeRule{
				shards: ruleShards,
				rule: &rule{
					database:         dbr.name,
					cluster:          t.ToCluster,
					readCutoverTime:  xtime.FromUnixMillis(t.ReadCutoverTime),
					writeCutoverTime: xtime.FromUnixMillis(t.WriteCutoverTime),
				},
			})
			continue
		}

		// transition from prior clusters
		for _, r := range dbr.active {
			shards := r.shards.Intersection(ruleShards)
			if shards.None() {
				continue
			}

			if r.cluster != t.FromCluster {
				// Gah! Something is broken
				dbr.log.Errorf("expecting to transition %d shards from cluster %s to "+
					"cluster %s but those shards are currently assigned to %s",
					shards.Count(), t.FromCluster, t.ToCluster, r.cluster)
				return errInvalidShardTransition
			}

			// create a mapping that closes the currently active mapping, then create
			// a new active mapping pointing to the new cluster
			dbr.log.Infof("transitioning %d shards from cluster %s:%s to %s:%s",
				shards.Count(), dbr.name, t.FromCluster, dbr.name, t.ToCluster)
			dbr.active = append(dbr.active, &activeRule{
				shards: shards,
				rule: &rule{
					database:         dbr.name,
					cluster:          t.ToCluster,
					readCutoverTime:  xtime.FromUnixMillis(t.ReadCutoverTime),
					writeCutoverTime: xtime.FromUnixMillis(t.WriteCutoverTime),
					prior: &rule{
						database:         r.database,
						cluster:          r.cluster,
						readCutoverTime:  r.readCutoverTime,
						writeCutoverTime: r.writeCutoverTime,
						cutoffTime:       xtime.FromUnixMillis(t.CutoverCompleteTime),
						prior:            r.prior,
					},
				},
			})

			// remove shards from the current active mapping, and delete that mapping
			// if it no longer has any assigned shards
			r.shards.InPlaceDifference(shards)

			// stop processing the transition if all the shards have been re-assigned
			ruleShards.InPlaceDifference(shards)
			if ruleShards.None() {
				break
			}
		}
	}

	dbr.gc()
	return nil
}

// findRulesForShard finds the active rules for a given shard
func (dbr *databaseRules) findRulesForShard(shard uint) *rule {
	for _, r := range dbr.active {
		if r.shards.Test(shard) {
			return r.rule
		}
	}

	return nil
}

// gc garbage collects expired and empty rules
func (dbr *databaseRules) gc() {
	for n, r := range dbr.active {
		if r.shards.Any() {
			continue
		}

		dbr.active = append(dbr.active[:n], dbr.active[n+1:]...)
	}

	// TODO(mmihic): Garbage collect old rules that cutoff before the start of the
	// database max retention
}

// rule is an individual routing rule
type rule struct {
	database, cluster                             string
	readCutoverTime, writeCutoverTime, cutoffTime time.Time
	prior                                         *rule
}

func (r rule) Database() string            { return r.database }
func (r rule) Cluster() string             { return r.cluster }
func (r rule) ReadCutoverTime() time.Time  { return r.readCutoverTime }
func (r rule) WriteCutoverTime() time.Time { return r.writeCutoverTime }
func (r rule) CutoffTime() time.Time       { return r.cutoffTime }

// clone clones the rule
func (r rule) clone() *rule {
	var prior *rule
	if r.prior != nil {
		prior = r.prior.clone()
	}

	return &rule{
		database:         r.database,
		cluster:          r.cluster,
		readCutoverTime:  r.readCutoverTime,
		writeCutoverTime: r.writeCutoverTime,
		cutoffTime:       r.cutoffTime,
		prior:            prior,
	}
}

// activeRule is a currently active routing rule, tracking
// which shards are route using that rule
type activeRule struct {
	*rule
	shards *bitset.BitSet
}

// clone clones the active rule
func (r *activeRule) clone() *activeRule {
	return &activeRule{
		rule:   r.rule.clone(),
		shards: r.shards.Clone(),
	}
}

// A ruleIter iterates over a rule chain
type ruleIter struct {
	current, prior *rule
}

// Next advances the iterator
func (iter *ruleIter) Next() bool {
	if iter.prior == nil {
		return false
	}

	iter.current, iter.prior = iter.prior, iter.prior.prior
	return true
}

// Current returns the current rule
func (iter *ruleIter) Current() Rule { return iter.current }

// Close closes the iterator
func (iter *ruleIter) Close() error { return nil }

// sort.Interface for sorting ClusterMappingRuleSets by version order
type ruleSetsByVersion []*schema.ClusterMappingRuleSet

func (m ruleSetsByVersion) Len() int      { return len(m) }
func (m ruleSetsByVersion) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
func (m ruleSetsByVersion) Less(i, j int) bool {
	return m[i].ForVersion < m[j].ForVersion
}
