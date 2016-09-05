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
	"time"

	"github.com/m3db/m3storage/generated/proto/schema"
	"github.com/m3db/m3storage/mapping"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"

	"github.com/willf/bitset"
)

// mappings maintains information about all of the shard mappings in a
// database
type mappings struct {
	name   string
	active []*activeMappingRule
	log    xlog.Logger
}

func newMappings(name string, log xlog.Logger) *mappings {
	return &mappings{
		name: name,
		log:  log,
	}
}

// clone clones the mappings
func (m *mappings) clone() *mappings {
	clone := &mappings{
		name:   m.name,
		active: make([]*activeMappingRule, len(m.active)),
		log:    m.log,
	}

	for n, a := range m.active {
		clone.active[n] = a.clone()
	}

	return clone
}

// apply generates new mappings based on the given set of rules
func (m *mappings) apply(rules *schema.ClusterMappingRuleSet, newClusterVersions map[string]int) error {
	m.log.Infof("applying rule set %d for database %s", rules.ForVersion, m.name)

	m.log.Infof("applying %d transitions for database %s", len(rules.ShardTransitions), m.name)
	for _, t := range rules.ShardTransitions {
		ruleShards := bitset.From(t.Shards.Bits)

		// if there is no FromCluster, this is an initial assignment
		if t.FromCluster == "" {
			m.log.Infof("assigning %d shards to %s", ruleShards.Count(), t.ToCluster)
			m.active = append(m.active, &activeMappingRule{
				shards: ruleShards,
				mappingRule: &mappingRule{
					database:         m.name,
					cluster:          t.ToCluster,
					readCutoverTime:  xtime.FromUnixMillis(t.ReadCutoverTime),
					writeCutoverTime: xtime.FromUnixMillis(t.WriteCutoverTime),
				},
			})
			newClusterVersions[t.ToCluster] = int(rules.ForVersion)
			continue
		}

		// transition from prior clusters
		for _, a := range m.active {
			shards := a.shards.Intersection(ruleShards)
			if shards.None() {
				continue
			}

			if a.cluster != t.FromCluster {
				// Gah! Something is broken
				m.log.Errorf("expecting to transition %d shards from cluster %s to "+
					"cluster %s but those shards are currently assigned to %s",
					shards.Count(), t.FromCluster, t.ToCluster, a.cluster)
				return errInvalidTransition
			}

			// create a mapping that closes the currently active mapping, then create
			// a new active mapping pointing to the new cluster
			m.log.Infof("transitioning %d shards from cluster %s:%s to %s:%s",
				shards.Count(), m.name, t.FromCluster, m.name, t.ToCluster)
			m.active = append(m.active, &activeMappingRule{
				shards: shards,
				mappingRule: &mappingRule{
					database:         m.name,
					cluster:          t.ToCluster,
					readCutoverTime:  xtime.FromUnixMillis(t.ReadCutoverTime),
					writeCutoverTime: xtime.FromUnixMillis(t.WriteCutoverTime),
					prior: &mappingRule{
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

	// ...look at cluster configurations that have changed
	m.log.Infof("applying %d cluster config updates", len(rules.ClusterConfigUpdates))
	for _, change := range rules.ClusterConfigUpdates {
		newClusterVersions[change.ClusterName] = int(rules.ForVersion)
	}

	m.gc()
	return nil
}

// gc garbage collects any mappings that no longer apply.
func (m *mappings) gc() {
	// Garbage collect active mappings that are no longer used
	for n, a := range m.active {
		if a.shards.Any() {
			continue
		}

		m.active = append(m.active[:n], m.active[n+1:]...)
	}

	// TODO(mmihic): Garbage collect mappings that cutoff before the start of the
	// database max retention
}

// findActiveForShard finds the active mapping for a given shard
func (m *mappings) findActiveForShard(shard uint) *mappingRule {
	for _, a := range m.active {
		if a.shards.Test(shard) {
			return a.mappingRule
		}
	}

	return nil
}

// mappingRuleIter is a shard specific iterator over cluster mappings
type mappingRuleIter struct {
	current, prior *mappingRule
}

// Next advances the iterator, returning true if there is another mapping
func (iter *mappingRuleIter) Next() bool {
	if iter.prior == nil {
		return false
	}

	iter.current, iter.prior = iter.prior, iter.prior.prior
	return true
}

// Current returns the current mapping
func (iter *mappingRuleIter) Current() mapping.Rule { return iter.current }

// Close closes the iterator
func (iter *mappingRuleIter) Close() error { return nil }

// mappingRule is a cluster mapping
type mappingRule struct {
	database, cluster                             string
	readCutoverTime, writeCutoverTime, cutoffTime time.Time
	prior                                         *mappingRule
}

func (m mappingRule) Database() string            { return m.database }
func (m mappingRule) Cluster() string             { return m.cluster }
func (m mappingRule) ReadCutoverTime() time.Time  { return m.readCutoverTime }
func (m mappingRule) WriteCutoverTime() time.Time { return m.writeCutoverTime }
func (m mappingRule) CutoffTime() time.Time       { return m.cutoffTime }

func (m mappingRule) clone() *mappingRule {
	var prior *mappingRule
	if m.prior != nil {
		prior = m.prior.clone()
	}

	return &mappingRule{
		database:         m.database,
		cluster:          m.cluster,
		readCutoverTime:  m.readCutoverTime,
		writeCutoverTime: m.writeCutoverTime,
		cutoffTime:       m.cutoffTime,
		prior:            prior,
	}
}

// activeMappingRule is a currently active cluster mapping rule
type activeMappingRule struct {
	*mappingRule
	shards *bitset.BitSet
}

func (m *activeMappingRule) clone() *activeMappingRule {
	return &activeMappingRule{
		mappingRule: m.mappingRule.clone(),
		shards:      m.shards.Clone(),
	}
}
