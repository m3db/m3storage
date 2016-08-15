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
	"time"

	"github.com/m3db/m3cluster"
	"github.com/m3db/m3storage"
)

// CommitOptions are options for performing a commit
type CommitOptions interface {
	// RolloutDelay is the amount of time to wait for the configuration to be
	// distributed across all interested listeners
	RolloutDelay() time.Duration
	SetRolloutDelay(t time.Duration) CommitOptions

	// TransitionDelay is the amount of time to wait for the cluster to converge
	// on the same state after a transition
	TransitionDelay() time.Duration
	SetTransitionDelay(t time.Duration) CommitOptions
}

// StoragePlacement handles mapping shards
type StoragePlacement interface {
	// AddDatabase adds a new database to handle a set of retention periods
	AddDatabase(db storage.Database) error

	// JoinCluster adds a new cluster to an existing database and rebalances
	// shards onto that cluster
	JoinCluster(c storage.Cluster) error

	// ReplaceCluster adds a new cluster as a replacement for an existing
	// cluster, taking on all of that cluster's shards
	ReplaceCluster(c storage.Cluster, forCluster string) error

	// DecommissionCluster marks a cluster as being decomissioned, moving
	// its shards to other clusters.  Read traffic will continue to be directed
	// to this cluster until the max retention period for this database expires
	DecommissionCluster(c string) error

	// CommitChanges commits and propagates any unapplied changes
	CommitChanges(opts CommitOptions) error
}

type decommission struct {
	Cluster string
}

type join struct {
	Cluster *cluster
}

type database struct {
	Name                string          `json:"name"`
	NumShards           int             `json:"num-shards"`
	RetentionPeriods    []time.Duration `json:"retention-periods"`
	ReadCutoverTime     time.Time       `json:"read-cutover"`
	WriteCutoverTime    time.Time       `json:"write-cutover"`
	CutoverCompleteTime time.Time       `json:"cutover-complete"`
	Clusters            []*cluster      `json:"clusters"`
	Weight              int             `json:"weight"`
	ShardAssignments    clusterShardSet `json:"shard-assignments"`
}

type clusterChanges struct {
	NewClusters         []*cluster
	NewShardAssignments clusterShardSet
	Cutoffs             clusterShardSet
	Cutovers            clusterShardSet
}

// computeClusterChanges computes the necessary changes to a cluster based on
// the current state and a set of join and decommission requests
func (db *database) computeClusterChanges(joins []*join, decomms []*decommission, ops CommitOptions) (*clusterChanges, error) {
	changes := &clusterChanges{
		Cutoffs:  make(clusterShardSet),
		Cutovers: make(clusterShardSet),
	}

	// TODO(mmihic): Handle state transitions in the clusters themselves
	activeClusters := make(map[string]*cluster, 0, len(db.Clusters))
	for _, c := range db.Clusters {
		if c.state == clusterActive {
			activeClusters[c.Name] = c
		}
	}

	// TODO(mmihic): Validate that we are not adding a cluster whose name conflicts with an existing cluster
	for _, j := range joins {
		activeClusters[j.Cluster.Name] = cluster
	}

	changes.NewShardAssignments = db.ShardAssignments.mkCopy()

	// Release all shards owned by decommed clusters back to the unowned pool
	// TODO(mmihic): Validate that we are not decommissioning a non-existent or already inactive cluster
	unownedShards := make(shardSet)
	for _, d := range decomms {
		delete(activeClusters, d.Cluster)
		if shards := changes.NewShardAssignments[d.Cluster]; shards != nil {
			for shard := range shards {
				unownedShards.add(shard)
				changes.Cutoffs.add(d.Cluster, shard)
			}
			delete(changes.NewShardAssignments, d.Cluster)
		}
	}

	// Rebalance shards on the active clusters.  Clusters that are overweight
	// will release shards to the unowned pool.  Clusters that are underweight
	// will acquire shards from the unowned pool.
	desiredNumShards := db.computeDesiredNumShards(activeClusters)

	// First rebalance overweight clusters by releasing some of their shards to the owned pool
	for c := range activeClusters {
		var (
			shards  = changes.NewShardAssignments[c]
			desired = desiredNumShards[c]
		)

		// Release shards back to the unowned pool until the cluster has the proper weight
		for len(shards) > desired {
			shard := shards.pop()
			unownedShards.add(shard)
			changes.Cutoffs.add(c, shard)
		}
	}

	// Now rebalance underweight clusters by acquiring unowned shards
	for c := range activeClusters {
		var (
			shards  = changes.NewShardAssignments[c]
			desired = desiredNumShards[c]
		)

		if shards == nil {
			shards = make(shardSet)
			changes.NewShardAssignments[c] = shards
		}

		// Release shards back to the unowned pool until the cluster has the proper weight
		for len(shards) < desired {
			shard := unownedShards.pop()
			shards.add(shard)
			changes.Cutoffs.add(c, shard)
		}
	}

	// TODO(mmihic): Update changes.NewClusters with the new cluster states
	return changes, nil
}

// computeDesiredNumShards computes the desired number of shards to assign to
// each cluster, based on the current cluster weight
func (db *database) computeDesiredNumShards(clusters map[string]*cluster) map[string]int {
	if len(clusters) == 0 {
		return nil
	}

	// compute total weight of all active clusters
	var totalWeight int
	for _, c := range clusters {
		totalWeight += c.Weight
	}

	// compute desired # of shards based on relative weight of each cluster
	var (
		oldestCluster    *cluster
		shardsRemaining  = db.NumShards
		desiredNumShards = make(map[string]int, len(clusters))
	)
	for name, c := range clusters {
		relativeWeight := 100 * (float64(c.Weight) / float64(totalWeight))
		numShards := int(relativeWeight * float64(db.NumShards))
		if numShards > shardsRemaining {
			numShards = shardsRemaining
		}

		desiredNumShards[name] = numShards
		shardsRemaining -= numShards

		if oldestCluster == nil || oldestCluster.CreatedAt.After(c.CreatedAt) {
			oldestCluster = c
		}
	}

	// if we haven't yet allocated all shards due to rounding, pick oldest cluster and
	// add the remainder to it. we pick the oldest cluster to have a deterministic
	// mapping and avoid moving shards around unnecessarily
	if shardsRemaining > 0 {
		desiredNumShards[oldestCluster.Name] += shardsRemaining
	}
	return desiredNumShards
}

type clusterState string

const (
	clusterActive         clusterState = "active"
	clusterDecomissioning clusterState = "decommissioning"
	clusterDecomissioned  clusterState = "decomissioned"
)

type cluster struct {
	Name      string       `json:"name"`
	State     clusterState `json:"state"`
	CreatedAt time.Time    `json:"created-at"`
}

type shardSet map[uint32]struct{}

func (s shardSet) add(shard uint32) { s[shard] = struct{}{} }
func (s shardSet) pop() uint32 {
	for shard := range s {
		delete(s, shard)
		return shard
	}

	return uint32(0)
}

func (s shardSet) mkCopy() shardSet {
	cp := make(shardSet, len(s))
	for shard := range s {
		cp.add(shard)
	}
	return cp
}

type clusterShardSets map[string]shardSet

func (s clusterShardSets) add(c string, shard uint32) {
	shards := s[c]
	if shards == nil {
		shards = make(shardSet)
		s[c] = shards
	}

	shards.add(shard)
}

func (s clusterShardSets) mkCopy() clusterShardSets {
	cp := make(clusterShardSet, len(s))
	for c, shards := range s {
		cp[c] = shards.mkCopy()
	}

	return cp
}

type shardAssignment struct {
	shards  map[uint32]struct{}
	cluster string
}
