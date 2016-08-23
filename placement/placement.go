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
	"time"

	"github.com/facebookgo/clock"
	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3cluster/changeset"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3storage/generated/proto/schema"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"
)

var (
	errDatabaseAlreadyExists = errors.New("database already exists")
	errDatabaseNotFound      = errors.New("database not found")
	errClusterAlreadyExists  = errors.New("cluster already exists")
	errClusterNotFound       = errors.New("cluster not found")
)

// CommitOptions are options for performing a commit
type CommitOptions interface {
	// RolloutDelay is the amount of time to wait for the configuration to be
	// distributed across all interested listeners
	GetRolloutDelay() time.Duration
	RolloutDelay(t time.Duration) CommitOptions

	// TransitionDelay is the amount of time to wait for the cluster to converge
	// on the same state after a transition
	GetTransitionDelay() time.Duration
	TransitionDelay(t time.Duration) CommitOptions
}

// StoragePlacement handles mapping shards
type StoragePlacement interface {
	// AddDatabase adds a new database to handle a set of retention periods
	AddDatabase(db schema.DatabaseProperties) error

	// JoinCluster adds a new cluster to an existing database and rebalances
	// shards onto that cluster
	JoinCluster(db string, c schema.ClusterProperties) error

	// DecommissionCluster marks a cluster as being decomissioned, moving
	// its shards to other clusters.  Read traffic will continue to be directed
	// to this cluster until the max retention period for this database expires
	DecommissionCluster(db, c string) error

	// CommitChanges commits and propagates any unapplied changes
	CommitChanges(version int, opts CommitOptions) error

	// GetPendingChanges gets pending placement changes
	GetPendingChanges() (int, *schema.Placement, *schema.PlacementChanges, error)
}

// StoragePlacementOptions are options to building a storage placement
type StoragePlacementOptions interface {
	// Clock is the clock to use in placement
	Clock() clock.Clock
	SetClock(c clock.Clock) StoragePlacementOptions

	// Logger is the logger to use in placement
	Logger() xlog.Logger
	SetLogger(l xlog.Logger) StoragePlacementOptions
}

// NewStoragePlacementOptions creates new StoragePlacementOptions
func NewStoragePlacementOptions() StoragePlacementOptions { return new(storagePlacementOptions) }

// NewStoragePlacement creates a new StoragePlacement around a given config store
func NewStoragePlacement(kv kv.Store, key string, opts StoragePlacementOptions) (StoragePlacement, error) {
	var logger xlog.Logger
	var c clock.Clock
	if opts != nil {
		logger = opts.Logger()
		c = opts.Clock()
	}

	if logger == nil {
		logger = xlog.NullLogger
	}

	if c == nil {
		c = clock.New()
	}

	mgr, err := changeset.NewManager(changeset.NewManagerOptions().
		KV(kv).
		ConfigType(&schema.Placement{}).
		ChangesType(&schema.PlacementChanges{}).
		ConfigKey(key).
		Logger(logger))
	if err != nil {
		return nil, err
	}

	return storagePlacement{
		key:   key,
		log:   logger,
		mgr:   mgr,
		clock: c,
	}, nil
}

type storagePlacement struct {
	key   string
	log   xlog.Logger
	mgr   changeset.Manager
	clock clock.Clock
}

func (sp storagePlacement) AddDatabase(db schema.DatabaseProperties) error {
	return sp.mgr.Change(wrapFn(func(p *schema.Placement, changes *schema.PlacementChanges) error {
		if _, exists := p.Databases[db.Name]; exists {
			return errDatabaseAlreadyExists
		}

		if _, newlyAdded := changes.DatabaseAdds[db.Name]; newlyAdded {
			return errDatabaseAlreadyExists
		}

		now := sp.clock.Now()
		changes.DatabaseAdds[db.Name] = &schema.DatabaseAdd{
			Database: &schema.Database{
				Name:               db.Name,
				NumShards:          db.NumShards,
				MaxRetentionInSecs: db.MaxRetentionInSecs,
				CreatedAt:          xtime.ToUnixMillis(now),
				LastUpdatedAt:      xtime.ToUnixMillis(now),
				Clusters:           make(map[string]*schema.Cluster),
				ShardAssignments:   make(map[string]*schema.ClusterShardAssignment),
			},
		}

		changes.DatabaseChanges[db.Name] = &schema.DatabaseChanges{}
		return nil
	}))
}

func (sp storagePlacement) JoinCluster(dbName string, c schema.ClusterProperties) error {
	return sp.mgr.Change(wrapFn(func(p *schema.Placement, changes *schema.PlacementChanges) error {
		db, dbChanges := sp.findDatabase(p, changes, dbName)
		if db == nil {
			return errDatabaseNotFound
		}

		if _, existing := db.Clusters[c.Name]; existing {
			return errClusterAlreadyExists
		}

		if _, joining := dbChanges.Joins[c.Name]; joining {
			return errClusterAlreadyExists
		}

		dbChanges.Joins[c.Name] = &schema.ClusterJoin{
			Cluster: &schema.Cluster{
				Name:      c.Name,
				Type:      c.Type,
				Weight:    c.Weight,
				Status:    schema.ClusterStatus_ACTIVE,
				CreatedAt: xtime.ToUnixMillis(sp.clock.Now()),
			},
		}
		return nil
	}))
}

func (sp storagePlacement) DecommissionCluster(dbName, cName string) error {
	return sp.mgr.Change(wrapFn(func(p *schema.Placement, changes *schema.PlacementChanges) error {
		db, dbChanges := sp.findDatabase(p, changes, dbName)
		if db == nil {
			return errDatabaseNotFound
		}

		// If this is an existing cluster, add to the list of decomissions
		if _, existing := db.Clusters[cName]; existing {
			dbChanges.Decomms[cName] = &schema.ClusterDecommission{
				ClusterName: cName,
			}
			return nil
		}

		// If this is a pending join, delete from the join list
		if _, joining := dbChanges.Joins[cName]; joining {
			delete(dbChanges.Joins, cName)
			return nil
		}
		return errClusterNotFound
	}))
}

func (sp storagePlacement) GetPendingChanges() (int, *schema.Placement, *schema.PlacementChanges, error) {
	vers, config, changes, err := sp.mgr.GetPendingChanges()
	if err != nil {
		return vers, nil, nil, err
	}

	return vers, config.(*schema.Placement), changes.(*schema.PlacementChanges), nil
}

func (sp storagePlacement) CommitChanges(version int, opts CommitOptions) error {
	return sp.mgr.Commit(version, wrapFn(func(p *schema.Placement, changes *schema.PlacementChanges) error {
		for name, add := range changes.DatabaseAdds {
			change := changes.DatabaseChanges[name]
			if err := sp.commitNewDatabase(p, add.Database, change, opts); err != nil {
				return err
			}

			// Don't double process as a set of changes
			delete(changes.DatabaseChanges, name)
		}

		for name, change := range changes.DatabaseChanges {
			if err := sp.commitDatabaseChanges(p, p.Databases[name], change, opts); err != nil {
				return err
			}
		}

		return nil
	}))
}

func (sp storagePlacement) commitNewDatabase(
	p *schema.Placement,
	db *schema.Database,
	changes *schema.DatabaseChanges,
	opts CommitOptions) error {
	// Check to see if there are any existing databases that cover this retention
	// period - if so then we need to do a staged cutover to that database
	// TODO(mmihic): This will do a staged cutover if two newly introduced databases
	// overlap with each other
	needsStagedCutover := false
	for _, existing := range p.Databases {
		needsStagedCutover = needsStagedCutover || existing.MaxRetentionInSecs > db.MaxRetentionInSecs
	}

	if needsStagedCutover {
		var (
			readCutoverTime     = sp.clock.Now().Add(opts.GetRolloutDelay())
			writeCutoverTime    = readCutoverTime.Add(opts.GetRolloutDelay())
			cutoverCompleteTime = writeCutoverTime.Add(opts.GetTransitionDelay())
		)

		db.ReadCutoverTime = xtime.ToUnixMillis(readCutoverTime)
		db.WriteCutoverTime = xtime.ToUnixMillis(writeCutoverTime)
		db.CutoverCompleteTime = xtime.ToUnixMillis(cutoverCompleteTime)
	}

	p.Databases[db.Name] = db
	return sp.commitDatabaseChanges(p, db, changes, opts)
}

func (sp storagePlacement) commitDatabaseChanges(
	p *schema.Placement,
	db *schema.Database,
	changes *schema.DatabaseChanges,
	opts CommitOptions) error {

	var (
		unowned       []uint32
		shardCutoffs  = make(map[string][]uint32)
		shardCutovers = make(map[string][]uint32)
	)

	// Add all joining cluster to the set of active clusters
	for name, join := range changes.Joins {
		db.Clusters[name] = join.Cluster
		db.ShardAssignments[name] = &schema.ClusterShardAssignment{}
	}

	// Go through all decomissioned clusters, remove them from the active
	// clusters list, and add their shards to an "unowned" pool
	for name := range changes.Decomms {
		delete(db.Clusters, name)

		shards := db.ShardAssignments[name]
		if shards == nil {
			continue
		}

		unowned = append(unowned, shards.Shards...)
		shardCutoffs[name] = append(shardCutoffs[name], shards.Shards...)
		delete(db.ShardAssignments, name)
	}

	// Rebalance shards on the active clusters.
	// pass 1, return shards on overweight clusters to the "unowned" pool
	desiredNumShards := sp.computeDesiredNumShards(db.Clusters, int(db.NumShards))
	for name := range db.Clusters {
		var (
			desired  = desiredNumShards[name]
			assigned = db.ShardAssignments[name]
			removed  []uint32
		)

		if desired >= len(assigned.Shards) {
			continue // Will acquire more shards in pass 2
		}

		numToRemove := len(assigned.Shards) - desired
		removed, assigned.Shards = assigned.Shards[:numToRemove], assigned.Shards[numToRemove+1:]
		unowned = append(unowned, removed...)
		shardCutoffs[name] = append(shardCutoffs[name], removed...)
	}

	// pass 2, assign shards from the "unowned" pool to underweight clusters
	for name := range db.Clusters {
		var (
			desired  = desiredNumShards[name]
			assigned = db.ShardAssignments[name]
			added    []uint32
		)

		if desired <= len(assigned.Shards) {
			continue // NB(mmihic): Should never be less at this point
		}

		// TODO(mmihic): Maybe validate that the numToAdd is >= the number remaining
		numToAdd := desired - len(assigned.Shards)
		added, unowned = unowned[:numToAdd], unowned[numToAdd:]
		shardCutovers[name] = append(shardCutovers[name], added...)
		assigned.Shards = append(assigned.Shards, added...)
	}

	// now build out all of the rules
	db.Version++
	rules := &schema.ClusterMappingRuleSet{
		ForVersion: db.Version,
	}

	var (
		readCutoverTime  = sp.clock.Now()
		writeCutoverTime = readCutoverTime.Add(opts.GetRolloutDelay())
		cutoffTime       = writeCutoverTime.Add(opts.GetTransitionDelay())
	)

	for name, shards := range shardCutovers {
		// TODO(mmihic): If this is a new database we don't need graceful cutovers
		rules.Cutovers = append(rules.Cutovers, &schema.CutoverRule{
			ClusterName:      name,
			Shards:           shards,
			ReadCutoverTime:  xtime.ToUnixMillis(readCutoverTime),
			WriteCutoverTime: xtime.ToUnixMillis(writeCutoverTime),
		})
	}

	for name, shards := range shardCutoffs {
		rules.Cutoffs = append(rules.Cutoffs, &schema.CutoffRule{
			ClusterName: name,
			Shards:      shards,
			CutoffTime:  xtime.ToUnixMillis(cutoffTime),
		})
	}

	db.MappingRules = append(db.MappingRules, rules)
	return nil
}

// computeDesiredNumShards computes the desired number of shards per cluster,
// based on the cluster weight and total number of shards
func (sp storagePlacement) computeDesiredNumShards(
	clusters map[string]*schema.Cluster,
	totalShards int) map[string]int {

	if len(clusters) == 0 {
		return nil
	}

	// Compute the total weight of all clusters
	var totalWeight uint32
	for _, c := range clusters {
		totalWeight += c.Weight
	}

	// compute desired # of shards based on relative weight of each cluster
	var (
		oldestCluster    *schema.Cluster
		shardsRemaining  = totalShards
		desiredNumShards = make(map[string]int, len(clusters))
	)
	for name, c := range clusters {
		relativeWeight := 100 * (float64(c.Weight) / float64(totalWeight))
		numShards := int(relativeWeight * float64(totalShards))
		if numShards > shardsRemaining {
			numShards = shardsRemaining
		}

		desiredNumShards[name] = numShards
		shardsRemaining -= numShards

		if oldestCluster == nil || oldestCluster.CreatedAt > c.CreatedAt {
			oldestCluster = c
		}
	}

	// if we haven't yet allocated all shards due to rounding, pick oldest
	// cluster and add the remainder to it. we pick the oldest cluster to have a
	// deterministic mapping and avoid moving shards around unnecessarily
	if shardsRemaining > 0 {
		desiredNumShards[oldestCluster.Name] += shardsRemaining
	}
	return desiredNumShards
}

func (sp storagePlacement) findDatabase(
	p *schema.Placement,
	changes *schema.PlacementChanges,
	name string) (*schema.Database, *schema.DatabaseChanges) {

	db := p.Databases[name]
	if db == nil {
		if add := changes.DatabaseAdds[name]; add != nil {
			db = add.Database
		}
	}

	if db == nil {
		return nil, nil
	}

	dbChanges := changes.DatabaseChanges[name]
	if dbChanges == nil {
		dbChanges = &schema.DatabaseChanges{}
		changes.DatabaseChanges[name] = dbChanges
	}

	return db, dbChanges
}

type placementUpdateFn func(*schema.Placement, *schema.PlacementChanges) error

// wrapFn wraps a placement modification function
func wrapFn(f func(*schema.Placement, *schema.PlacementChanges) error) func(proto.Message, proto.Message) error {
	return func(protoConfig, protoChanges proto.Message) error {
		var (
			config  = protoConfig.(*schema.Placement)
			changes = protoChanges.(*schema.PlacementChanges)
		)

		if config.Databases == nil {
			config.Databases = make(map[string]*schema.Database)
		}

		if changes.DatabaseAdds == nil {
			changes.DatabaseAdds = make(map[string]*schema.DatabaseAdd)
			changes.DatabaseChanges = make(map[string]*schema.DatabaseChanges)
		}

		return f(config, changes)
	}
}

type storagePlacementOptions struct {
	clock  clock.Clock
	logger xlog.Logger
}

func (opts *storagePlacementOptions) Clock() clock.Clock  { return opts.clock }
func (opts *storagePlacementOptions) Logger() xlog.Logger { return opts.logger }

func (opts *storagePlacementOptions) SetClock(clock clock.Clock) StoragePlacementOptions {
	opts.clock = clock
	return opts
}

func (opts *storagePlacementOptions) SetLogger(logger xlog.Logger) StoragePlacementOptions {
	opts.logger = logger
	return opts
}
