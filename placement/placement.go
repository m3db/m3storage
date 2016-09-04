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
	"strings"
	"time"

	"github.com/m3db/m3cluster/changeset"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3storage/generated/proto/schema"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"

	"github.com/facebookgo/clock"
	"github.com/golang/protobuf/proto"
	"github.com/willf/bitset"
)

var (
	errDatabaseAlreadyExists           = errors.New("database already exists")
	errDatabaseNotFound                = errors.New("database not found")
	errClusterAlreadyExists            = errors.New("cluster already exists")
	errClusterNotFound                 = errors.New("cluster not found")
	errDatabaseRetentionPeriodConflict = errors.New("retention period conflicts with another database")

	errDatabaseInvalidName               = errors.New("database name cannot be empty")
	errDatabaseInvalidNumShards          = errors.New("database number of shards cannot be <= 0")
	errDatabaseInvalidMaxRetentionInSecs = errors.New("database max retention in seconds cannot be <= 0")
	errClusterInvalidName                = errors.New("cluster name cannot be empty")
	errClusterInvalidWeight              = errors.New("cluster weight cannot be <= 0")
	errClusterInvalidType                = errors.New("cluster type cannot be empty")

	errShardsAbandoned = errors.New("shards abandoned")
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

// NewCommitOptions returns an empty set of CommitOptions
func NewCommitOptions() CommitOptions { return new(commitOptions) }

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
	GetClock() clock.Clock
	Clock(c clock.Clock) StoragePlacementOptions

	// Logger is the logger to use in placement
	GetLogger() xlog.Logger
	Logger(l xlog.Logger) StoragePlacementOptions
}

// NewStoragePlacementOptions creates new StoragePlacementOptions
func NewStoragePlacementOptions() StoragePlacementOptions { return new(storagePlacementOptions) }

// NewStoragePlacement creates a new StoragePlacement around a given config store
func NewStoragePlacement(kv kv.Store, key string, opts StoragePlacementOptions) (StoragePlacement, error) {
	var logger xlog.Logger
	var c clock.Clock
	if opts != nil {
		logger = opts.GetLogger()
		c = opts.GetClock()
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
	if db.Name == "" {
		return errDatabaseInvalidName
	}

	if db.NumShards <= 0 {
		return errDatabaseInvalidNumShards
	}

	if db.MaxRetentionInSecs <= 0 {
		return errDatabaseInvalidMaxRetentionInSecs
	}

	return sp.mgr.Change(modificationFn(func(p *schema.Placement, changes *schema.PlacementChanges) error {
		// Make sure the database doesn't already exist
		if _, exists := p.Databases[db.Name]; exists {
			sp.log.Errorf("database %s already exists", db.Name)
			return errDatabaseAlreadyExists
		}

		if _, newlyAdded := changes.DatabaseAdds[db.Name]; newlyAdded {
			sp.log.Errorf("database %s already added in this changeset", db.Name)
			return errDatabaseAlreadyExists
		}

		// Make sure the retention period for this database doesn't line up
		// directly with any other database
		for _, existing := range p.Databases {
			if existing.Properties.MaxRetentionInSecs == db.MaxRetentionInSecs {
				sp.log.Errorf("database %s retention period conflicts with existing database %s",
					db.Name, existing.Properties.Name)
				return errDatabaseRetentionPeriodConflict
			}
		}

		for _, newlyAdded := range changes.DatabaseAdds {
			if newlyAdded.Database.Properties.MaxRetentionInSecs == db.MaxRetentionInSecs {
				sp.log.Errorf("database %s retention period conflicts with newly added database %s",
					db.Name, newlyAdded.Database.Properties.Name)
				return errDatabaseRetentionPeriodConflict
			}
		}

		// Add the database
		sp.log.Infof("adding database %s (shards: %d, max retention in secs: %d)",
			db.Name, db.NumShards, db.MaxRetentionInSecs)
		now := sp.clock.Now()
		changes.DatabaseAdds[db.Name] = &schema.DatabaseAdd{
			Database: &schema.Database{
				Properties:       &db,
				CreatedAt:        xtime.ToUnixMillis(now),
				LastUpdatedAt:    xtime.ToUnixMillis(now),
				Clusters:         make(map[string]*schema.Cluster),
				ShardAssignments: make(map[string]*schema.ShardSet),
			},
		}

		changes.DatabaseChanges[db.Name] = &schema.DatabaseChanges{}
		return nil
	}))
}

func (sp storagePlacement) JoinCluster(dbName string, c schema.ClusterProperties) error {
	if c.Name == "" {
		return errClusterInvalidName
	}

	if c.Weight == 0 {
		return errClusterInvalidWeight
	}

	if c.Type == "" {
		return errClusterInvalidType
	}

	return sp.mgr.Change(modificationFn(func(p *schema.Placement, changes *schema.PlacementChanges) error {
		db, dbChanges := sp.findDatabase(p, changes, dbName)
		if db == nil {
			return errDatabaseNotFound
		}

		if _, existing := db.Clusters[c.Name]; existing {
			sp.log.Errorf("cluster %s already exists in database %s", c.Name, db.Properties.Name)
			return errClusterAlreadyExists
		}

		if _, joining := dbChanges.Joins[c.Name]; joining {
			sp.log.Errorf("cluster %s already joined to database %s in this changeset",
				c.Name, db.Properties.Name)
			return errClusterAlreadyExists
		}

		if dbChanges.Joins == nil {
			dbChanges.Joins = make(map[string]*schema.ClusterJoin)
		}

		sp.log.Infof("joining cluster %s (weight:%d, type:%s) to database %s", c.Name, c.Weight, c.Type, dbName)
		dbChanges.Joins[c.Name] = &schema.ClusterJoin{
			Cluster: &schema.Cluster{
				Properties: &c,
				Status:     schema.ClusterStatus_ACTIVE,
				CreatedAt:  xtime.ToUnixMillis(sp.clock.Now()),
			},
		}
		return nil
	}))
}

func (sp storagePlacement) DecommissionCluster(dbName, cName string) error {
	return sp.mgr.Change(modificationFn(func(p *schema.Placement, changes *schema.PlacementChanges) error {
		db, dbChanges := sp.findDatabase(p, changes, dbName)
		if db == nil {
			return errDatabaseNotFound
		}

		// If this is an existing cluster, add to the list of decomissions if it's not already there
		if _, existing := db.Clusters[cName]; existing {
			if dbChanges.Decomms == nil {
				dbChanges.Decomms = make(map[string]*schema.ClusterDecommission)
			}

			sp.log.Infof("decommissioning cluster %s in database %s", cName, dbName)
			dbChanges.Decomms[cName] = &schema.ClusterDecommission{
				ClusterName: cName,
			}
			return nil
		}

		// If this is a pending join, delete from the join list
		if _, joining := dbChanges.Joins[cName]; joining {
			sp.log.Infof("removing pending cluster %s from database %s", cName, dbName)
			delete(dbChanges.Joins, cName)
			return nil
		}
		return errClusterNotFound
	}))
}

func (sp storagePlacement) GetPendingChanges() (int, *schema.Placement, *schema.PlacementChanges, error) {
	vers, protoConfig, protoChanges, err := sp.mgr.GetPendingChanges()

	var p *schema.Placement
	if protoConfig != nil {
		p = protoConfig.(*schema.Placement)
	}

	var changes *schema.PlacementChanges
	if protoChanges != nil {
		changes = protoChanges.(*schema.PlacementChanges)
	}

	return vers, p, changes, err
}

func (sp storagePlacement) CommitChanges(version int, opts CommitOptions) error {
	return sp.mgr.Commit(version, modificationFn(func(p *schema.Placement, changes *schema.PlacementChanges) error {
		sp.log.Infof("committing changes at version %d", version)

		// Pull all existing databases, and order them by max retention period.  This will be used
		// to determine if any new databases are pulling traffic from an existing database
		existingDatabases := make([]*schema.Database, 0, len(p.Databases))
		for _, existing := range p.Databases {
			existingDatabases = append(existingDatabases, existing)
		}
		sort.Sort(databasesByRetention(existingDatabases))

		// Add new databases
		for _, add := range changes.DatabaseAdds {
			if err := sp.commitNewDatabase(p, add.Database, existingDatabases, opts); err != nil {
				return err
			}
		}

		// Process database changes, including those from new databases
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
	existingDatabases []*schema.Database,
	opts CommitOptions) error {
	sp.log.Infof("creating initial database configuration for %s", db.Properties.Name)

	// Check to see if there are any existing databases that cover this retention
	// period - if so then we need to do a staged cutover to that database
	for _, existing := range existingDatabases {
		if existing.Properties.MaxRetentionInSecs < db.Properties.MaxRetentionInSecs {
			// We're not pulling traffic from this database
			continue
		}

		var (
			readCutoverTime     = sp.clock.Now().Add(opts.GetRolloutDelay())
			writeCutoverTime    = readCutoverTime.Add(opts.GetRolloutDelay())
			cutoverCompleteTime = writeCutoverTime.Add(opts.GetTransitionDelay())
		)

		db.ReadCutoverTime = xtime.ToUnixMillis(readCutoverTime)
		db.WriteCutoverTime = xtime.ToUnixMillis(writeCutoverTime)
		db.CutoverCompleteTime = xtime.ToUnixMillis(cutoverCompleteTime)

		sp.log.Infof("database %s needs staged cutover from %s; reads starting @ %s, writes starting @ %s, reads to %s stopping @ %s",
			db.Properties.Name, existing.Properties.Name,
			readCutoverTime, writeCutoverTime, existing.Properties.Name, cutoverCompleteTime)

		break
	}

	p.Databases[db.Properties.Name] = db
	return nil
}

func (sp storagePlacement) commitDatabaseChanges(
	p *schema.Placement,
	db *schema.Database,
	changes *schema.DatabaseChanges,
	opts CommitOptions) error {

	sp.log.Infof("processing changes to database %s", db.Properties.Name)
	var (
		unowned       bitset.BitSet
		shardCutoffs  = make(map[string]*bitset.BitSet)
		shardCutovers = make(map[string]*bitset.BitSet)
	)

	initialClusters := db.Clusters == nil
	if initialClusters {
		db.Clusters = make(map[string]*schema.Cluster)

		// Every shard is unowned
		for n := uint(0); n < uint(db.Properties.NumShards); n++ {
			unowned.Set(n)
		}
	}

	if db.ShardAssignments == nil {
		db.ShardAssignments = make(map[string]*schema.ShardSet)
	}

	// Add all joining cluster to the set of active clusters
	for name, join := range changes.Joins {
		sp.log.Infof("joining cluster %s:%s", db.Properties.Name, name)
		db.Clusters[name] = join.Cluster
		db.ShardAssignments[name] = &schema.ShardSet{}
	}

	// Mark	all decomissioned clusters as such, and return their shards to the
	// unowned pool for redistribution
	for name := range changes.Decomms {
		sp.log.Infof("decommissioning cluster %s:%s", db.Properties.Name, name)

		db.Clusters[name].Status = schema.ClusterStatus_DECOMMISSIONING
		shards := db.ShardAssignments[name]
		if shards == nil {
			continue
		}

		assigned := bitset.From(shards.Bits)
		shardCutoffs[name] = assigned
		unowned.InPlaceUnion(assigned)
		db.ShardAssignments[name].Bits = nil
	}

	// Compute active clusters, sorting the names so that assignments are deterministic (for testing)
	var (
		activeClusters     = make(map[string]*schema.Cluster, len(db.Clusters))
		activeClusterNames = make([]string, 0, len(db.Clusters))
	)
	for name, c := range db.Clusters {
		if c.Status != schema.ClusterStatus_ACTIVE {
			continue
		}

		activeClusters[name] = c
		activeClusterNames = append(activeClusterNames, name)
	}
	sort.Strings(activeClusterNames)

	// Rebalance shards on the active clusters.  pass 1, return shards on
	// overweight clusters to the "unowned" pool
	desiredNumShards := sp.computeDesiredNumShards(activeClusters, int(db.Properties.NumShards))
	for _, name := range activeClusterNames {
		var (
			desired     = desiredNumShards[name]
			assigned    = bitset.From(db.ShardAssignments[name].Bits)
			numAssigned = int(assigned.Count())
		)

		if desired >= numAssigned {
			continue // Will acquire more shards in pass 2
		}

		numToRemove := numAssigned - desired
		sp.log.Infof("%s:%s wants %d shards, has %d shards, returning %d",
			db.Properties.Name, name, desired, numAssigned, numToRemove)

		removed := bitset.New(uint(db.Properties.NumShards))
		for i, e := assigned.NextSet(0); e && numToRemove > 0; i, e = assigned.NextSet(i + 1) {
			removed.Set(i)
			numToRemove--
		}

		assigned.InPlaceDifference(removed)
		unowned.InPlaceUnion(removed)
		shardCutoffs[name] = removed
		db.ShardAssignments[name].Bits = assigned.Bytes()

		sp.log.Infof("%s:%s now owns %d shards", db.Properties.Name, name, assigned.Count())
	}

	// pass 2, assign shards from the "unowned" pool to underweight clusters
	for _, name := range activeClusterNames {
		var (
			desired     = desiredNumShards[name]
			assigned    = bitset.From(db.ShardAssignments[name].Bits)
			numAssigned = int(assigned.Count())
		)

		if desired <= numAssigned {
			continue // NB(mmihic): Should never be less at this point
		}

		// TODO(mmihic): Maybe validate that the numToAdd is >= the number remaining
		numToAdd := desired - numAssigned
		sp.log.Infof("%s:%s wants %d shards, has %d shards, pulling %d shards from %d unowned",
			db.Properties.Name, name, desired, numAssigned, numToAdd, unowned.Count())

		added := bitset.New(uint(db.Properties.NumShards))
		for i, e := unowned.NextSet(0); e && numToAdd > 0; i, e = unowned.NextSet(i + 1) {
			added.Set(i)
			numToAdd--
		}

		assigned.InPlaceUnion(added)
		unowned.InPlaceDifference(added)
		db.ShardAssignments[name].Bits = assigned.Bytes()
		shardCutovers[name] = added

		sp.log.Infof("%s:%s now owns %d shards", db.Properties.Name, name, assigned.Count())
	}

	// now build out all of the rules.
	db.Version++
	rules := &schema.ClusterMappingRuleSet{
		ForVersion: db.Version,
	}

	var (
		readCutoverTime     = sp.clock.Now()
		writeCutoverTime    = readCutoverTime.Add(opts.GetRolloutDelay())
		cutoverCompleteTime = writeCutoverTime.Add(opts.GetTransitionDelay())
	)

	// Compute transitions
	for to, cutoverShards := range shardCutovers {
		for from, cutoffShards := range shardCutoffs {
			shards := cutoffShards.Intersection(cutoverShards)
			if shards.None() {
				continue
			}

			sp.log.Infof("shifting %d shards from %s:%s to %s:%s (reads starting @ %v, writes starting @ %v)",
				shards.Count(), db.Properties.Name, from, db.Properties.Name, to, readCutoverTime, writeCutoverTime)

			rules.ShardTransitions = append(rules.ShardTransitions, &schema.ShardTransitionRule{
				FromCluster:         from,
				ToCluster:           to,
				Shards:              &schema.ShardSet{Bits: shards.Bytes()},
				ReadCutoverTime:     xtime.ToUnixMillis(readCutoverTime),
				WriteCutoverTime:    xtime.ToUnixMillis(writeCutoverTime),
				CutoverCompleteTime: xtime.ToUnixMillis(cutoverCompleteTime),
			})

			cutoverShards.InPlaceDifference(shards)
			cutoffShards.InPlaceDifference(shards)

			if cutoverShards.None() {
				// All transitions out of this shard have been processed
				break
			}
		}

		// If there are shards still outstanding, create an initial rule
		if cutoverShards.Any() {
			sp.log.Infof("assigning %d initial shards to %s:%s (reads starting @ %v, writes starting @ %v)",
				cutoverShards.Count(), db.Properties.Name, to, readCutoverTime, writeCutoverTime)

			rules.ShardTransitions = append(rules.ShardTransitions, &schema.ShardTransitionRule{
				ToCluster:        to,
				Shards:           &schema.ShardSet{Bits: cutoverShards.Bytes()},
				ReadCutoverTime:  xtime.ToUnixMillis(readCutoverTime),
				WriteCutoverTime: xtime.ToUnixMillis(writeCutoverTime),
			})
		}
	}

	// We've processed all transitions, so make sure there are no shards with no place to go...
	for cutoffName, cutoffShards := range shardCutoffs {
		if cutoffShards.Any() {
			sp.log.Infof("%d shards moved off %s:%s with no place to go",
				cutoffShards.Count(), db.Properties.Name, cutoffName)
			return errShardsAbandoned
		}
	}

	// ...sort the transitions so they have a deterministic ordering...
	sort.Sort(shardTransitionsByCluster(rules.ShardTransitions))

	// ...and add to the list of mappings for the database
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
		totalWeight += c.Properties.Weight
	}

	// compute desired # of shards based on relative weight of each cluster
	var (
		oldestCluster    *schema.Cluster
		shardsRemaining  = totalShards
		desiredNumShards = make(map[string]int, len(clusters))
	)
	for name, c := range clusters {
		relativeWeight := (float64(c.Properties.Weight) / float64(totalWeight))
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
		desiredNumShards[oldestCluster.Properties.Name] += shardsRemaining
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

// modificationFn wraps a placement modification function
func modificationFn(f func(*schema.Placement, *schema.PlacementChanges) error,
) func(proto.Message, proto.Message) error {
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
		}

		if changes.DatabaseChanges == nil {
			changes.DatabaseChanges = make(map[string]*schema.DatabaseChanges)
		}

		return f(config, changes)
	}
}

// storagePlacementOptions
type storagePlacementOptions struct {
	clock  clock.Clock
	logger xlog.Logger
}

func (opts *storagePlacementOptions) GetClock() clock.Clock  { return opts.clock }
func (opts *storagePlacementOptions) GetLogger() xlog.Logger { return opts.logger }

func (opts *storagePlacementOptions) Clock(clock clock.Clock) StoragePlacementOptions {
	opts.clock = clock
	return opts
}

func (opts *storagePlacementOptions) Logger(logger xlog.Logger) StoragePlacementOptions {
	opts.logger = logger
	return opts
}

// commitOptions
type commitOptions struct {
	rolloutDelay    time.Duration
	transitionDelay time.Duration
}

func (opts *commitOptions) RolloutDelay(t time.Duration) CommitOptions {
	opts.rolloutDelay = t
	return opts
}

func (opts *commitOptions) TransitionDelay(t time.Duration) CommitOptions {
	opts.transitionDelay = t
	return opts
}

func (opts *commitOptions) GetRolloutDelay() time.Duration    { return opts.rolloutDelay }
func (opts *commitOptions) GetTransitionDelay() time.Duration { return opts.transitionDelay }

// sort.Interface for shards
type shardsInOrder []uint32

func (shards shardsInOrder) Len() int           { return len(shards) }
func (shards shardsInOrder) Swap(i, j int)      { shards[i], shards[j] = shards[j], shards[i] }
func (shards shardsInOrder) Less(i, j int) bool { return shards[i] < shards[j] }

// sort.Interface for databases by MaxRetentionInSecs
type databasesByRetention []*schema.Database

func (dbs databasesByRetention) Len() int      { return len(dbs) }
func (dbs databasesByRetention) Swap(i, j int) { dbs[i], dbs[j] = dbs[j], dbs[i] }
func (dbs databasesByRetention) Less(i, j int) bool {
	return dbs[i].Properties.MaxRetentionInSecs < dbs[j].Properties.MaxRetentionInSecs
}

// sort.Interface for ShardTransitionRule by to cluster name
type shardTransitionsByCluster []*schema.ShardTransitionRule

func (c shardTransitionsByCluster) Len() int      { return len(c) }
func (c shardTransitionsByCluster) Swap(i, j int) { c[i], c[j] = c[j], c[i] }
func (c shardTransitionsByCluster) Less(i, j int) bool {
	toOrder := strings.Compare(c[i].ToCluster, c[j].ToCluster)
	if toOrder < 0 {
		return true
	}

	if toOrder > 0 {
		return false
	}

	return strings.Compare(c[i].FromCluster, c[j].FromCluster) < 0
}

// NewShardSet returns a new shard set initialized with a list of shards
func NewShardSet(toSet ...uint) *schema.ShardSet {
	var b bitset.BitSet
	for _, n := range toSet {
		b.Set(n)
	}

	return &schema.ShardSet{
		Bits: b.Bytes(),
	}
}
