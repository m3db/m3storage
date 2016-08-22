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
		return nil
	}))
}

func (sp storagePlacement) findDatabase(p *schema.Placement, changes *schema.PlacementChanges, name string,
) (*schema.Database, *schema.DatabaseChanges) {

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
