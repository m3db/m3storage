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
	"github.com/m3db/m3cluster"
	"github.com/m3db/m3storage/generated/proto/schema"
	"github.com/m3db/m3x/log"
	_ "github.com/m3db/m3x/time"
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
	AddDatabase(db schema.Database) error

	// JoinCluster adds a new cluster to an existing database and rebalances
	// shards onto that cluster
	JoinCluster(db string, c schema.Cluster) error

	// DecommissionCluster marks a cluster as being decomissioned, moving
	// its shards to other clusters.  Read traffic will continue to be directed
	// to this cluster until the max retention period for this database expires
	DecommissionCluster(db, c string) error

	// CommitChanges commits and propagates any unapplied changes
	CommitChanges(opts CommitOptions) error
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
func NewStoragePlacement(kv cluster.KVStore, key string, opts StoragePlacementOptions) StoragePlacement {
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

	return storagePlacement{
		kv:    kv,
		key:   key,
		log:   logger,
		clock: c,
	}
}

type storagePlacement struct {
	kv    cluster.KVStore
	key   string
	log   xlog.Logger
	clock clock.Clock
}

func (sp storagePlacement) AddDatabase(db schema.Database) error {
	return sp.updatePlacement(func(p *schema.Placement) error {
		if _, exists := p.Databases[db.Name]; exists {
			return errDatabaseAlreadyExists
		}

		for _, add := range p.PendingChanges.DatabaseAdds {
			if add.Database.Name == db.Name {
				return errDatabaseAlreadyExists
			}
		}

		p.PendingChanges.DatabaseAdds = append(p.PendingChanges.DatabaseAdds, &schema.DatabaseAdd{
			Database: newDatabase(db),
		})
		return nil
	})
}

func (sp storagePlacement) JoinCluster(dbName string, c schema.Cluster) error {
	return sp.updatePlacement(func(p *schema.Placement) error {
		db := sp.findDatabase(p, dbName)
		if db == nil {
			return errDatabaseNotFound
		}

		if _, existing := db.CurrentLayout.Clusters[c.Name]; existing {
			return errClusterAlreadyExists
		}

		for _, joining := range db.PendingChanges.Joins {
			if joining.Cluster.Name == c.Name {
				return errClusterAlreadyExists
			}
		}

		db.PendingChanges.Joins = append(db.PendingChanges.Joins, &schema.ClusterJoin{
			Cluster: &c,
		})
		return nil
	})
}

func (sp storagePlacement) DecommissionCluster(dbName, c string) error {
	return sp.updatePlacement(func(p *schema.Placement) error {
		db := sp.findDatabase(p, dbName)
		if db == nil {
			return errDatabaseNotFound
		}

		// If this is an existing cluster, add to the list of decomissions
		if _, existing := db.CurrentLayout.Clusters[c]; existing {
			db.PendingChanges.Decomms = append(db.PendingChanges.Decomms, &schema.ClusterDecommission{
				ClusterName: c,
			})
			return nil
		}

		// If this is a pending join, delete from the join list
		for n, joining := range db.PendingChanges.Joins {
			if joining.Cluster.Name != c {
				continue
			}

			db.PendingChanges.Joins = append(db.PendingChanges.Joins[:n], db.PendingChanges.Joins[n+1:]...)
			return nil
		}

		return errClusterNotFound
	})
}

func (sp storagePlacement) CommitChanges(opts CommitOptions) error {
	return sp.updatePlacement(func(p *schema.Placement) error {
		// TODO(mmihic): Handle database adds which take over traffic from an existing database
		for _, add := range p.PendingChanges.DatabaseAdds {
			p.Databases[add.Database.Name] = add.Database
		}
		p.PendingChanges = nil
		return nil
	})
}

func (sp storagePlacement) findDatabase(p *schema.Placement, name string) *schema.Database {
	if db := p.Databases[name]; db != nil {
		return db
	}

	for _, add := range p.PendingChanges.DatabaseAdds {
		if add.Database.Name == name {
			return add.Database
		}
	}

	return nil
}

func (sp storagePlacement) updatePlacement(f func(p *schema.Placement) error) error {
	// TODO(mmihic): Add attempt count?
	// TODO(mmihic): Generalize into cluster
	for {
		v, err := sp.kv.Get(sp.key)
		if err != nil && err != cluster.ErrNotFound {
			return err
		}

		var p schema.Placement
		if err == cluster.ErrNotFound {
			p.PendingChanges = new(schema.PlacementChanges)
		} else if err := v.Get(&p); err != nil {
			return err
		}

		if err := f(&p); err != nil {
			return err
		}

		if v == nil {
			err = sp.kv.SetIfNotExists(sp.key, &p)
		} else {
			err = sp.kv.CheckAndSet(sp.key, v.Version(), &p)
		}

		if err == nil {
			return nil
		} else if err == cluster.ErrVersionMismatch || err == cluster.ErrAlreadyExists {
			return err
		}
	}
}

func newDatabase(db schema.Database) *schema.Database {
	ref := &db
	if ref.PendingChanges == nil {
		ref.PendingChanges = new(schema.DatabaseChanges)
	}

	if ref.CurrentLayout == nil {
		ref.CurrentLayout = new(schema.DatabaseLayout)
	}

	return ref
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
