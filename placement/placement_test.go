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
	_ "encoding/json"
	"testing"
	"time"

	"github.com/facebookgo/clock"
	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3storage/generated/proto/schema"
	"github.com/m3db/m3x/time"
	"github.com/stretchr/testify/require"
)

const (
	testRetentionInSecs = int32((time.Hour * 24 * 2) / time.Second)
	testNumShards       = int32(4)
	testRolloutDelay    = time.Minute * 30
	testTransitionDelay = time.Minute * 45
)

var (
	testCommitOpts = NewCommitOptions().
		RolloutDelay(testRolloutDelay).
		TransitionDelay(testTransitionDelay)
)

func TestPlacement_AddDatabase(t *testing.T) {
	ts := newTestSuite(t)
	ts.clock.Add(time.Second * 34)

	err := ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo",
		MaxRetentionInSecs: testRetentionInSecs,
		NumShards:          testNumShards,
	})

	require.NoError(t, err)

	p := ts.latestPlacement()
	require.Equal(t, &schema.Placement{}, p)

	changes := ts.latestChanges()
	ts.requireEqualChanges(&schema.PlacementChanges{
		DatabaseAdds: map[string]*schema.DatabaseAdd{
			"foo": &schema.DatabaseAdd{
				Database: &schema.Database{
					Name:               "foo",
					MaxRetentionInSecs: testRetentionInSecs,
					NumShards:          testNumShards,
					CreatedAt:          xtime.ToUnixMillis(ts.clock.Now()),
					LastUpdatedAt:      xtime.ToUnixMillis(ts.clock.Now()),
					Clusters:           make(map[string]*schema.Cluster),
					ShardAssignments:   make(map[string]*schema.ClusterShardAssignment),
				}}},
		DatabaseChanges: map[string]*schema.DatabaseChanges{
			"foo": &schema.DatabaseChanges{}},
	}, changes)
}

func TestPlacement_AddDatabaseConflictsWithExisting(t *testing.T) {
	ts := newTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create a placement with the given database
	ts.forcePlacement(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Name: "foo",
			},
		},
	})

	// Attempting to add a database with the same name should fail
	err := ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo",
		MaxRetentionInSecs: testRetentionInSecs,
		NumShards:          testNumShards,
	})
	require.Equal(t, errDatabaseAlreadyExists, err)

	// and should not modify the placement changes
	changes := ts.latestChanges()
	ts.requireEqualChanges(&schema.PlacementChanges{
		DatabaseAdds:    map[string]*schema.DatabaseAdd{},
		DatabaseChanges: map[string]*schema.DatabaseChanges{},
	}, changes)

}

func TestPlacement_AddDatabaseConflictsWithNewlyAdded(t *testing.T) {
	ts := newTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create placement changes with that database in the adds list
	existingChanges := &schema.PlacementChanges{
		DatabaseAdds: map[string]*schema.DatabaseAdd{
			"foo": &schema.DatabaseAdd{
				Database: &schema.Database{
					Name:               "foo",
					MaxRetentionInSecs: testRetentionInSecs,
					NumShards:          testNumShards,
					CreatedAt:          xtime.ToUnixMillis(ts.clock.Now()),
					LastUpdatedAt:      xtime.ToUnixMillis(ts.clock.Now()),
					Clusters:           make(map[string]*schema.Cluster),
					ShardAssignments:   make(map[string]*schema.ClusterShardAssignment),
				}}},
		DatabaseChanges: map[string]*schema.DatabaseChanges{
			"foo": &schema.DatabaseChanges{}},
	}
	ts.forceChanges(existingChanges)

	// Should fail adding a duplicate database
	err := ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo",
		MaxRetentionInSecs: testRetentionInSecs,
		NumShards:          testNumShards,
	})
	require.Equal(t, errDatabaseAlreadyExists, err)

	// Should not modify existing changes
	ts.requireEqualChanges(ts.latestChanges(), existingChanges)
}

func TestPlacement_JoinClusterOnNewDatabase(t *testing.T) {
	ts := newTestSuite(t)
	ts.clock.Add(time.Second * 34)

	err := ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo",
		MaxRetentionInSecs: testRetentionInSecs,
		NumShards:          testNumShards,
	})

	require.NoError(t, err)

	err = ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "bar",
		Type:   "m3db",
		Weight: 256,
	})
	require.NoError(t, err)

	changes := ts.latestChanges()
	ts.requireEqualChanges(&schema.PlacementChanges{
		DatabaseAdds: map[string]*schema.DatabaseAdd{
			"foo": &schema.DatabaseAdd{
				Database: &schema.Database{
					Name:               "foo",
					MaxRetentionInSecs: testRetentionInSecs,
					NumShards:          testNumShards,
					CreatedAt:          xtime.ToUnixMillis(ts.clock.Now()),
					LastUpdatedAt:      xtime.ToUnixMillis(ts.clock.Now()),
					Clusters:           make(map[string]*schema.Cluster),
					ShardAssignments:   make(map[string]*schema.ClusterShardAssignment),
				}}},
		DatabaseChanges: map[string]*schema.DatabaseChanges{
			"foo": &schema.DatabaseChanges{
				Joins: map[string]*schema.ClusterJoin{
					"bar": &schema.ClusterJoin{
						Cluster: &schema.Cluster{
							Name:      "bar",
							Type:      "m3db",
							Status:    schema.ClusterStatus_ACTIVE,
							Weight:    256,
							CreatedAt: xtime.ToUnixMillis(ts.clock.Now()),
						},
					},
				},
			}},
	}, changes)

}

func TestPlacement_JoinClusterOnExistingDatabase(t *testing.T) {
	ts := newTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create a placement with the given database
	ts.forcePlacement(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Name: "foo",
			},
		},
	})

	// Perform join
	err := ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "bar",
		Type:   "m3db",
		Weight: 256,
	})
	require.NoError(t, err)

	// Make sure we have the right changes
	changes := ts.latestChanges()
	ts.requireEqualChanges(&schema.PlacementChanges{
		DatabaseChanges: map[string]*schema.DatabaseChanges{
			"foo": &schema.DatabaseChanges{
				Joins: map[string]*schema.ClusterJoin{
					"bar": &schema.ClusterJoin{
						Cluster: &schema.Cluster{
							Name:      "bar",
							Type:      "m3db",
							Status:    schema.ClusterStatus_ACTIVE,
							Weight:    256,
							CreatedAt: xtime.ToUnixMillis(ts.clock.Now()),
						},
					},
				},
			}},
	}, changes)
}

func TestPlacement_JoinClusterConflictsWithExisting(t *testing.T) {
	ts := newTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create a placement with the given database and cluster
	ts.forcePlacement(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Name: "foo",
				Clusters: map[string]*schema.Cluster{
					"bar": &schema.Cluster{},
				},
			},
		},
	})

	// Attempting to join should fail
	err := ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "bar",
		Type:   "m3db",
		Weight: 256,
	})
	require.Equal(t, errClusterAlreadyExists, err)

	// Shouldn't modify changes
	ts.requireEqualChanges(ts.latestChanges(), &schema.PlacementChanges{})
}

func TestPlacement_JoinClusterConflictsWithJoining(t *testing.T) {
	ts := newTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create a placement with the given database
	ts.forcePlacement(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Name: "foo",
			},
		},
	})

	// Join
	err := ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "bar",
		Type:   "m3db",
		Weight: 256,
	})
	require.NoError(t, err)

	// Attempting to join again should fail
	err = ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "bar",
		Type:   "m3db",
		Weight: 256,
	})
	require.Equal(t, errClusterAlreadyExists, err)

	// Should only have the cluster once
	ts.requireEqualChanges(&schema.PlacementChanges{
		DatabaseChanges: map[string]*schema.DatabaseChanges{
			"foo": &schema.DatabaseChanges{
				Joins: map[string]*schema.ClusterJoin{
					"bar": &schema.ClusterJoin{
						Cluster: &schema.Cluster{
							Name:      "bar",
							Type:      "m3db",
							Status:    schema.ClusterStatus_ACTIVE,
							Weight:    256,
							CreatedAt: xtime.ToUnixMillis(ts.clock.Now()),
						},
					},
				},
			}},
	}, ts.latestChanges())
}

func TestPlacement_JoinClusterNonExistentDatabase(t *testing.T) {
	ts := newTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Join
	err := ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "bar",
		Type:   "m3db",
		Weight: 256,
	})
	require.Equal(t, errDatabaseNotFound, err)
}

func TestPlacement_DecommissionExistingCluster(t *testing.T) {
	ts := newTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create a placement with the given database and cluster
	ts.forcePlacement(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Name: "foo",
				Clusters: map[string]*schema.Cluster{
					"bar": &schema.Cluster{
						Name: "bar",
					},
				},
			},
		},
	})

	// Decommission - should succeed
	err := ts.sp.DecommissionCluster("foo", "bar")
	require.NoError(t, err)

	// Should have the decommission in the list
	changes := ts.latestChanges()
	ts.requireEqualChanges(&schema.PlacementChanges{
		DatabaseChanges: map[string]*schema.DatabaseChanges{
			"foo": &schema.DatabaseChanges{
				Decomms: map[string]*schema.ClusterDecommission{
					"bar": &schema.ClusterDecommission{
						ClusterName: "bar",
					},
				},
			}},
	}, changes)
}

func TestPlacement_DecomissionJoiningCluster(t *testing.T) {
	ts := newTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create a placement with the given database and cluster
	ts.forcePlacement(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Name: "foo",
			},
		},
	})

	// Join the cluster...
	err := ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "bar",
		Type:   "m3db",
		Weight: 256,
	})
	require.NoError(t, err)

	// ...and decommission it
	err = ts.sp.DecommissionCluster("foo", "bar")
	require.NoError(t, err)

	// Should just have an empty changes list
	changes := ts.latestChanges()
	ts.requireEqualChanges(&schema.PlacementChanges{
		DatabaseChanges: map[string]*schema.DatabaseChanges{
			"foo": &schema.DatabaseChanges{},
		},
	}, changes)

}

func TestPlacement_DoubleDecommision(t *testing.T) {
	ts := newTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create a placement with the given database and cluster
	ts.forcePlacement(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Name: "foo",
				Clusters: map[string]*schema.Cluster{
					"bar": &schema.Cluster{
						Name: "bar",
					},
				},
			},
		},
	})

	// Decommission - should succeed
	err := ts.sp.DecommissionCluster("foo", "bar")
	require.NoError(t, err)

	// Decommission again - should succeed
	err = ts.sp.DecommissionCluster("foo", "bar")
	require.NoError(t, err)

	// Should have the decommission in the list
	changes := ts.latestChanges()
	ts.requireEqualChanges(&schema.PlacementChanges{
		DatabaseChanges: map[string]*schema.DatabaseChanges{
			"foo": &schema.DatabaseChanges{
				Decomms: map[string]*schema.ClusterDecommission{
					"bar": &schema.ClusterDecommission{
						ClusterName: "bar",
					},
				},
			}},
	}, changes)
}

func TestPlacement_DecomissionNonExistentCluster(t *testing.T) {
	ts := newTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create a placement with the given database but no cluster
	ts.forcePlacement(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Name: "foo",
			},
		},
	})

	// Decommission - should fail
	err := ts.sp.DecommissionCluster("foo", "bar")
	require.Equal(t, errClusterNotFound, err)

	// Should not modify changes
	ts.requireEqualChanges(&schema.PlacementChanges{}, ts.latestChanges())
}

func TestPlacement_DecomissionClusterNonExistentDatabase(t *testing.T) {
	ts := newTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Decommission - should fail
	err := ts.sp.DecommissionCluster("foo", "bar")
	require.Equal(t, errDatabaseNotFound, err)

	// Should not modify changes
	ts.requireEqualChanges(&schema.PlacementChanges{}, ts.latestChanges())
}

func TestPlacement_CommitAddDatabase(t *testing.T) {
	ts := newTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Add the database
	err := ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo",
		MaxRetentionInSecs: testRetentionInSecs,
		NumShards:          testNumShards,
	})

	require.NoError(t, err)

	// Commit the changes
	err = ts.sp.CommitChanges(ts.latestVersion(), testCommitOpts)
	require.NoError(t, err)

	// Confirm the placement looks correct
	ts.requireEqualPlacements(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Name:               "foo",
				MaxRetentionInSecs: testRetentionInSecs,
				NumShards:          testNumShards,
				CreatedAt:          xtime.ToUnixMillis(ts.clock.Now()),
				LastUpdatedAt:      xtime.ToUnixMillis(ts.clock.Now()),
				Clusters:           make(map[string]*schema.Cluster),
				ShardAssignments:   make(map[string]*schema.ClusterShardAssignment),
				Version:            1,
				MappingRules: []*schema.ClusterMappingRuleSet{
					&schema.ClusterMappingRuleSet{
						ForVersion: 1,
					},
				},
			},
		},
	}, ts.latestPlacement())

	// ...and we've moved on to a new set of changes
	require.Nil(t, ts.latestChanges())
}

func TestPlacement_CommitAddDatabaseWithGracefulCutover(t *testing.T) {
}

func TestPlacement_CommitInitialClusters(t *testing.T) {
	ts := newTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Add the database and some clusters
	require.NoError(t, ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo",
		MaxRetentionInSecs: testRetentionInSecs,
		NumShards:          testNumShards,
	}))
	require.NoError(t, ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "bar",
		Weight: uint32(testNumShards / 2),
		Type:   "m3db",
	}))
	require.NoError(t, ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "zed",
		Weight: uint32(testNumShards / 2),
		Type:   "m3db",
	}))

	createTime := xtime.ToUnixMillis(ts.clock.Now())
	ts.clock.Add(time.Second * 45)

	// Commit the changes
	err := ts.sp.CommitChanges(ts.latestVersion(), testCommitOpts)
	require.NoError(t, err)

	// Confirm the placement looks correct
	ts.requireEqualPlacements(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Name:               "foo",
				MaxRetentionInSecs: testRetentionInSecs,
				NumShards:          testNumShards,
				CreatedAt:          createTime,
				LastUpdatedAt:      createTime,
				ShardAssignments: map[string]*schema.ClusterShardAssignment{
					"bar": &schema.ClusterShardAssignment{
						Shards: []uint32{0, 1},
					},
					"zed": &schema.ClusterShardAssignment{
						Shards: []uint32{2, 3},
					},
				},
				Version: 1,
				Clusters: map[string]*schema.Cluster{
					"bar": &schema.Cluster{
						Name:      "bar",
						Weight:    uint32(testNumShards) / 2,
						Type:      "m3db",
						Status:    schema.ClusterStatus_ACTIVE,
						CreatedAt: createTime,
					},
					"zed": &schema.Cluster{
						Name:      "zed",
						Weight:    uint32(testNumShards) / 2,
						Type:      "m3db",
						Status:    schema.ClusterStatus_ACTIVE,
						CreatedAt: createTime,
					},
				},
				MappingRules: []*schema.ClusterMappingRuleSet{
					&schema.ClusterMappingRuleSet{
						ForVersion: 1,
						Cutovers: []*schema.CutoverRule{
							&schema.CutoverRule{
								ClusterName:      "bar",
								Shards:           []uint32{0, 1},
								ReadCutoverTime:  xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay)),
								WriteCutoverTime: xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay + testTransitionDelay)),
							},
							&schema.CutoverRule{
								ClusterName:      "zed",
								Shards:           []uint32{2, 3},
								ReadCutoverTime:  xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay)),
								WriteCutoverTime: xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay + testTransitionDelay)),
							},
						},
					},
				},
			},
		},
	}, ts.latestPlacement())

	// ...and we've moved on to a new set of changes
	require.Nil(t, ts.latestChanges())

}

func TestPlacement_CommitDecommissionCluster(t *testing.T) {
}

func TestPlacement_CommitJoinClusters(t *testing.T) {
}

func TestPlacement_CommitUnevenClusterDistribution(t *testing.T) {
}

func TestPlacement_CommitComplexTopologyChange(t *testing.T) {
}

type testSuite struct {
	kv    kv.Store
	sp    StoragePlacement
	t     *testing.T
	clock *clock.Mock
}

func newTestSuite(t *testing.T) *testSuite {
	kv := kv.NewFakeStore()
	clock := clock.NewMock()

	sp, err := NewStoragePlacement(kv, "p", NewStoragePlacementOptions().Clock(clock))
	require.NoError(t, err)

	return &testSuite{
		kv:    kv,
		sp:    sp,
		clock: clock,
		t:     t,
	}
}

func (ts *testSuite) latestPlacement() *schema.Placement {
	_, p, _, err := ts.sp.GetPendingChanges()
	require.NoError(ts.t, err)
	return p
}

func (ts *testSuite) latestVersion() int {
	v, _, _, err := ts.sp.GetPendingChanges()
	require.NoError(ts.t, err)
	return v
}

func (ts *testSuite) latestChanges() *schema.PlacementChanges {
	_, _, changes, err := ts.sp.GetPendingChanges()
	require.NoError(ts.t, err)
	return changes
}

func (ts *testSuite) forcePlacement(newPlacement *schema.Placement) {
	require.NoError(ts.t, ts.sp.(storagePlacement).mgr.Change(func(_, _ proto.Message) error {
		return nil
	}))

	latestVersion := ts.latestVersion()
	require.NoError(ts.t, ts.sp.(storagePlacement).mgr.Commit(latestVersion,
		func(cfg, _ proto.Message) error {
			p := cfg.(*schema.Placement)
			*p = *newPlacement
			return nil
		}))
}

func (ts *testSuite) forceChanges(newChanges *schema.PlacementChanges) {
	require.NoError(ts.t, ts.sp.(storagePlacement).mgr.Change(func(_, protoChanges proto.Message) error {
		c := protoChanges.(*schema.PlacementChanges)
		*c = *newChanges
		return nil
	}))
}

func (ts *testSuite) requireEqualPlacements(p1, p2 *schema.Placement) {
	t := ts.t

	if p1.Databases == nil {
		require.Nil(t, p2.Databases, "expected nil Databases")
	}

	for name, db1 := range p1.Databases {
		db2 := p2.Databases[name]
		require.NotNil(t, db2, "no Database named %s", name)
		ts.requireEqualDatabases(name, db1, db2)
	}

	require.Equal(t, len(p1.Databases), len(p2.Databases), "Databases")
}

func (ts *testSuite) requireEqualDatabases(dbname string, db1, db2 *schema.Database) {
	t := ts.t
	require.Equal(t, db1.Name, db2.Name, "Name for db %s", dbname)
	require.Equal(t, db1.MaxRetentionInSecs, db2.MaxRetentionInSecs, "MaxRetentionInSecs[%s]", dbname)
	require.Equal(t, db1.CreatedAt, db2.CreatedAt, "CreatedAt[%s]", dbname)
	require.Equal(t, db1.LastUpdatedAt, db2.LastUpdatedAt, "LastUpdatedAt[%s]", dbname)
	require.Equal(t, db1.DecommissionedAt, db2.DecommissionedAt, "DecommissionedAt[%s]", dbname)
	require.Equal(t, db1.ReadCutoverTime, db2.ReadCutoverTime, "ReadCutoverTime[%s]", dbname)
	require.Equal(t, db1.WriteCutoverTime, db2.WriteCutoverTime, "WriteCutoverTime[%s]", dbname)
	require.Equal(t, db1.CutoverCompleteTime, db2.CutoverCompleteTime, "CutoverCompleteTime[%s]", dbname)
	require.Equal(t, db1.Version, db2.Version, "Version[%s]", dbname)

	for cname, c1 := range db1.Clusters {
		c2 := db2.Clusters[cname]
		require.NotNil(t, c2, "no Cluster named %s in %s", cname, dbname)
		ts.requireEqualClusters(dbname, cname, c1, c2)
	}
	require.Equal(t, len(db1.Clusters), len(db2.Clusters), "Clusters[%s]", dbname)

	for cname, a1 := range db1.ShardAssignments {
		a2 := db2.ShardAssignments[cname]
		require.NotNil(t, a2, "no ShardAssigment for %s in %s", cname, dbname)
		require.Equal(t, a1.Shards, a2.Shards, "Shards[%s:%s]", dbname, cname)
	}
	require.Equal(t, len(db1.ShardAssignments), len(db2.ShardAssignments))

	require.Equal(t, len(db1.MappingRules), len(db2.MappingRules), "MappingRules[%s]", dbname)
	for i := range db1.MappingRules {
		r1, r2 := db1.MappingRules[i], db2.MappingRules[i]
		require.Equal(t, r1.ForVersion, r2.ForVersion, "ForVersion[%s:%d]", dbname, i)
		require.Equal(t, len(r1.Cutovers), len(r2.Cutovers), "Cutovers[%s:%d]", dbname, i)
		require.Equal(t, len(r1.Cutoffs), len(r2.Cutoffs), "Cutoffs[%s:%d]", dbname, i)
	}
}

func (ts *testSuite) requireEqualClusters(dbname, cname string, c1, c2 *schema.Cluster) {
	t := ts.t

	require.Equal(t, c1.Name, c2.Name, "Name[%s:%s]", dbname, cname)
	require.Equal(t, c1.Weight, c2.Weight, "Weight[%s:%s]", dbname, cname)
	require.Equal(t, c1.Status, c2.Status, "Status[%s:%s]", dbname, cname)
	require.Equal(t, c1.Type, c2.Type, "Type[%s:%s]", dbname, cname)
	require.Equal(t, c1.CreatedAt, c2.CreatedAt, "CreatedAt[%s:%s]", dbname, cname)
}

func (ts *testSuite) requireEqualChanges(c1, c2 *schema.PlacementChanges) {
	t := ts.t

	for dbname, add1 := range c1.DatabaseAdds {
		add2 := c2.DatabaseAdds[dbname]
		require.NotNil(t, add2, "No DatabaseAdd for %s", dbname)
		ts.requireEqualDatabases(dbname, add1.Database, add2.Database)
	}
	require.Equal(t, len(c1.DatabaseAdds), len(c2.DatabaseAdds))

	for dbname, dbchange1 := range c1.DatabaseChanges {
		dbchange2 := c2.DatabaseChanges[dbname]
		require.NotNil(t, dbchange2, "No DatabaseChange for %s", dbname)
		ts.requireEqualDatabaseChanges(dbname, dbchange1, dbchange2)
	}
	require.Equal(t, len(c1.DatabaseChanges), len(c2.DatabaseChanges))
}

func (ts *testSuite) requireEqualDatabaseChanges(dbname string, c1, c2 *schema.DatabaseChanges) {
	t := ts.t

	for cname, j1 := range c1.Joins {
		j2 := c2.Joins[cname]
		require.NotNil(t, j2, "no join for %s in %s", cname, dbname)
		ts.requireEqualClusters(dbname, cname, j1.Cluster, j2.Cluster)
	}

	for cname, d1 := range c1.Decomms {
		d2 := c2.Decomms[cname]
		require.NotNil(t, d2, "no decomm for %s in %s", cname, dbname)
		require.Equal(t, d1.ClusterName, d2.ClusterName, "ClusterName[%s:%s]", dbname, cname)
	}
}
