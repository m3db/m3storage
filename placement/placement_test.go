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
	"testing"
	"time"

	"github.com/facebookgo/clock"
	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3storage/generated/proto/schema"
	"github.com/m3db/m3x/log"
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

func TestPlacement_AddDatabaseInvalid(t *testing.T) {
	ts := newPlacementTestSuite(t)

	tests := []struct {
		db  schema.DatabaseProperties
		err error
	}{
		{schema.DatabaseProperties{NumShards: 1024, MaxRetentionInSecs: 20},
			errDatabaseInvalidName},
		{schema.DatabaseProperties{Name: "Foo", MaxRetentionInSecs: 20},
			errDatabaseInvalidNumShards},
		{schema.DatabaseProperties{Name: "Foo", NumShards: -100, MaxRetentionInSecs: 20},
			errDatabaseInvalidNumShards},
		{schema.DatabaseProperties{Name: "Foo", NumShards: 1024},
			errDatabaseInvalidMaxRetentionInSecs},
		{schema.DatabaseProperties{Name: "Foo", NumShards: 1024, MaxRetentionInSecs: -100},
			errDatabaseInvalidMaxRetentionInSecs},
	}

	for _, test := range tests {
		require.Equal(t, test.err, ts.sp.AddDatabase(test.db))
	}
}

func TestPlacement_AddDatabase(t *testing.T) {
	ts := newPlacementTestSuite(t)
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
	requireEqualChanges(t, &schema.PlacementChanges{
		DatabaseAdds: map[string]*schema.DatabaseAdd{
			"foo": &schema.DatabaseAdd{
				Database: &schema.Database{
					Properties: &schema.DatabaseProperties{
						Name:               "foo",
						MaxRetentionInSecs: testRetentionInSecs,
						NumShards:          testNumShards,
					},
					CreatedAt:        xtime.ToUnixMillis(ts.clock.Now()),
					LastUpdatedAt:    xtime.ToUnixMillis(ts.clock.Now()),
					Clusters:         make(map[string]*schema.Cluster),
					ShardAssignments: make(map[string]*schema.ShardSet),
				}}},
		DatabaseChanges: map[string]*schema.DatabaseChanges{
			"foo": &schema.DatabaseChanges{}},
	}, changes)
}

func TestPlacement_AddDatabaseRetentionPeriodConflictsWithExisting(t *testing.T) {
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create a placement with the given database
	ts.forcePlacement(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Properties: &schema.DatabaseProperties{
					Name:               "foo",
					MaxRetentionInSecs: testRetentionInSecs,
				},
			},
		},
	})

	// Attempting to add a database with the same name should fail
	err := ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "another-foo",
		MaxRetentionInSecs: testRetentionInSecs,
		NumShards:          testNumShards,
	})
	require.Equal(t, errDatabaseRetentionPeriodConflict, err)

	// and should not modify the placement changes
	changes := ts.latestChanges()
	requireEqualChanges(t, &schema.PlacementChanges{
		DatabaseAdds:    map[string]*schema.DatabaseAdd{},
		DatabaseChanges: map[string]*schema.DatabaseChanges{},
	}, changes)
}

func TestPlacement_AddDatabaseNameConflictsWithExisting(t *testing.T) {
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create a placement with the given database
	ts.forcePlacement(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Properties: &schema.DatabaseProperties{
					Name: "foo",
				},
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
	requireEqualChanges(t, &schema.PlacementChanges{
		DatabaseAdds:    map[string]*schema.DatabaseAdd{},
		DatabaseChanges: map[string]*schema.DatabaseChanges{},
	}, changes)
}

func TestPlacement_AddDatabaseNameConflictsWithNewlyAdded(t *testing.T) {
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create placement changes with that database in the adds list
	existingChanges := &schema.PlacementChanges{
		DatabaseAdds: map[string]*schema.DatabaseAdd{
			"foo": &schema.DatabaseAdd{
				Database: &schema.Database{
					Properties: &schema.DatabaseProperties{
						Name:               "foo",
						MaxRetentionInSecs: testRetentionInSecs,
						NumShards:          testNumShards,
					},
					CreatedAt:        xtime.ToUnixMillis(ts.clock.Now()),
					LastUpdatedAt:    xtime.ToUnixMillis(ts.clock.Now()),
					Clusters:         make(map[string]*schema.Cluster),
					ShardAssignments: make(map[string]*schema.ShardSet),
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
	requireEqualChanges(t, ts.latestChanges(), existingChanges)
}

func TestPlacement_AddDatabaseRetentionPeriodConflictsWithNewlyAdded(t *testing.T) {
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create placement changes with that database in the adds list
	existingChanges := &schema.PlacementChanges{
		DatabaseAdds: map[string]*schema.DatabaseAdd{
			"foo": &schema.DatabaseAdd{
				Database: &schema.Database{
					Properties: &schema.DatabaseProperties{
						Name:               "foo",
						MaxRetentionInSecs: testRetentionInSecs,
						NumShards:          testNumShards,
					},
					CreatedAt:        xtime.ToUnixMillis(ts.clock.Now()),
					LastUpdatedAt:    xtime.ToUnixMillis(ts.clock.Now()),
					Clusters:         make(map[string]*schema.Cluster),
					ShardAssignments: make(map[string]*schema.ShardSet),
				}}},
		DatabaseChanges: map[string]*schema.DatabaseChanges{
			"foo": &schema.DatabaseChanges{}},
	}
	ts.forceChanges(existingChanges)

	// Should fail adding a duplicate database
	err := ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "another-foo",
		MaxRetentionInSecs: testRetentionInSecs,
		NumShards:          testNumShards,
	})
	require.Equal(t, errDatabaseRetentionPeriodConflict, err)

	// Should not modify existing changes
	requireEqualChanges(t, ts.latestChanges(), existingChanges)
}

func TestPlacement_JoinClusterInvalid(t *testing.T) {
	ts := newPlacementTestSuite(t)

	require.NoError(t, ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo",
		NumShards:          1024,
		MaxRetentionInSecs: testRetentionInSecs,
	}))

	tests := []struct {
		c   schema.ClusterProperties
		err error
	}{
		{schema.ClusterProperties{Weight: 1024, Type: "m3db"}, errClusterInvalidName},
		{schema.ClusterProperties{Name: "c1", Type: "m3db"}, errClusterInvalidWeight},
		{schema.ClusterProperties{Name: "c1", Weight: 1024}, errClusterInvalidType},
	}

	for _, test := range tests {
		require.Equal(t, test.err, ts.sp.JoinCluster("foo", test.c, nil))
	}
}

func TestPlacement_JoinClusterOnNewDatabase(t *testing.T) {
	ts := newPlacementTestSuite(t)
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
	}, []byte("my-config"))
	require.NoError(t, err)

	changes := ts.latestChanges()
	requireEqualChanges(t, &schema.PlacementChanges{
		DatabaseAdds: map[string]*schema.DatabaseAdd{
			"foo": &schema.DatabaseAdd{
				Database: &schema.Database{
					Properties: &schema.DatabaseProperties{
						Name:               "foo",
						MaxRetentionInSecs: testRetentionInSecs,
						NumShards:          testNumShards,
					},
					CreatedAt:        xtime.ToUnixMillis(ts.clock.Now()),
					LastUpdatedAt:    xtime.ToUnixMillis(ts.clock.Now()),
					Clusters:         make(map[string]*schema.Cluster),
					ShardAssignments: make(map[string]*schema.ShardSet),
				}}},
		DatabaseChanges: map[string]*schema.DatabaseChanges{
			"foo": &schema.DatabaseChanges{
				Joins: map[string]*schema.ClusterJoin{
					"bar": &schema.ClusterJoin{
						Cluster: &schema.Cluster{
							Properties: &schema.ClusterProperties{
								Name:   "bar",
								Type:   "m3db",
								Weight: 256,
							},
							Status:        schema.ClusterStatus_ACTIVE,
							CreatedAt:     xtime.ToUnixMillis(ts.clock.Now()),
							LastUpdatedAt: xtime.ToUnixMillis(ts.clock.Now()),
							Config:        []byte("my-config"),
						},
					},
				},
			}},
	}, changes)

}

func TestPlacement_JoinClusterOnExistingDatabase(t *testing.T) {
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create a placement with the given database
	ts.forcePlacement(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Properties: &schema.DatabaseProperties{
					Name: "foo",
				},
			},
		},
	})

	// Perform join
	err := ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "bar",
		Type:   "m3db",
		Weight: 256,
	}, []byte("bar-config"))
	require.NoError(t, err)

	// Make sure we have the right changes
	changes := ts.latestChanges()
	requireEqualChanges(t, &schema.PlacementChanges{
		DatabaseChanges: map[string]*schema.DatabaseChanges{
			"foo": &schema.DatabaseChanges{
				Joins: map[string]*schema.ClusterJoin{
					"bar": &schema.ClusterJoin{
						Cluster: &schema.Cluster{
							Properties: &schema.ClusterProperties{
								Name:   "bar",
								Type:   "m3db",
								Weight: 256,
							},
							Status:        schema.ClusterStatus_ACTIVE,
							CreatedAt:     xtime.ToUnixMillis(ts.clock.Now()),
							LastUpdatedAt: xtime.ToUnixMillis(ts.clock.Now()),
							Config:        []byte("bar-config"),
						},
					},
				},
			}},
	}, changes)
}

func TestPlacement_JoinClusterConflictsWithExisting(t *testing.T) {
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create a placement with the given database and cluster
	ts.forcePlacement(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Properties: &schema.DatabaseProperties{
					Name: "foo",
				},
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
	}, []byte("bar-config"))
	require.Equal(t, errClusterAlreadyExists, err)

	// Shouldn't modify changes
	requireEqualChanges(t, ts.latestChanges(), &schema.PlacementChanges{})
}

func TestPlacement_JoinClusterConflictsWithJoining(t *testing.T) {
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create a placement with the given database
	ts.forcePlacement(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Properties: &schema.DatabaseProperties{
					Name: "foo",
				},
			},
		},
	})

	// Join
	err := ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "bar",
		Type:   "m3db",
		Weight: 256,
	}, []byte("bar-config"))
	require.NoError(t, err)

	// Attempting to join again should fail
	err = ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "bar",
		Type:   "m3db",
		Weight: 256,
	}, []byte("bar-config2"))
	require.Equal(t, errClusterAlreadyExists, err)

	// Should only have the cluster once
	requireEqualChanges(t, &schema.PlacementChanges{
		DatabaseChanges: map[string]*schema.DatabaseChanges{
			"foo": &schema.DatabaseChanges{
				Joins: map[string]*schema.ClusterJoin{
					"bar": &schema.ClusterJoin{
						Cluster: &schema.Cluster{
							Properties: &schema.ClusterProperties{
								Name:   "bar",
								Type:   "m3db",
								Weight: 256,
							},
							Status:        schema.ClusterStatus_ACTIVE,
							CreatedAt:     xtime.ToUnixMillis(ts.clock.Now()),
							LastUpdatedAt: xtime.ToUnixMillis(ts.clock.Now()),
							Config:        []byte("bar-config"),
						},
					},
				},
			}},
	}, ts.latestChanges())
}

func TestPlacement_JoinClusterNonExistentDatabase(t *testing.T) {
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Join
	err := ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "bar",
		Type:   "m3db",
		Weight: 256,
	}, []byte("bar-config"))
	require.Equal(t, errDatabaseNotFound, err)
}

func TestPlacement_UpdateClusterConfigOnExisting(t *testing.T) {
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Create a database with one cluster and commit
	require.NoError(t, ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo",
		MaxRetentionInSecs: 240000,
		NumShards:          4096,
	}))
	require.NoError(t, ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "c1",
		Type:   "m3db",
		Weight: 256,
	}, []byte("c1-config1")))

	ts.commitLatest()

	// Update the config for that cluster
	require.NoError(t, ts.sp.UpdateClusterConfig("foo", "c1", []byte("c1-config2")))

	// Confirm changes are correct
	requireEqualChanges(t, &schema.PlacementChanges{
		DatabaseChanges: map[string]*schema.DatabaseChanges{
			"foo": &schema.DatabaseChanges{
				ClusterConfigUpdates: map[string][]byte{
					"c1": []byte("c1-config2"),
				},
			},
		},
	}, ts.latestChanges())
}

func TestPlacement_UpdateClusterConfigOnJoining(t *testing.T) {
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Create a database with one cluster and commit
	require.NoError(t, ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo",
		MaxRetentionInSecs: 240000,
		NumShards:          4096,
	}))
	ts.commitLatest()

	// Join a new cluster...
	require.NoError(t, ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "c1",
		Type:   "m3db",
		Weight: 256,
	}, []byte("c1-config1")))

	// ...then update its config
	require.NoError(t, ts.sp.UpdateClusterConfig("foo", "c1", []byte("c1-config2")))

	// Confirm changes are correct
	requireEqualChanges(t, &schema.PlacementChanges{
		DatabaseChanges: map[string]*schema.DatabaseChanges{
			"foo": &schema.DatabaseChanges{
				Joins: map[string]*schema.ClusterJoin{
					"c1": &schema.ClusterJoin{
						Cluster: &schema.Cluster{
							Properties: &schema.ClusterProperties{
								Name:   "c1",
								Type:   "m3db",
								Weight: 256,
							},
							Status:        schema.ClusterStatus_ACTIVE,
							CreatedAt:     xtime.ToUnixMillis(ts.clock.Now()),
							LastUpdatedAt: xtime.ToUnixMillis(ts.clock.Now()),
							Config:        []byte("c1-config2"),
						},
					},
				},
			},
		},
	}, ts.latestChanges())
}

func TestPlacement_UpdateClusterConfigOnNonExistentDatabase(t *testing.T) {
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)
	require.Equal(t, errDatabaseNotFound, ts.sp.UpdateClusterConfig("foo", "c1", []byte("c1-config2")))
}

func TestPlacement_UpdateClusterConfigOnOnExistentCluster(t *testing.T) {
	ts := newPlacementTestSuite(t)

	require.NoError(t, ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo",
		MaxRetentionInSecs: 240000,
		NumShards:          4096,
	}))
	require.Equal(t, errClusterNotFound, ts.sp.UpdateClusterConfig("foo", "c1", []byte("c1-config2")))
}

func TestPlacement_DecommissionExistingCluster(t *testing.T) {
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create a placement with the given database and cluster
	ts.forcePlacement(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Properties: &schema.DatabaseProperties{
					Name: "foo",
				},
				Clusters: map[string]*schema.Cluster{
					"bar": &schema.Cluster{
						Properties: &schema.ClusterProperties{
							Name: "bar",
						},
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
	requireEqualChanges(t, &schema.PlacementChanges{
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
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create a placement with the given database and cluster
	ts.forcePlacement(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Properties: &schema.DatabaseProperties{
					Name: "foo",
				},
			},
		},
	})

	// Join the cluster...
	err := ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "bar",
		Type:   "m3db",
		Weight: 256,
	}, []byte("bar-config"))
	require.NoError(t, err)

	// ...and decommission it
	err = ts.sp.DecommissionCluster("foo", "bar")
	require.NoError(t, err)

	// Should just have an empty changes list
	changes := ts.latestChanges()
	requireEqualChanges(t, &schema.PlacementChanges{
		DatabaseChanges: map[string]*schema.DatabaseChanges{
			"foo": &schema.DatabaseChanges{},
		},
	}, changes)

}

func TestPlacement_DoubleDecommision(t *testing.T) {
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create a placement with the given database and cluster
	ts.forcePlacement(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Properties: &schema.DatabaseProperties{
					Name: "foo",
				},
				Clusters: map[string]*schema.Cluster{
					"bar": &schema.Cluster{
						Properties: &schema.ClusterProperties{
							Name: "bar",
						},
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
	requireEqualChanges(t, &schema.PlacementChanges{
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
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Force create a placement with the given database but no cluster
	ts.forcePlacement(&schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Properties: &schema.DatabaseProperties{
					Name: "foo",
				},
			},
		},
	})

	// Decommission - should fail
	err := ts.sp.DecommissionCluster("foo", "bar")
	require.Equal(t, errClusterNotFound, err)

	// Should not modify changes
	requireEqualChanges(t, &schema.PlacementChanges{}, ts.latestChanges())
}

func TestPlacement_DecomissionClusterNonExistentDatabase(t *testing.T) {
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Decommission - should fail
	err := ts.sp.DecommissionCluster("foo", "bar")
	require.Equal(t, errDatabaseNotFound, err)

	// Should not modify changes
	requireEqualChanges(t, &schema.PlacementChanges{}, ts.latestChanges())
}

func TestPlacement_CommitAddDatabase(t *testing.T) {
	ts := newPlacementTestSuite(t)
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
	requireEqualPlacements(t, &schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Properties: &schema.DatabaseProperties{
					Name:               "foo",
					MaxRetentionInSecs: testRetentionInSecs,
					NumShards:          testNumShards,
				},
				CreatedAt:        xtime.ToUnixMillis(ts.clock.Now()),
				LastUpdatedAt:    xtime.ToUnixMillis(ts.clock.Now()),
				Clusters:         make(map[string]*schema.Cluster),
				ShardAssignments: make(map[string]*schema.ShardSet),
				Version:          1,
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

func TestPlacement_CommitMultipleOverlappingInitialDatabases(t *testing.T) {
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Add the databases
	require.NoError(t, ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo",
		MaxRetentionInSecs: testRetentionInSecs,
		NumShards:          testNumShards,
	}))
	require.NoError(t, ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo-short",
		MaxRetentionInSecs: testRetentionInSecs / 2,
		NumShards:          testNumShards,
	}))

	// Commit the changes
	require.NoError(t, ts.sp.CommitChanges(ts.latestVersion(), testCommitOpts))

	// Confirm the placement looks correct
	requireEqualPlacements(t, &schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Properties: &schema.DatabaseProperties{
					Name:               "foo",
					MaxRetentionInSecs: testRetentionInSecs,
					NumShards:          testNumShards,
				},
				CreatedAt:     xtime.ToUnixMillis(ts.clock.Now()),
				LastUpdatedAt: xtime.ToUnixMillis(ts.clock.Now()),
				Version:       1,
				MappingRules: []*schema.ClusterMappingRuleSet{
					&schema.ClusterMappingRuleSet{
						ForVersion: 1,
					},
				},
			},
			"foo-short": &schema.Database{
				Properties: &schema.DatabaseProperties{
					Name:               "foo-short",
					MaxRetentionInSecs: testRetentionInSecs / 2,
					NumShards:          testNumShards,
				},
				CreatedAt:     xtime.ToUnixMillis(ts.clock.Now()),
				LastUpdatedAt: xtime.ToUnixMillis(ts.clock.Now()),
				Version:       1,
				MappingRules: []*schema.ClusterMappingRuleSet{
					&schema.ClusterMappingRuleSet{
						ForVersion: 1,
					},
				},
			},
		},
	}, ts.latestPlacement())
}

func TestPlacement_CommitDatabaseOverlapsWithExisting(t *testing.T) {
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)

	// Add the initial databases
	require.NoError(t, ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo",
		MaxRetentionInSecs: testRetentionInSecs,
		NumShards:          testNumShards,
	}))
	require.NoError(t, ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo-short",
		MaxRetentionInSecs: testRetentionInSecs / 2,
		NumShards:          testNumShards,
	}))
	require.NoError(t, ts.sp.CommitChanges(ts.latestVersion(), testCommitOpts))

	// Add another database that overlaps with both of the existing databases
	require.NoError(t, ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo-tiny",
		MaxRetentionInSecs: testRetentionInSecs / 10,
		NumShards:          testNumShards,
	}))
	require.NoError(t, ts.sp.CommitChanges(ts.latestVersion(), testCommitOpts))

	// And add a final database that sits between the first two
	require.NoError(t, ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo-medium",
		MaxRetentionInSecs: testRetentionInSecs - int32(100),
		NumShards:          testNumShards,
	}))
	require.NoError(t, ts.sp.CommitChanges(ts.latestVersion(), testCommitOpts))

	// The new database should require graceful cutover
	requireEqualPlacements(t, &schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Properties: &schema.DatabaseProperties{
					Name:               "foo",
					MaxRetentionInSecs: testRetentionInSecs,
					NumShards:          testNumShards,
				},
				CreatedAt:     xtime.ToUnixMillis(ts.clock.Now()),
				LastUpdatedAt: xtime.ToUnixMillis(ts.clock.Now()),
				Version:       1,
				MappingRules: []*schema.ClusterMappingRuleSet{
					&schema.ClusterMappingRuleSet{
						ForVersion: 1,
					},
				},
			},
			"foo-medium": &schema.Database{
				Properties: &schema.DatabaseProperties{
					Name:               "foo-medium",
					MaxRetentionInSecs: testRetentionInSecs - int32(100),
					NumShards:          testNumShards,
				},
				CreatedAt:           xtime.ToUnixMillis(ts.clock.Now()),
				LastUpdatedAt:       xtime.ToUnixMillis(ts.clock.Now()),
				ReadCutoverTime:     xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay)),
				WriteCutoverTime:    xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay * 2)),
				CutoverCompleteTime: xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay*2 + testTransitionDelay)),
				Version:             1,
				MappingRules: []*schema.ClusterMappingRuleSet{
					&schema.ClusterMappingRuleSet{
						ForVersion: 1,
					},
				},
			},
			"foo-short": &schema.Database{
				Properties: &schema.DatabaseProperties{
					Name:               "foo-short",
					MaxRetentionInSecs: testRetentionInSecs / 2,
					NumShards:          testNumShards,
				},
				CreatedAt:     xtime.ToUnixMillis(ts.clock.Now()),
				LastUpdatedAt: xtime.ToUnixMillis(ts.clock.Now()),
				Version:       1,
				MappingRules: []*schema.ClusterMappingRuleSet{
					&schema.ClusterMappingRuleSet{
						ForVersion: 1,
					},
				},
			},
			"foo-tiny": &schema.Database{
				Properties: &schema.DatabaseProperties{
					Name:               "foo-tiny",
					MaxRetentionInSecs: testRetentionInSecs / 10,
					NumShards:          testNumShards,
				},
				CreatedAt:           xtime.ToUnixMillis(ts.clock.Now()),
				LastUpdatedAt:       xtime.ToUnixMillis(ts.clock.Now()),
				ReadCutoverTime:     xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay)),
				WriteCutoverTime:    xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay * 2)),
				CutoverCompleteTime: xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay*2 + testTransitionDelay)),
				Version:             1,
				MappingRules: []*schema.ClusterMappingRuleSet{
					&schema.ClusterMappingRuleSet{
						ForVersion: 1,
					},
				},
			},
		},
	}, ts.latestPlacement())

}

func TestPlacement_CommitInitialClusters(t *testing.T) {
	ts := newPlacementTestSuite(t)
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
	}, []byte("bar-config")))
	require.NoError(t, ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "zed",
		Weight: uint32(testNumShards / 2),
		Type:   "m3db",
	}, []byte("zed-config")))

	createTime := xtime.ToUnixMillis(ts.clock.Now())
	ts.clock.Add(time.Second * 45)

	// Commit the changes
	err := ts.sp.CommitChanges(ts.latestVersion(), testCommitOpts)
	require.NoError(t, err)

	// Confirm the placement looks correct
	requireEqualPlacements(t, &schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": &schema.Database{
				Properties: &schema.DatabaseProperties{
					Name:               "foo",
					MaxRetentionInSecs: testRetentionInSecs,
					NumShards:          testNumShards,
				},
				CreatedAt:     createTime,
				LastUpdatedAt: createTime,
				ShardAssignments: map[string]*schema.ShardSet{
					"bar": NewShardSet(0, 1),
					"zed": NewShardSet(2, 3),
				},
				Version: 1,
				Clusters: map[string]*schema.Cluster{
					"bar": &schema.Cluster{
						Properties: &schema.ClusterProperties{
							Name:   "bar",
							Weight: uint32(testNumShards) / 2,
							Type:   "m3db",
						},
						Status:        schema.ClusterStatus_ACTIVE,
						CreatedAt:     createTime,
						LastUpdatedAt: createTime,
						Config:        []byte("bar-config"),
					},
					"zed": &schema.Cluster{
						Properties: &schema.ClusterProperties{
							Name:   "zed",
							Weight: uint32(testNumShards) / 2,
							Type:   "m3db",
						},
						Status:        schema.ClusterStatus_ACTIVE,
						CreatedAt:     createTime,
						LastUpdatedAt: createTime,
						Config:        []byte("zed-config"),
					},
				},
				MappingRules: []*schema.ClusterMappingRuleSet{
					&schema.ClusterMappingRuleSet{
						ForVersion: 1,
						ShardTransitions: []*schema.ShardTransitionRule{
							&schema.ShardTransitionRule{
								ToCluster:        "bar",
								Shards:           NewShardSet(0, 1),
								ReadCutoverTime:  xtime.ToUnixMillis(ts.clock.Now()),
								WriteCutoverTime: xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay)),
							}, &schema.ShardTransitionRule{
								ToCluster:        "zed",
								Shards:           NewShardSet(2, 3),
								ReadCutoverTime:  xtime.ToUnixMillis(ts.clock.Now()),
								WriteCutoverTime: xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay)),
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
	// Create a database with a set of clusters
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)
	createTime := xtime.ToUnixMillis(ts.clock.Now())

	// Add the database and some clusters
	require.NoError(t, ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo",
		MaxRetentionInSecs: testRetentionInSecs,
		NumShards:          testNumShards,
	}))

	ts.clock.Add(time.Second * 50)
	c1CreateTime := xtime.ToUnixMillis(ts.clock.Now())
	require.NoError(t, ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "c1",
		Weight: uint32(testNumShards / 2),
		Type:   "m3db",
	}, []byte("c1-config")))

	ts.clock.Add(time.Minute * 30)
	c2CreateTime := xtime.ToUnixMillis(ts.clock.Now())
	require.NoError(t, ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "c2",
		Weight: uint32(testNumShards / 2),
		Type:   "m3db",
	}, []byte("c2-config")))

	ts.clock.Add(time.Second * 45)

	// Commit the changes
	require.NoError(t, ts.sp.CommitChanges(ts.latestVersion(), testCommitOpts))

	// Confirm the placement looks correct
	db := &schema.Database{
		Properties: &schema.DatabaseProperties{
			Name:               "foo",
			MaxRetentionInSecs: testRetentionInSecs,
			NumShards:          testNumShards,
		},
		CreatedAt:     createTime,
		LastUpdatedAt: createTime,
		ShardAssignments: map[string]*schema.ShardSet{
			"c1": NewShardSet(0, 1),
			"c2": NewShardSet(2, 3),
		},
		Version: 1,
		Clusters: map[string]*schema.Cluster{
			"c1": &schema.Cluster{
				Properties: &schema.ClusterProperties{
					Name:   "c1",
					Weight: uint32(testNumShards) / 2,
					Type:   "m3db",
				},
				Status:        schema.ClusterStatus_ACTIVE,
				CreatedAt:     c1CreateTime,
				LastUpdatedAt: c1CreateTime,
				Config:        []byte("c1-config"),
			},
			"c2": &schema.Cluster{
				Properties: &schema.ClusterProperties{
					Name:   "c2",
					Weight: uint32(testNumShards) / 2,
					Type:   "m3db",
				},
				Status:        schema.ClusterStatus_ACTIVE,
				CreatedAt:     c2CreateTime,
				LastUpdatedAt: c2CreateTime,
				Config:        []byte("c2-config"),
			},
		},
		MappingRules: []*schema.ClusterMappingRuleSet{
			&schema.ClusterMappingRuleSet{
				ForVersion: 1,
				ShardTransitions: []*schema.ShardTransitionRule{
					&schema.ShardTransitionRule{
						ToCluster:        "c1",
						Shards:           NewShardSet(0, 1),
						ReadCutoverTime:  xtime.ToUnixMillis(ts.clock.Now()),
						WriteCutoverTime: xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay)),
					},
					&schema.ShardTransitionRule{
						ToCluster:        "c2",
						Shards:           NewShardSet(2, 3),
						ReadCutoverTime:  xtime.ToUnixMillis(ts.clock.Now()),
						WriteCutoverTime: xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay)),
					},
				},
			},
		},
	}

	expectedPlacement := &schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": db,
		},
	}
	requireEqualPlacements(t, expectedPlacement, ts.latestPlacement())

	// Decommission an existing cluster
	ts.clock.Add(time.Hour * 72)
	require.NoError(t, ts.sp.DecommissionCluster("foo", "c1"))
	require.NoError(t, ts.sp.CommitChanges(ts.latestVersion(), testCommitOpts))

	// should redistribute traffic to the remaining cluster
	db.Version = 2
	db.Clusters["c1"].Status = schema.ClusterStatus_DECOMMISSIONING
	db.ShardAssignments["c1"].Bits = nil
	db.ShardAssignments["c2"] = NewShardSet(0, 1, 2, 3)
	db.MappingRules = append(db.MappingRules, &schema.ClusterMappingRuleSet{
		ForVersion: 2,
		ShardTransitions: []*schema.ShardTransitionRule{
			&schema.ShardTransitionRule{
				ToCluster:           "c2",
				FromCluster:         "c1",
				Shards:              NewShardSet(0, 1),
				ReadCutoverTime:     xtime.ToUnixMillis(ts.clock.Now()),
				WriteCutoverTime:    xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay)),
				CutoverCompleteTime: xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay + testTransitionDelay)),
			},
		},
	})

	requireEqualPlacements(t, expectedPlacement, ts.latestPlacement())
}

func TestPlacement_CommitJoinClusters(t *testing.T) {
	// Create an empty database
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)
	createTime := xtime.ToUnixMillis(ts.clock.Now())

	require.NoError(t, ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo",
		MaxRetentionInSecs: testRetentionInSecs,
		NumShards:          testNumShards,
	}))
	require.NoError(t, ts.sp.CommitChanges(ts.latestVersion(), testCommitOpts))

	// Confirm the empty placement
	db := &schema.Database{
		Properties: &schema.DatabaseProperties{
			Name:               "foo",
			MaxRetentionInSecs: testRetentionInSecs,
			NumShards:          testNumShards,
		},
		CreatedAt:     createTime,
		LastUpdatedAt: createTime,
		Version:       1,
		MappingRules: []*schema.ClusterMappingRuleSet{
			&schema.ClusterMappingRuleSet{
				ForVersion: 1,
			},
		},
	}

	expectedPlacement := &schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": db,
		},
	}

	requireEqualPlacements(t, expectedPlacement, ts.latestPlacement())

	// Join the first cluster
	ts.clock.Add(time.Minute * 30)
	c1CreateTime := xtime.ToUnixMillis(ts.clock.Now())

	require.NoError(t, ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "c1",
		Weight: uint32(testNumShards / 2),
		Type:   "m3db",
	}, []byte("c1-config")))
	require.NoError(t, ts.sp.CommitChanges(ts.latestVersion(), testCommitOpts))

	// Confirm the cluster and shard distribution line up
	db.Version = 2
	db.Clusters = map[string]*schema.Cluster{
		"c1": &schema.Cluster{
			Properties: &schema.ClusterProperties{
				Name:   "c1",
				Type:   "m3db",
				Weight: uint32(testNumShards / 2),
			},
			Status:        schema.ClusterStatus_ACTIVE,
			CreatedAt:     c1CreateTime,
			LastUpdatedAt: c1CreateTime,
			Config:        []byte("c1-config"),
		},
	}

	db.ShardAssignments = map[string]*schema.ShardSet{
		"c1": NewShardSet(0, 1, 2, 3),
	}

	db.MappingRules = append(db.MappingRules, &schema.ClusterMappingRuleSet{
		ForVersion: 2,
		ShardTransitions: []*schema.ShardTransitionRule{
			&schema.ShardTransitionRule{
				ToCluster:        "c1",
				Shards:           NewShardSet(0, 1, 2, 3),
				ReadCutoverTime:  xtime.ToUnixMillis(ts.clock.Now()),
				WriteCutoverTime: xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay)),
			},
		},
	})

	// Join another cluster, shards should be redistributed
	ts.clock.Add(time.Minute * 30)
	c2CreateTime := xtime.ToUnixMillis(ts.clock.Now())

	require.NoError(t, ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "c2",
		Weight: uint32(testNumShards / 2),
		Type:   "m3db",
	}, []byte("c2-config")))
	require.NoError(t, ts.sp.CommitChanges(ts.latestVersion(), testCommitOpts))

	// Confirm the shards were properly redistributed
	db.Version = 3
	db.Clusters["c2"] = &schema.Cluster{
		Properties: &schema.ClusterProperties{
			Name:   "c2",
			Type:   "m3db",
			Weight: uint32(testNumShards / 2),
		},
		Status:        schema.ClusterStatus_ACTIVE,
		CreatedAt:     c2CreateTime,
		LastUpdatedAt: c2CreateTime,
		Config:        []byte("c2-config"),
	}

	db.ShardAssignments["c1"] = NewShardSet(2, 3)
	db.ShardAssignments["c2"] = NewShardSet(0, 1)

	db.MappingRules = append(db.MappingRules, &schema.ClusterMappingRuleSet{
		ForVersion: 3,
		ShardTransitions: []*schema.ShardTransitionRule{
			&schema.ShardTransitionRule{
				ToCluster:           "c2",
				FromCluster:         "c1",
				Shards:              NewShardSet(0, 1),
				ReadCutoverTime:     xtime.ToUnixMillis(ts.clock.Now()),
				WriteCutoverTime:    xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay)),
				CutoverCompleteTime: xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay + testTransitionDelay)),
			},
		},
	})
	requireEqualPlacements(t, expectedPlacement, ts.latestPlacement())

	// Join a third cluster, this leaves the distribution unbalanced, so the oldest
	// cluster should have one more shard than the others
	ts.clock.Add(time.Minute * 30)
	c3CreateTime := xtime.ToUnixMillis(ts.clock.Now())

	require.NoError(t, ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "c3",
		Weight: uint32(testNumShards / 2),
		Type:   "m3db",
	}, []byte("c3-config")))
	require.NoError(t, ts.sp.CommitChanges(ts.latestVersion(), testCommitOpts))

	// Confirm the shards were properly redistributed
	db.Version = 4
	db.Clusters["c3"] = &schema.Cluster{
		Properties: &schema.ClusterProperties{
			Name:   "c3",
			Type:   "m3db",
			Weight: uint32(testNumShards / 2),
		},
		Status:        schema.ClusterStatus_ACTIVE,
		CreatedAt:     c3CreateTime,
		LastUpdatedAt: c3CreateTime,
		Config:        []byte("c3-config"),
	}

	db.ShardAssignments["c1"] = NewShardSet(2, 3)
	db.ShardAssignments["c2"] = NewShardSet(1)
	db.ShardAssignments["c3"] = NewShardSet(0)

	db.MappingRules = append(db.MappingRules, &schema.ClusterMappingRuleSet{
		ForVersion: 4,
		ShardTransitions: []*schema.ShardTransitionRule{
			&schema.ShardTransitionRule{
				ToCluster:           "c3",
				FromCluster:         "c2",
				Shards:              NewShardSet(0),
				ReadCutoverTime:     xtime.ToUnixMillis(ts.clock.Now()),
				WriteCutoverTime:    xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay)),
				CutoverCompleteTime: xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay + testTransitionDelay)),
			},
		},
	})

	requireEqualPlacements(t, expectedPlacement, ts.latestPlacement())
}

func TestPlacement_CommitComplexTopologyChanges(t *testing.T) {
	// Create a database with a set of clusters
	ts := newPlacementTestSuite(t)
	ts.clock.Add(time.Second * 34)
	createTime := xtime.ToUnixMillis(ts.clock.Now())

	// Add the database and some clusters
	require.NoError(t, ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "foo",
		MaxRetentionInSecs: testRetentionInSecs,
		NumShards:          testNumShards,
	}))

	ts.clock.Add(time.Second * 50)
	c1CreateTime := xtime.ToUnixMillis(ts.clock.Now())
	require.NoError(t, ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "c1",
		Weight: uint32(testNumShards / 2),
		Type:   "m3db",
	}, []byte("c1-config")))

	ts.clock.Add(time.Minute * 30)
	c2CreateTime := xtime.ToUnixMillis(ts.clock.Now())
	require.NoError(t, ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "c2",
		Weight: uint32(testNumShards / 2),
		Type:   "m3db",
	}, []byte("c2-config")))

	ts.clock.Add(time.Second * 45)

	// Commit the changes
	require.NoError(t, ts.sp.CommitChanges(ts.latestVersion(), testCommitOpts))

	// Confirm the placement looks correct
	db := &schema.Database{
		Properties: &schema.DatabaseProperties{
			Name:               "foo",
			MaxRetentionInSecs: testRetentionInSecs,
			NumShards:          testNumShards,
		},
		CreatedAt:     createTime,
		LastUpdatedAt: createTime,
		ShardAssignments: map[string]*schema.ShardSet{
			"c1": NewShardSet(0, 1),
			"c2": NewShardSet(2, 3),
		},
		Version: 1,
		Clusters: map[string]*schema.Cluster{
			"c1": &schema.Cluster{
				Properties: &schema.ClusterProperties{
					Name:   "c1",
					Weight: uint32(testNumShards) / 2,
					Type:   "m3db",
				},
				Status:        schema.ClusterStatus_ACTIVE,
				CreatedAt:     c1CreateTime,
				LastUpdatedAt: c1CreateTime,
				Config:        []byte("c1-config"),
			},
			"c2": &schema.Cluster{
				Properties: &schema.ClusterProperties{
					Name:   "c2",
					Weight: uint32(testNumShards) / 2,
					Type:   "m3db",
				},
				Status:        schema.ClusterStatus_ACTIVE,
				CreatedAt:     c2CreateTime,
				LastUpdatedAt: c2CreateTime,
				Config:        []byte("c2-config"),
			},
		},
		MappingRules: []*schema.ClusterMappingRuleSet{
			&schema.ClusterMappingRuleSet{
				ForVersion: 1,
				ShardTransitions: []*schema.ShardTransitionRule{
					&schema.ShardTransitionRule{
						ToCluster:        "c1",
						Shards:           NewShardSet(0, 1),
						ReadCutoverTime:  xtime.ToUnixMillis(ts.clock.Now()),
						WriteCutoverTime: xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay)),
					},
					&schema.ShardTransitionRule{
						ToCluster:        "c2",
						Shards:           NewShardSet(2, 3),
						ReadCutoverTime:  xtime.ToUnixMillis(ts.clock.Now()),
						WriteCutoverTime: xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay)),
					},
				},
			},
		},
	}

	expectedPlacement := &schema.Placement{
		Databases: map[string]*schema.Database{
			"foo": db,
		},
	}
	requireEqualPlacements(t, expectedPlacement, ts.latestPlacement())

	// Join another cluster
	ts.clock.Add(time.Minute * 30)
	c3CreateTime := xtime.ToUnixMillis(ts.clock.Now())

	require.NoError(t, ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "c3",
		Weight: uint32(testNumShards / 2),
		Type:   "m3db",
	}, []byte("c3-config")))
	require.NoError(t, ts.sp.CommitChanges(ts.latestVersion(), testCommitOpts))

	// Should redistribute the load, leaving more shards on the oldest cluster
	db.Version = 2
	db.Clusters["c3"] = &schema.Cluster{
		Properties: &schema.ClusterProperties{
			Name:   "c3",
			Weight: uint32(testNumShards / 2),
			Type:   "m3db",
		},
		Status:        schema.ClusterStatus_ACTIVE,
		CreatedAt:     c3CreateTime,
		LastUpdatedAt: c3CreateTime,
		Config:        []byte("c3-config"),
	}
	db.ShardAssignments["c1"] = NewShardSet(0, 1)
	db.ShardAssignments["c2"] = NewShardSet(3)
	db.ShardAssignments["c3"] = NewShardSet(2)
	db.MappingRules = append(db.MappingRules, &schema.ClusterMappingRuleSet{
		ForVersion: 2,
		ShardTransitions: []*schema.ShardTransitionRule{
			&schema.ShardTransitionRule{
				ToCluster:           "c3",
				FromCluster:         "c2",
				Shards:              NewShardSet(2),
				ReadCutoverTime:     xtime.ToUnixMillis(ts.clock.Now()),
				WriteCutoverTime:    xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay)),
				CutoverCompleteTime: xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay + testTransitionDelay)),
			},
		},
	})

	requireEqualPlacements(t, expectedPlacement, ts.latestPlacement())

	// Decommission an existing clusters and add a third
	ts.clock.Add(time.Hour * 72)
	c4CreateTime := xtime.ToUnixMillis(ts.clock.Now())
	require.NoError(t, ts.sp.JoinCluster("foo", schema.ClusterProperties{
		Name:   "c4",
		Weight: uint32(testNumShards / 2),
		Type:   "m3db",
	}, []byte("c4-config")))
	require.NoError(t, ts.sp.DecommissionCluster("foo", "c1"))
	require.NoError(t, ts.sp.CommitChanges(ts.latestVersion(), testCommitOpts))

	// should redistribute traffic between the newly added and remaining clusters
	db.Version = 3
	db.Clusters["c4"] = &schema.Cluster{
		Properties: &schema.ClusterProperties{
			Name:   "c4",
			Weight: uint32(testNumShards / 2),
			Type:   "m3db",
		},
		Status:        schema.ClusterStatus_ACTIVE,
		CreatedAt:     c4CreateTime,
		LastUpdatedAt: c4CreateTime,
		Config:        []byte("c4-config"),
	}
	db.Clusters["c1"].Status = schema.ClusterStatus_DECOMMISSIONING
	db.ShardAssignments["c1"] = NewShardSet()
	db.ShardAssignments["c2"] = NewShardSet(0, 3)
	db.ShardAssignments["c4"] = NewShardSet(1)

	db.MappingRules = append(db.MappingRules, &schema.ClusterMappingRuleSet{
		ForVersion: 3,
		ShardTransitions: []*schema.ShardTransitionRule{
			&schema.ShardTransitionRule{
				ToCluster:           "c2",
				FromCluster:         "c1",
				Shards:              NewShardSet(0),
				ReadCutoverTime:     xtime.ToUnixMillis(ts.clock.Now()),
				WriteCutoverTime:    xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay)),
				CutoverCompleteTime: xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay + testTransitionDelay)),
			},

			&schema.ShardTransitionRule{
				ToCluster:           "c4",
				FromCluster:         "c1",
				Shards:              NewShardSet(1),
				ReadCutoverTime:     xtime.ToUnixMillis(ts.clock.Now()),
				WriteCutoverTime:    xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay)),
				CutoverCompleteTime: xtime.ToUnixMillis(ts.clock.Now().Add(testRolloutDelay + testTransitionDelay)),
			},
		},
	})

	requireEqualPlacements(t, expectedPlacement, ts.latestPlacement())
}

type placementTestSuite struct {
	kv    kv.Store
	sp    StoragePlacement
	t     require.TestingT
	clock *clock.Mock
}

func newPlacementTestSuite(t require.TestingT) *placementTestSuite {
	return newPlacementTestSuiteWithLogger(t, xlog.SimpleLogger)
}

func newPlacementTestSuiteWithLogger(t require.TestingT, logger xlog.Logger) *placementTestSuite {
	kv := kv.NewFakeStore()
	clock := clock.NewMock()

	sp, err := NewStoragePlacement(kv, "p", NewStoragePlacementOptions().
		Clock(clock).
		Logger(xlog.NullLogger))
	require.NoError(t, err)

	return &placementTestSuite{
		kv:    kv,
		sp:    sp,
		clock: clock,
		t:     t,
	}
}

func (ts *placementTestSuite) latestPlacement() *schema.Placement {
	_, p, _, err := ts.sp.GetPendingChanges()
	require.NoError(ts.t, err)
	return p
}

func (ts *placementTestSuite) latestVersion() int {
	v, _, _, err := ts.sp.GetPendingChanges()
	require.NoError(ts.t, err)
	return v
}

func (ts *placementTestSuite) latestChanges() *schema.PlacementChanges {
	_, _, changes, err := ts.sp.GetPendingChanges()
	require.NoError(ts.t, err)
	return changes
}

func (ts *placementTestSuite) forcePlacement(newPlacement *schema.Placement) {
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

func (ts *placementTestSuite) forceChanges(newChanges *schema.PlacementChanges) {
	require.NoError(ts.t, ts.sp.(storagePlacement).mgr.Change(func(_, protoChanges proto.Message) error {
		c := protoChanges.(*schema.PlacementChanges)
		*c = *newChanges
		return nil
	}))
}

func (ts *placementTestSuite) commitLatest() {
	require.NoError(ts.t, ts.sp.CommitChanges(ts.latestVersion(), testCommitOpts), "error committing")
}

func requireEqualPlacements(t *testing.T, p1, p2 *schema.Placement) {
	if p1.Databases == nil {
		require.Nil(t, p2.Databases, "expected nil Databases")
	}

	for name, db1 := range p1.Databases {
		db2 := p2.Databases[name]
		require.NotNil(t, db2, "no Database named %s", name)
		requireEqualDatabases(t, name, db1, db2)
	}

	require.Equal(t, len(p1.Databases), len(p2.Databases), "Databases")
}

func requireEqualDatabases(t *testing.T, dbname string, db1, db2 *schema.Database) {
	require.Equal(t, db1.Properties.Name, db2.Properties.Name, "Name for db %s", dbname)
	require.Equal(t, db1.Properties.MaxRetentionInSecs, db2.Properties.MaxRetentionInSecs,
		"MaxRetentionInSecs[%s]", dbname)
	require.Equal(t, db1.Properties.NumShards, db2.Properties.NumShards, "NumShards[%s]", dbname)
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
		requireEqualClusters(t, dbname, cname, c1, c2)
	}
	require.Equal(t, len(db1.Clusters), len(db2.Clusters), "Clusters[%s]", dbname)

	for cname, a1 := range db1.ShardAssignments {
		a2 := db2.ShardAssignments[cname]
		require.NotNil(t, a2, "no ShardAssigment for %s in %s", cname, dbname)
		require.Equal(t, a1.Bits, a2.Bits, "Bits[%s:%s]", dbname, cname)
	}
	require.Equal(t, len(db1.ShardAssignments), len(db2.ShardAssignments))

	require.Equal(t, len(db1.MappingRules), len(db2.MappingRules), "MappingRules[%s]", dbname)
	for i := range db1.MappingRules {
		r1, r2 := db1.MappingRules[i], db2.MappingRules[i]
		require.Equal(t, r1.ForVersion, r2.ForVersion, "ForVersion[%s:%d]", dbname, i)
		require.Equal(t, len(r1.ShardTransitions), len(r2.ShardTransitions), "Cutovers[%s:%d]", dbname, i)
		for n := range r1.ShardTransitions {
			t1, t2 := r1.ShardTransitions[n], r2.ShardTransitions[n]
			require.Equal(t, t1.ToCluster, t2.ToCluster,
				"ShardTransitions[%s:%d:%d].ToCluster", dbname, i, n)
			require.Equal(t, t1.FromCluster, t2.FromCluster,
				"ShardTransitions[%s:%d:%d].FromCluster", dbname, i, n)
			require.Equal(t, t1.ReadCutoverTime, t2.ReadCutoverTime,
				"ShardTransitions[%s:%d:%d].ReadCutoverTime", dbname, i, n)
			require.Equal(t, t1.WriteCutoverTime, t2.WriteCutoverTime,
				"ShardTransitions[%s:%d:%d].WriteCutoverTime", dbname, i, n)
			require.Equal(t, t1.CutoverCompleteTime, t2.CutoverCompleteTime,
				"ShardTransitions[%s:%d:%d].CutoverCompleteTime", dbname, i, n)
			require.Equal(t, t1.Shards.Bits, t2.Shards.Bits, "ShardTransitions[%s:%d:%d].Bits", dbname, i, n)
		}
	}
}

func requireEqualClusters(t *testing.T, dbname, cname string, c1, c2 *schema.Cluster) {
	require.Equal(t, c1.Properties.Name, c2.Properties.Name, "Name[%s:%s]", dbname, cname)
	require.Equal(t, c1.Properties.Weight, c2.Properties.Weight, "Weight[%s:%s]", dbname, cname)
	require.Equal(t, c1.Properties.Type, c2.Properties.Type, "Type[%s:%s]", dbname, cname)
	require.Equal(t, c1.Status, c2.Status, "Status[%s:%s]", dbname, cname)
	require.Equal(t, c1.CreatedAt, c2.CreatedAt, "CreatedAt[%s:%s]", dbname, cname)
	require.Equal(t, string(c1.Config), string(c2.Config), "Config[%s:%s]", dbname, cname)
}

func requireEqualChanges(t *testing.T, c1, c2 *schema.PlacementChanges) {
	for dbname, add1 := range c1.DatabaseAdds {
		add2 := c2.DatabaseAdds[dbname]
		require.NotNil(t, add2, "No DatabaseAdd for %s", dbname)
		requireEqualDatabases(t, dbname, add1.Database, add2.Database)
	}
	require.Equal(t, len(c1.DatabaseAdds), len(c2.DatabaseAdds))

	for dbname, dbchange1 := range c1.DatabaseChanges {
		dbchange2 := c2.DatabaseChanges[dbname]
		require.NotNil(t, dbchange2, "No DatabaseChange for %s", dbname)
		requireEqualDatabaseChanges(t, dbname, dbchange1, dbchange2)
	}
	require.Equal(t, len(c1.DatabaseChanges), len(c2.DatabaseChanges))
}

func requireEqualDatabaseChanges(t *testing.T, dbname string, c1, c2 *schema.DatabaseChanges) {
	for cname, j1 := range c1.Joins {
		j2 := c2.Joins[cname]
		require.NotNil(t, j2, "no join for %s in %s", cname, dbname)
		requireEqualClusters(t, dbname, cname, j1.Cluster, j2.Cluster)
	}
	require.Equal(t, len(c1.Joins), len(c2.Joins))

	for cname, d1 := range c1.Decomms {
		d2 := c2.Decomms[cname]
		require.NotNil(t, d2, "no decomm for %s in %s", cname, dbname)
		require.Equal(t, d1.ClusterName, d2.ClusterName, "ClusterName[%s:%s]", dbname, cname)
	}
	require.Equal(t, len(c1.Decomms), len(c2.Decomms))

	for cname, u1 := range c1.ClusterConfigUpdates {
		u2 := c2.ClusterConfigUpdates[cname]
		require.NotNil(t, u2, "no config update for %s in %s", cname, dbname)
		require.Equal(t, string(u1), string(u2), "ClusterConfig[%s:%s]", dbname, cname)
	}
	require.Equal(t, len(c1.ClusterConfigUpdates), len(c2.ClusterConfigUpdates))
}
