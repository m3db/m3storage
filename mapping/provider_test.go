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
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3storage/generated/proto/configtest"
	"github.com/m3db/m3storage/generated/proto/schema"
	"github.com/m3db/m3storage/placement"
	"github.com/m3db/m3x/log"

	"github.com/facebookgo/clock"
	"github.com/stretchr/testify/require"
)

const (
	shortRetention  = time.Hour * 12
	mediumRetention = time.Hour * 24 * 7
	longRetention   = time.Hour * 24 * 30

	testTransitionDelay = time.Minute * 5
	testRolloutDelay    = time.Minute * 30
	readCutoverDelay    = testRolloutDelay * 2
	writeCutoverDelay   = readCutoverDelay + testRolloutDelay
	cutoffDelay         = writeCutoverDelay + testTransitionDelay
)

var (
	commitOptions = placement.NewCommitOptions().
		RolloutDelay(testRolloutDelay).
		TransitionDelay(testTransitionDelay)
)

func TestProviderFetchRules(t *testing.T) {
	ts := newProviderTestSuite(t)
	now := ts.clock.Now()

	type fetch struct {
		shard     uint32
		retention time.Duration
		results   []expectedRule
	}

	type changes struct {
		databases []schema.DatabaseProperties
		joins     map[string][]schema.ClusterProperties
		decomms   map[string][]string
	}

	type test struct {
		scenario string
		changes  changes
		queries  []fetch
	}

	config := &configtest.TestConfig{
		Hosts: []string{"h1", "h2"},
	}

	tests := []test{
		test{
			scenario: "no databases",
			queries: []fetch{
				{12, shortRetention, nil},
				{24, longRetention, nil},
			},
		},

		// add a database anda a cluster for medium retention
		test{
			scenario: "add database with one cluster",
			changes: changes{
				databases: []schema.DatabaseProperties{
					schema.DatabaseProperties{
						Name:               "wow",
						NumShards:          int32(testShards),
						MaxRetentionInSecs: int32(mediumRetention / time.Second),
					},
				},
				joins: map[string][]schema.ClusterProperties{
					"wow": []schema.ClusterProperties{
						schema.ClusterProperties{
							Name:   "wow1",
							Weight: uint32(testShards),
							Type:   "m3db",
						},
					},
				},
			},

			// all queries should hit the medium retention cluster
			queries: []fetch{
				fetch{0, shortRetention, []expectedRule{
					expectedRule{
						database: "wow",
						cluster:  "wow1",
						read:     now.Add(readCutoverDelay),
						write:    now.Add(writeCutoverDelay)},
				}},
				fetch{0, longRetention, []expectedRule{
					expectedRule{
						database: "wow", cluster: "wow1",
						read:  now.Add(readCutoverDelay),
						write: now.Add(writeCutoverDelay)},
				}},
			},
		},

		// join a new cluster to the existing database - the first sets of shards should move
		test{
			scenario: "join clusters to existing database",
			changes: changes{
				joins: map[string][]schema.ClusterProperties{
					"wow": []schema.ClusterProperties{
						{Name: "wow2", Weight: uint32(testShards), Type: "m3db"},
						{Name: "wow3", Weight: uint32(testShards), Type: "m3db"},
					},
				},
			},

			// queries for the first sets of shards should span clusters, tail shards
			// should remain where they were
			queries: []fetch{
				fetch{0, shortRetention, []expectedRule{
					expectedRule{
						database: "wow",
						cluster:  "wow2",
						read:     now.Add(readCutoverDelay + time.Hour),
						write:    now.Add(writeCutoverDelay + time.Hour)},

					expectedRule{
						database: "wow",
						cluster:  "wow1",
						read:     now.Add(readCutoverDelay),
						write:    now.Add(writeCutoverDelay),
						cutoff:   now.Add(cutoffDelay + time.Hour)},
				}},
				fetch{4095, longRetention, []expectedRule{
					expectedRule{
						database: "wow",
						cluster:  "wow1",
						read:     now.Add(readCutoverDelay),
						write:    now.Add(writeCutoverDelay)},
				}},
			},
		},

		// join two new clusters to the existing database and rebalance
		test{
			scenario: "join more clusters to existing database",
			changes: changes{
				joins: map[string][]schema.ClusterProperties{
					"wow": []schema.ClusterProperties{
						{Name: "wow4", Weight: uint32(testShards), Type: "m3db"},
						{Name: "wow5", Weight: uint32(testShards), Type: "m3db"},
					},
				},
			},

			queries: []fetch{
				fetch{0, shortRetention, []expectedRule{
					expectedRule{
						database: "wow",
						cluster:  "wow4",
						read:     now.Add(readCutoverDelay + time.Hour*2),
						write:    now.Add(writeCutoverDelay + time.Hour*2)},
					expectedRule{
						database: "wow",
						cluster:  "wow2",
						read:     now.Add(readCutoverDelay + time.Hour),
						write:    now.Add(writeCutoverDelay + time.Hour),
						cutoff:   now.Add(cutoffDelay + time.Hour*2)},
					expectedRule{
						database: "wow",
						cluster:  "wow1",
						read:     now.Add(readCutoverDelay),
						write:    now.Add(writeCutoverDelay),
						cutoff:   now.Add(cutoffDelay + time.Hour)},
				}},
				fetch{4095, longRetention, []expectedRule{
					expectedRule{
						database: "wow",
						cluster:  "wow1",
						read:     now.Add(readCutoverDelay),
						write:    now.Add(writeCutoverDelay)},
				}},
			},
		},
	}

	for _, test := range tests {
		changed := false
		// apply changes - first database adds, then cluster joins, then cluster decomms
		for _, db := range test.changes.databases {
			changed = true
			require.NoError(t, ts.sp.AddDatabase(db),
				"cannot add database %s", db.Name)
		}

		for dbname, joins := range test.changes.joins {
			changed = true
			for _, join := range joins {
				require.NoError(t, ts.sp.JoinCluster(dbname, join, config),
					"cannot join cluster %s:%s", dbname, join.Name)
			}
		}

		for dbname, decomms := range test.changes.decomms {
			changed = true
			for _, cname := range decomms {
				require.NoError(t, ts.sp.DecommissionCluster(dbname, cname),
					"cannot decomm cluster %s:%s", dbname, cname)
			}
		}

		if changed {
			ts.commitLatest()
			pl := ts.latestPlacement()
			require.NoError(t, ts.p.update(pl))
		}

		// advance time
		ts.clock.Add(time.Hour)

		// run queries and compare results
		for n, q := range test.queries {
			iter, err := ts.p.FetchRules(q.shard, ts.clock.Now().Add(-q.retention), ts.clock.Now())
			require.NoError(t, err)

			results := collectMappings(iter)
			require.NoError(t, iter.Close())

			require.Equal(t, len(q.results), len(results), "bad results for %s fetch %d", test.scenario, n)
			for i := range results {
				r1, r2 := q.results[i], results[i]
				requireRules(t, r1, r2, fmt.Sprintf("bad result %d for %s fetch %d", i, test.scenario, n))
			}
		}
	}

	ts.Close()
}

func TestProviderPropagatesClusterUpdates(t *testing.T) {
}

func BenchmarkProvider1Cluster(b *testing.B) {
	benchmarkNClusterSplits(b, 1, 1)
}

func BenchmarkProvider2ClusterSplits(b *testing.B) {
	benchmarkNClusterSplits(b, 2, 2)
}

func BenchmarkProvider8ClusterSplits(b *testing.B) {
	benchmarkNClusterSplits(b, 8, 8)
}

func BenchmarkProvider32ClusterSplits(b *testing.B) {
	benchmarkNClusterSplits(b, 32, 32)
}

func BenchmarkProvider64ClusterSplits(b *testing.B) {
	benchmarkNClusterSplits(b, 64, 60)
}

func BenchmarkProvider128ClusterSplits(b *testing.B) {
	benchmarkNClusterSplits(b, 128, 103)
}

func BenchmarkProvider256ClusterSplits(b *testing.B) {
	benchmarkNClusterSplits(b, 256, 135)
}

func benchmarkNClusterSplits(b *testing.B, numSplits, expectedLoMappings int) {
	ts := newProviderTestSuite(b)

	// Create a database, then join clusters to it repeatedly, splitting each time
	require.NoError(b, ts.sp.AddDatabase(schema.DatabaseProperties{
		Name:               "db1",
		NumShards:          testShards,
		MaxRetentionInSecs: int32(time.Hour * 24 * 30 / time.Second),
	}))
	ts.commitLatest()

	for i := 0; i < numSplits; i++ {
		ts.clock.Add(time.Hour)
		require.NoError(b, ts.sp.JoinCluster("db1", schema.ClusterProperties{
			Name:   fmt.Sprintf("c%d", i),
			Weight: uint32(testShards),
			Type:   "m3db",
		}, &configtest.TestConfig{
			Hosts: []string{fmt.Sprintf("h%d", i)},
		}))

		ts.commitLatest()
	}

	// update the mappings
	ts.p.update(ts.latestPlacement())

	// confirm we have the right number of mappings with the right values...
	start, end := ts.clock.Now().Add(-time.Hour*24), ts.clock.Now()
	iter, err := ts.p.FetchRules(0, start, end)
	require.NoError(b, err)

	loMappings := collectMappings(iter)
	require.NoError(b, iter.Close())

	require.Equal(b, expectedLoMappings, len(loMappings))

	// ...and run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, _ := ts.p.FetchRules(0, start, end)
		for iter.Next() {
		}
	}
}

func collectMappings(iter RuleIter) []Rule {
	var results []Rule
	for iter.Next() {
		results = append(results, iter.Current())
	}

	return results
}

type expectedRule struct {
	database, cluster   string
	read, write, cutoff time.Time
}

func (m expectedRule) ReadCutoverTime() time.Time  { return m.read }
func (m expectedRule) WriteCutoverTime() time.Time { return m.write }
func (m expectedRule) CutoffTime() time.Time       { return m.cutoff }
func (m expectedRule) Cluster() string             { return m.cluster }
func (m expectedRule) Database() string            { return m.database }

func requireRules(t *testing.T, expected, actual Rule, name string) {
	require.Equal(t, expected.ReadCutoverTime().String(), actual.ReadCutoverTime().String(),
		"%s ReadCutoverTime", name)
	require.Equal(t, expected.WriteCutoverTime().String(), actual.WriteCutoverTime().String(),
		"%s WriteCutoverTime", name)
	require.Equal(t, expected.CutoffTime().String(), actual.CutoffTime().String(), "%s CutoffTime", name)
	require.Equal(t, expected.Cluster(), actual.Cluster(), "%s Cluster", name)
	require.Equal(t, expected.Database(), actual.Database(), "%s Database", name)
}

const (
	testShards = 4096
)

type providerTestSuite struct {
	t     require.TestingT
	clock *clock.Mock
	kv    kv.Store
	sp    placement.StoragePlacement
	p     *provider
}

func newProviderTestSuite(t require.TestingT) *providerTestSuite {
	clock := clock.NewMock()
	kv := kv.NewFakeStore()

	sp, err := placement.NewStoragePlacement(kv, "cfg",
		placement.NewStoragePlacementOptions().Clock(clock))
	require.NoError(t, err)

	prov, err := NewProvider("cfg", kv, &ProviderOptions{
		Logger: xlog.SimpleLogger,
		Clock:  clock,
	})
	require.NoError(t, err)

	return &providerTestSuite{
		t:     t,
		clock: clock,
		kv:    kv,
		sp:    sp,
		p:     prov.(*provider),
	}
}

func (ts *providerTestSuite) Close() {
	ts.p.Close()
}

func (ts *providerTestSuite) commitLatest() {
	v, _, _, err := ts.sp.GetPendingChanges()
	require.NoError(ts.t, err)
	require.NoError(ts.t, ts.sp.CommitChanges(v, commitOptions))
}

func (ts *providerTestSuite) latestPlacement() *schema.Placement {
	_, pl, _, err := ts.sp.GetPendingChanges()
	require.NoError(ts.t, err)
	return pl

}
