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
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3storage"
	"github.com/m3db/m3storage/generated/proto/schema"
	"github.com/m3db/m3x/log"

	"github.com/stretchr/testify/require"
)

func TestQueryMappings(t *testing.T) {
	var (
		shortRetention  = time.Hour * 12
		mediumRetention = time.Hour * 24 * 7
		longRetention   = time.Hour * 24 * 30

		readCutoverDelay  = testRolloutDelay * 2
		writeCutoverDelay = readCutoverDelay + testRolloutDelay
		cutoffDelay       = writeCutoverDelay + testTransitionDelay

		ts     = newPlacementTestSuite(t)
		p, err = NewClusterMappingProvider(NewClusterMappingProviderOptions().
			Clock(ts.clock).
			Logger(xlog.SimpleLogger))

		now    = ts.clock.Now()
		notSet = time.Time{}
	)

	require.NoError(t, err)
	prov := p.(*clusterMappingProvider)

	type query struct {
		shard     uint32
		retention time.Duration
		results   []queryResult
	}

	type changes struct {
		databases []schema.DatabaseProperties
		joins     map[string][]schema.ClusterProperties
		decomms   map[string][]string
	}

	type test struct {
		scenario string
		changes  changes
		queries  []query
	}

	tests := []test{
		test{
			scenario: "no databases",
			queries: []query{
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
			queries: []query{
				query{0, shortRetention, []queryResult{
					queryResult{"wow1", now.Add(readCutoverDelay), now.Add(writeCutoverDelay), notSet},
				}},
				query{0, longRetention, []queryResult{
					queryResult{"wow1", now.Add(readCutoverDelay), now.Add(writeCutoverDelay), notSet},
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
			queries: []query{
				query{0, shortRetention, []queryResult{
					queryResult{
						cluster: "wow2",
						read:    now.Add(readCutoverDelay + time.Hour),
						write:   now.Add(writeCutoverDelay + time.Hour),
						cutoff:  notSet},

					queryResult{
						cluster: "wow1",
						read:    now.Add(readCutoverDelay),
						write:   now.Add(writeCutoverDelay),
						cutoff:  now.Add(cutoffDelay + time.Hour)},
				}},
				query{4095, longRetention, []queryResult{
					queryResult{"wow1", now.Add(readCutoverDelay), now.Add(writeCutoverDelay), notSet},
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

			queries: []query{
				query{0, shortRetention, []queryResult{
					queryResult{
						cluster: "wow4",
						read:    now.Add(readCutoverDelay + time.Hour*2),
						write:   now.Add(writeCutoverDelay + time.Hour*2),
						cutoff:  notSet},
					queryResult{
						cluster: "wow2",
						read:    now.Add(readCutoverDelay + time.Hour),
						write:   now.Add(writeCutoverDelay + time.Hour),
						cutoff:  now.Add(cutoffDelay + time.Hour*2)},
					queryResult{
						cluster: "wow1",
						read:    now.Add(readCutoverDelay),
						write:   now.Add(writeCutoverDelay),
						cutoff:  now.Add(cutoffDelay + time.Hour)},
				}},
				query{4095, longRetention, []queryResult{
					queryResult{
						cluster: "wow1",
						read:    now.Add(readCutoverDelay),
						write:   now.Add(writeCutoverDelay),
						cutoff:  notSet,
					},
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
				require.NoError(t, ts.sp.JoinCluster(dbname, join),
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
			prov.update(ts.latestPlacement())
		}

		// advance time
		ts.clock.Add(time.Hour)

		// run queries and compare results
		for n, q := range test.queries {
			iter := prov.QueryMappings(q.shard, ts.clock.Now().Add(-q.retention), ts.clock.Now())
			results := collectMappings(iter)
			require.Equal(t, len(q.results), len(results), "bad results for %s query %d", test.scenario, n)

			for i := range results {
				r1, r2 := q.results[i], results[i]
				requireEqualClusterMappings(t, r1, r2, fmt.Sprintf("bad result %d for %s query %d", i, test.scenario, n))
			}
		}
	}
}

func Benchmark1Cluster(b *testing.B) {
	benchmarkNClusterSplits(b, 1, 1)
}

func Benchmark2ClusterSplits(b *testing.B) {
	benchmarkNClusterSplits(b, 2, 2)
}

func Benchmark8ClusterSplits(b *testing.B) {
	benchmarkNClusterSplits(b, 8, 8)
}

func Benchmark32ClusterSplits(b *testing.B) {
	benchmarkNClusterSplits(b, 32, 33)
}

func Benchmark64ClusterSplits(b *testing.B) {
	benchmarkNClusterSplits(b, 64, 70)
}

func Benchmark128ClusterSplits(b *testing.B) {
	benchmarkNClusterSplits(b, 64, 70)
}

func Benchmark256ClusterSplits(b *testing.B) {
	benchmarkNClusterSplits(b, 256, 176)
}

func benchmarkNClusterSplits(b *testing.B, numSplits, expectedLoMappings int) {
	ts := newPlacementTestSuiteWithLogger(b, xlog.NullLogger)

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
		}))

		ts.commitLatest()
	}

	// Prepare a new provider
	p, err := NewClusterMappingProvider(NewClusterMappingProviderOptions().
		Clock(ts.clock).
		Logger(xlog.NullLogger))

	require.NoError(b, err)
	prov := p.(*clusterMappingProvider)
	prov.update(ts.latestPlacement())

	start, end := ts.clock.Now().Add(-time.Hour*24), ts.clock.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		loMappings := collectMappings(prov.QueryMappings(0, start, end))
		require.Equal(b, expectedLoMappings, len(loMappings))
	}
}

func collectMappings(iter storage.ClusterMappingIter) []storage.ClusterMapping {
	var results []storage.ClusterMapping
	for iter.Next() {
		results = append(results, iter.Current())
	}

	return results
}

type queryResult struct {
	cluster             string
	read, write, cutoff time.Time
}

func (m queryResult) ReadCutoverTime() time.Time  { return m.read }
func (m queryResult) WriteCutoverTime() time.Time { return m.write }
func (m queryResult) CutoffTime() time.Time       { return m.cutoff }
func (m queryResult) Cluster() string             { return m.cluster }

func requireEqualClusterMappings(t *testing.T, expected, actual storage.ClusterMapping, name string) {
	require.Equal(t, expected.ReadCutoverTime().String(), actual.ReadCutoverTime().String(),
		"%s ReadCutoverTime", name)
	require.Equal(t, expected.WriteCutoverTime().String(), actual.WriteCutoverTime().String(),
		"%s WriteCutoverTime", name)
	require.Equal(t, expected.CutoffTime().String(), actual.CutoffTime().String(),
		"%s CutoffTime", name)
	require.Equal(t, expected.Cluster(), actual.Cluster(), "%s Cluster", name)
}

const (
	testShards = 4096
)
