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

package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/facebookgo/clock"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"
	"github.com/stretchr/testify/require"
)

func TestBuildClusterQueryPlan(t *testing.T) {
	clock := clock.NewMock()
	now := clock.Now().Add(time.Hour * 24 * 365)

	keep1d := MustParseRetentionPolicy("10s:1d")
	//	keep2d := MustParseRetentionPolicy("30s:2d")
	//	keep7d := MustParseRetentionPolicy("1min:7d")
	from, until := now.Add(-time.Hour*12), now.Add(-time.Minute*5)

	tests := []struct {
		name     string
		queries  []query
		mappings fakeClusterMappingProvider
		expected []clusterQuery
	}{
		{name: " Mapping has no cutoff or cutover time",
			queries: []query{
				{Range: xtime.Range{from, until}, RetentionPolicy: keep1d},
			},
			mappings: fakeClusterMappingProvider{
				&clusterMapping{cluster: "foozle"},
			},
			expected: []clusterQuery{
				{Range: xtime.Range{from, until}, RetentionPolicy: keep1d, cluster: "foozle"},
			},
		},
		{name: "Mapping has cutoff time before the start of the query",
			queries: []query{
				{Range: xtime.Range{from, until}, RetentionPolicy: keep1d},
			},
			mappings: fakeClusterMappingProvider{
				&clusterMapping{readCutoverTime: from.Add(-time.Millisecond * 5), cluster: "barzle"},
				&clusterMapping{cutoffTime: from.Add(-time.Millisecond), cluster: "foozle"},
			},
			expected: []clusterQuery{
				{Range: xtime.Range{from, until}, RetentionPolicy: keep1d, cluster: "barzle"},
			},
		},
		{name: "Mapping has cutoff time after the start of the query but before the end",
			queries: []query{
				{Range: xtime.Range{from, until}, RetentionPolicy: keep1d},
			},
			mappings: fakeClusterMappingProvider{
				&clusterMapping{readCutoverTime: from.Add(-time.Millisecond * 5), cluster: "barzle"},
				&clusterMapping{cutoffTime: from.Add(time.Millisecond * 10), cluster: "foozle"},
			},
			expected: []clusterQuery{
				{Range: xtime.Range{from, until}, RetentionPolicy: keep1d, cluster: "barzle"},
				{Range: xtime.Range{from, from.Add(time.Millisecond * 10)}, RetentionPolicy: keep1d, cluster: "foozle"},
			},
		},
		{name: "Mapping has cutoff time after the end of the query",
			queries: []query{
				{Range: xtime.Range{from, until}, RetentionPolicy: keep1d},
			},
			mappings: fakeClusterMappingProvider{
				&clusterMapping{cutoffTime: until.Add(time.Millisecond * 10), cluster: "foozle"},
			},
			expected: []clusterQuery{
				{Range: xtime.Range{from, until}, RetentionPolicy: keep1d, cluster: "foozle"},
			},
		},
		{name: "Mapping has cutover time after the end of the query",
			queries: []query{
				{Range: xtime.Range{from, until}, RetentionPolicy: keep1d},
			},
			mappings: fakeClusterMappingProvider{
				&clusterMapping{readCutoverTime: until.Add(time.Millisecond * 5), cluster: "barzle"},
			},
			expected: []clusterQuery{},
		},
		{name: "Mapping has cutover time after the start but before the end of the query",
			queries: []query{
				{Range: xtime.Range{from, until}, RetentionPolicy: keep1d},
			},
			mappings: fakeClusterMappingProvider{
				&clusterMapping{readCutoverTime: from.Add(time.Millisecond * 20), cluster: "barzle"},
			},
			expected: []clusterQuery{
				{Range: xtime.Range{from.Add(time.Millisecond * 20), until}, RetentionPolicy: keep1d, cluster: "barzle"},
			},
		},
		{name: "Mapping has cutover time before the start of the query",
			queries: []query{
				{Range: xtime.Range{from, until}, RetentionPolicy: keep1d},
			},
			mappings: fakeClusterMappingProvider{
				&clusterMapping{readCutoverTime: from.Add(-time.Millisecond * 20), cluster: "barzle"},
			},
			expected: []clusterQuery{
				{Range: xtime.Range{from, until}, RetentionPolicy: keep1d, cluster: "barzle"},
			},
		},
		// TODO(mmihic): This should be treated as an error
		{name: "No mapping exists for the provided shard+policy",
			queries: []query{
				{Range: xtime.Range{from, until}, RetentionPolicy: keep1d},
			},
			mappings: fakeClusterMappingProvider{},
			expected: nil,
		},

		// TODO(mmihic): Coalesce same cluster
	}

	for _, test := range tests {
		qp := newClusterQueryPlanner(test.mappings, clock, xlog.SimpleLogger)
		actualPlan, err := qp.buildClusterQueryPlan(103, test.queries)
		require.NoError(t, err)
		require.Equal(t, len(test.expected), len(actualPlan), "incorrect plan count for %s", test.name)

		for n := range actualPlan {
			actual, expected := actualPlan[n], test.expected[n]
			require.Equal(t, expected.RetentionPolicy.String(), actual.RetentionPolicy.String(),
				"wrong policy for %s[%d]", test.name, n)
			require.Equal(t, expected.Start.String(), actual.Start.String(),
				"wrong start time for %s[%d]", test.name, n)
			require.Equal(t, expected.End.String(), actual.End.String(),
				"wrong end time for %s[%d]", test.name, n)
			require.Equal(t, expected.cluster, actual.cluster,
				"wrong cluster for %s[%d]", test.name, n)
		}
	}
}

type fakeClusterMappingProvider []ClusterMapping

func (scm fakeClusterMappingProvider) QueryMappings(shard uint32, start, end time.Time) (ClusterMappingIter, error) {
	return &fakeClusterMappingIter{
		mappings: scm,
		current:  0,
		next:     0,
	}, nil
}

func (scm fakeClusterMappingProvider) WatchCluster(database, cluster string) (ClusterWatch, error) {
	// TODO(mmihic): Support later
	return nil, errors.New("no such cluster")
}

func (scm fakeClusterMappingProvider) Close() error { return nil }

type fakeClusterMappingIter struct {
	mappings      []ClusterMapping
	current, next int
}

func (i *fakeClusterMappingIter) Next() bool {
	if i.next >= len(i.mappings) {
		return false
	}

	i.current, i.next = i.next, i.next+1
	return true
}

func (i *fakeClusterMappingIter) Current() ClusterMapping {
	if i.current < len(i.mappings) {
		return i.mappings[i.current]
	}

	return nil
}

func (i *fakeClusterMappingIter) Close() error { return nil }

type clusterMapping struct {
	readCutoverTime, writeCutoverTime, cutoffTime time.Time
	cluster, database                             string
}

func (m clusterMapping) ReadCutoverTime() time.Time  { return m.readCutoverTime }
func (m clusterMapping) WriteCutoverTime() time.Time { return m.writeCutoverTime }
func (m clusterMapping) CutoffTime() time.Time       { return m.cutoffTime }
func (m clusterMapping) Cluster() string             { return m.cluster }
func (m clusterMapping) Database() string            { return m.database }
