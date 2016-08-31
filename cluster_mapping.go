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
	"time"

	"github.com/facebookgo/clock"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"
)

// A ClusterMappingProvider provides cluster mapping rules
type ClusterMappingProvider interface {
	// MappingsFor returns all of the currently active cluster mappings for the
	// given shard and retention policy
	MappingsForShard(shard uint32, policy RetentionPolicy) ClusterMappingIter
}

// ClusterMappingIter is an iterator over ClusterMappings.  Allows provider to
// control how these are stored internally
type ClusterMappingIter interface {
	// Next moves to the next mapping, returning false if there are no more
	// mappings
	Next() bool

	// Current returns the current mapping
	Current() ClusterMapping
}

// A ClusterMapping defines which cluster holds the datapoints within a given timeframe
type ClusterMapping interface {
	// Cluster is the cluster that is targeted by this mapping
	Cluster() string

	// CutoffTime defines the time that reads from FromCluster should stop.  Will
	// inherently fall after the WriteCutoverTime, to account for configuration
	// changes not arriving at all writers simultaneously
	CutoffTime() time.Time

	// ReadCutoverTime defines the time that reads to ToCluster should begin
	ReadCutoverTime() time.Time

	// WriteCutoverTime defines the time that writes to ToCluster should begin.  Will
	// inherently fall after ReadCutoverTime, to account for configuration changes
	// reaching writers ahead of readers.
	WriteCutoverTime() time.Time
}

// clusterQueryPlanner produces plans for distributing queries across clusters
type clusterQueryPlanner struct {
	clock clock.Clock
	log   xlog.Logger
	p     ClusterMappingProvider
}

// newClusterQueryPlanner returns a new cluster query planner given a set of initial mappings
func newClusterQueryPlanner(provider ClusterMappingProvider, clock clock.Clock, log xlog.Logger) *clusterQueryPlanner {
	return &clusterQueryPlanner{
		p:     provider,
		clock: clock,
		log:   log,
	}
}

// buildClusterQueryPlan takes a set of queries and returns the set of clusters
// that need to be queries, along with the time range for each query.  Assumes
// the mapping rules are sorted by cutoff time, with the most recently applied
// rule appearing first.
func (p *clusterQueryPlanner) buildClusterQueryPlan(shard uint32, queries []query) ([]clusterQuery, error) {
	cqueries := make([]clusterQuery, 0, len(queries))
	for _, q := range queries {
		// Find the mappings for the query retention period
		// TODO(mmihic): and resolution?
		mappings := p.p.MappingsForShard(shard, q.RetentionPolicy)
		if mappings == nil {
			// No mappings for this retention policy, skip it
			continue
		}

		// Find all of the rules that apply to the query time range.
		p.log.Debugf("building query plan from %v to %v for period %s",
			q.Range.Start, q.Range.End, q.RetentionPolicy.RetentionPeriod().Duration())

		for mappings.Next() {
			m := mappings.Current()

			if !m.CutoffTime().IsZero() && m.CutoffTime().Before(q.Range.Start) {
				// We've reached a rule that ends before the start of the query - no more rules apply
				p.log.Debugf("cluster mapping cutoff time %s before query start %s, stopping build",
					m.CutoffTime(), q.Range.Start)
				break
			}

			if !m.ReadCutoverTime().IsZero() && m.ReadCutoverTime().After(q.Range.End) {
				// We've reached a rule that doesn't apply by the time the query ends - keep going until
				// we fnd an older rule which might apply
				p.log.Debugf("cluster mapping cutover time %s after query end %s, skipping rule",
					m.ReadCutoverTime(), q.Range.End)
				continue
			}

			// Capture the cluster to query...
			cq := clusterQuery{
				Range: xtime.Range{
					Start: q.Range.Start,
					End:   q.Range.End,
				},
				RetentionPolicy: q.RetentionPolicy,
				cluster:         m.Cluster(),
			}

			// ...and bound the query time by the mapping time range
			if !m.CutoffTime().IsZero() && m.CutoffTime().Before(cq.Range.End) {
				cq.Range.End = m.CutoffTime()
			}

			if !m.ReadCutoverTime().IsZero() && m.ReadCutoverTime().After(cq.Range.Start) {
				cq.Range.Start = m.ReadCutoverTime()
			}

			if cq.Range.IsEmpty() {
				continue
			}

			cqueries = append(cqueries, cq)
		}
	}

	// TODO(mmihic): Coalesce
	return cqueries, nil
}

type clusterQuery struct {
	xtime.Range
	RetentionPolicy
	cluster string
}
