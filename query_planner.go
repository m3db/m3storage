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

	"github.com/m3db/m3storage/mapping"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"

	"github.com/facebookgo/clock"
)

// queryPlanner produces plans for splitting queries across clusters
type queryPlanner struct {
	clock clock.Clock
	log   xlog.Logger
	p     mapping.Provider
}

// newQueryPlanner returns a new cluster query planner given a set of initial rules
func newQueryPlanner(provider mapping.Provider, clock clock.Clock, log xlog.Logger) *queryPlanner {
	return &queryPlanner{
		p:     provider,
		clock: clock,
		log:   log,
	}
}

// plan takes a query and returns the set of clusters that need to be queried
// along with the time range for each query.  Assumes the mapping rules are
// sorted by cutoff time, with the most recently applied rule appearing first.
func (p *queryPlanner) plan(shard uint32, start, end time.Time) ([]query, error) {
	var queries []query

	rules, err := p.p.FetchRules(shard, start, end)
	if err != nil {
		return nil, err
	}

	if rules == nil {
		// No rules for this time range, so bail
		return nil, nil
	}

	// Find all of the rules that apply to the query time range.
	p.log.Debugf("building query plan from %v to %v", start, end)

	for rules.Next() {
		m := rules.Current()

		if !m.CutoffTime().IsZero() && m.CutoffTime().Before(start) {
			// We've reached a rule that ends before the start of the query - no more rules apply
			p.log.Debugf("cluster mapping cutoff time %s before query start %s, stopping build",
				m.CutoffTime(), start)
			break
		}

		if !m.ReadCutoverTime().IsZero() && m.ReadCutoverTime().After(end) {
			// We've reached a rule that doesn't apply by the time the query ends - keep going until
			// we fnd an older rule which might apply
			p.log.Debugf("cluster mapping cutover time %s after query end %s, skipping rule",
				m.ReadCutoverTime(), end)
			continue
		}

		// Capture the cluster to query...
		q := query{
			Range: xtime.Range{
				Start: start,
				End:   end,
			},
			cluster:  m.Cluster(),
			database: m.Database(),
		}

		// ...and bound the query time by the mapping time range
		if !m.CutoffTime().IsZero() && m.CutoffTime().Before(q.Range.End) {
			q.Range.End = m.CutoffTime()
		}

		if !m.ReadCutoverTime().IsZero() && m.ReadCutoverTime().After(q.Range.Start) {
			q.Range.Start = m.ReadCutoverTime()
		}

		if q.Range.IsEmpty() {
			continue
		}

		queries = append(queries, q)
	}

	if err := rules.Close(); err != nil {
		return nil, err
	}

	// TODO(mmihic): Coalesce
	return queries, nil
}

type query struct {
	xtime.Range
	database, cluster string
}
