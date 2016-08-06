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
	"fmt"
	"strings"
	"time"

	"github.com/facebookgo/clock"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"
)

// A RetentionPolicy describes the resolution and retention period for a set of
// datapoints (e.g. 1min at 30d)
type RetentionPolicy interface {
	// Resolution is the resolution at which the datapoints will be stored
	Resolution() Resolution

	// RetentionPeriod is the amount of time to retain the datapoints
	RetentionPeriod() RetentionPeriod

	// Equal checks whether this retention policy is equal to another
	Equal(other RetentionPolicy) bool
}

// RetentionPoliciesByRetentionPeriod is a sort.Interface for sorting a slice of
// RetentionPolicy objects by the duration of their retention period, shortest
// retention period first
type RetentionPoliciesByRetentionPeriod []RetentionPolicy

// Less compares two retention rules by their cutoff time
func (rr RetentionPoliciesByRetentionPeriod) Less(i, j int) bool {
	return rr[i].RetentionPeriod().Duration() < rr[j].RetentionPeriod().Duration()
}

// Swap swaps two retention rules in the slice
func (rr RetentionPoliciesByRetentionPeriod) Swap(i, j int) { rr[i], rr[j] = rr[j], rr[i] }

// Len returns the length of the retention rule slice
func (rr RetentionPoliciesByRetentionPeriod) Len() int { return len(rr) }

// NewRetentionPolicy creates a new RetentionPolicy
func NewRetentionPolicy(r Resolution, p RetentionPeriod) RetentionPolicy {
	return retentionPolicy{r: r, p: p}
}

// ParseRetentionPolicy parses a retention policy ion the form of resolution:period
func ParseRetentionPolicy(s string, precision xtime.Unit) (RetentionPolicy, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid retention policy %s, expect in form 10s:7d", s)
	}

	rDuration, err := xtime.ParseExtendedDuration(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid retention policy %s, invalid resolution %s", s, parts[0])
	}
	r := NewResolution(parts[0], rDuration, precision)

	pDuration, err := xtime.ParseExtendedDuration(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid retention policy %s, invalid retention period %s", s, parts[1])
	}
	p := NewRetentionPeriod(parts[1], pDuration)

	return NewRetentionPolicy(r, p), nil
}

// A RetentionRule defines the retention policies that apply to a set of
// metrics at a particular time.  Retention policy can change dynamically over
// time in response to user actions, so retention rules have a CutoverTime and
// CutoffTime which determine when the rules apply.  When determining which
// retention periods to query over, the storage manager determines which rule
// apply within the query time range, then builds a set of sub-queries covering
// each retention period
type RetentionRule interface {
	// RetentionPolicies are the retention policies at the time of the rule, ordered
	// by retention period (from shorted to longest)
	RetentionPolicies() []RetentionPolicy
	SetRetentionPolicies(p []RetentionPolicy) RetentionRule

	// CutoverTime is the time that the rule will begin to be applied
	CutoverTime() time.Time
	SetCutoverTime(t time.Time) RetentionRule

	// CutoffTime is the time that the rule no longer applies
	CutoffTime() time.Time
	SetCutoffTime(t time.Time) RetentionRule
}

// NewRetentionRule creates a new retention rule
func NewRetentionRule() RetentionRule { return new(retentionRule) }

// RetentionRulesByCutoffTime is a sort.Interface for sorting RetentionRules by
// CutoffTime, with the latest cutoff time first
type RetentionRulesByCutoffTime []RetentionRule

// Less compares two retention rules by their cutoff time
func (rr RetentionRulesByCutoffTime) Less(i, j int) bool {
	if rr[i].CutoffTime().IsZero() {
		return true
	}

	if rr[j].CutoffTime().IsZero() {
		return false
	}

	return rr[i].CutoffTime().After(rr[j].CutoffTime())
}

// Swap swaps two retention rules in the slice
func (rr RetentionRulesByCutoffTime) Swap(i, j int) { rr[i], rr[j] = rr[j], rr[i] }

// Len returns the length of the retention rule slice
func (rr RetentionRulesByCutoffTime) Len() int { return len(rr) }

// RetentionRuleProvider looks up retention rules for a given id and timerange
type RetentionRuleProvider interface {
	// FindRetentionRules returns the list of retention rules that apply
	// for the given id over the requested time range
	FindRetentionRules(id string, start, end time.Time) ([]RetentionRule, error)
}

type retentionPolicy struct {
	r Resolution
	p RetentionPeriod
}

func (p retentionPolicy) Resolution() Resolution           { return p.r }
func (p retentionPolicy) RetentionPeriod() RetentionPeriod { return p.p }
func (p retentionPolicy) Equal(other RetentionPolicy) bool {
	return p.r.Equal(other.Resolution()) && p.p.Equal(other.RetentionPeriod())
}

type retentionRule struct {
	policies                []RetentionPolicy
	cutoffTime, cutoverTime time.Time
}

func (r *retentionRule) RetentionPolicies() []RetentionPolicy { return r.policies }
func (r *retentionRule) CutoverTime() time.Time               { return r.cutoverTime }
func (r *retentionRule) CutoffTime() time.Time                { return r.cutoffTime }

func (r *retentionRule) SetRetentionPolicies(p []RetentionPolicy) RetentionRule {
	r.policies = p
	return r
}

func (r *retentionRule) SetCutoverTime(t time.Time) RetentionRule {
	r.cutoverTime = t
	return r
}

func (r *retentionRule) SetCutoffTime(t time.Time) RetentionRule {
	r.cutoffTime = t
	return r
}

type query struct {
	xtime.Range
	RetentionPolicy
}

type retentionQueryPlanner struct {
	log   xlog.Logger
	clock clock.Clock
}

func (p retentionQueryPlanner) buildRetentionQueryPlan(from, until time.Time, rules []RetentionRule) ([]query, error) {
	now := p.clock.Now()

	p.log.Debugf("building query plan from %v to %v over %d rules", from, until, len(rules))

	var queries []query

	// Assumes rules are sorted by cutoff time, with most recent cutoff time first.
	for _, r := range rules {
		if !r.CutoffTime().IsZero() && r.CutoffTime().Before(from) {
			// We've reached a rule that end before the start of the query - no more rules will apply
			p.log.Debugf("rule cutoff time %s before query start %s, stopping build", r.CutoffTime(), from)
			break
		}

		if !r.CutoverTime().IsZero() && r.CutoverTime().After(until) {
			// We've reached a rule that doesn't apply by the time the query ends - keep going until we find
			// an older rule which might apply
			p.log.Debugf("rule cutover time %s after query end %s, skipping rule", r.CutoverTime(), until)
			continue
		}

		// Bound the application of the rule by the query range
		ruleStart, ruleEnd := from, until
		if !r.CutoffTime().IsZero() && r.CutoffTime().Before(ruleEnd) {
			ruleEnd = r.CutoffTime()
		}

		if !r.CutoverTime().IsZero() && r.CutoverTime().After(ruleStart) {
			ruleStart = r.CutoverTime()
		}

		p.log.Debugf("rule applies from %s until %s", ruleStart, ruleEnd)

		// Determine queries based on the retention policies in the rule.  Assumes
		// that the retention policies are ordered by retention period, shortest
		// retention period first.  Stops as soon as we've reached the last policy
		// whose retention period falls within the rule or query bounds
		lastPolicyStart := ruleEnd
		policies := r.RetentionPolicies()
		for n, policy := range policies {
			query := query{
				RetentionPolicy: policy,
				Range: xtime.Range{
					End:   lastPolicyStart,
					Start: now.Add(-policy.RetentionPeriod().Duration()),
				},
			}

			if !query.Range.Start.Before(ruleEnd) {
				p.log.Debugf("policy %d (%s:%s) starts (%s) after end of rule (%s), does not apply",
					n, policy.Resolution().Name(), policy.RetentionPeriod().Name(), query.Range.Start, ruleEnd)
				continue
			}

			if query.Range.End.Before(ruleStart) {
				p.log.Debugf("policy %d (%s:%s) ends (%s) before the start of the rule (%s), no more policies apply",
					n, policy.Resolution().Name(), policy.RetentionPeriod().Name(), query.Range.End, ruleStart)
				break
			}

			if query.Range.Start.Before(ruleStart) {
				query.Range.Start = ruleStart
			}

			if query.Range.End.After(ruleEnd) {
				query.Range.End = ruleEnd
			}

			p.log.Debugf("policy %d (%s:%s) applies from %s until %s (%s)",
				n, policy.Resolution().Name(), policy.RetentionPeriod().Name(),
				query.Range.Start, query.Range.End, query.Range.End.Sub(query.Range.Start))

			if query.Range.IsEmpty() {
				p.log.Debugf("skipping policy %d; does not apply within query range", n)
				continue
			}

			lastPolicyStart = query.Range.Start
			queries = append(queries, query)
		}
	}

	if len(queries) == 0 {
		return queries, nil
	}

	// Coalesce queries for the same retention policy
	lastQuery := queries[0]
	coalescedQueries := make([]query, 0, len(queries))
	for i := 1; i < len(queries); i++ {
		if queries[i].RetentionPolicy.Equal(lastQuery.RetentionPolicy) {
			lastQuery.Start = xtime.MinTime(queries[i].Start, lastQuery.Start)
			lastQuery.End = xtime.MaxTime(queries[i].End, lastQuery.End)
			continue
		}

		coalescedQueries = append(coalescedQueries, lastQuery)
		lastQuery = queries[i]
	}

	coalescedQueries = append(coalescedQueries, lastQuery)
	return coalescedQueries, nil
}
