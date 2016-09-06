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

package retention

import (
	"errors"
	"time"

	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"

	"github.com/facebookgo/clock"
)

var (
	errInvalidRange = errors.New("invalid range; from does not precede until")
)

// A PolicyTimeRange is the application of a retention Policy within a given
// time range
type PolicyTimeRange struct {
	xtime.Range
	Policy
}

// PolicyResolverOptions are options for creating a PolicyResolver
type PolicyResolverOptions interface {
	// Logger is the resolver's logger
	Logger(logger xlog.Logger) PolicyResolverOptions
	GetLogger() xlog.Logger

	// Clock is the resolver's clock
	Clock(c clock.Clock) PolicyResolverOptions
	GetClock() clock.Clock
}

// NewPolicyResolverOptions returns a new set of PolicyResolverOptions
func NewPolicyResolverOptions() PolicyResolverOptions {
	return new(policyResolverOptions)
}

// A PolicyResolver resolves policies based on a set of retention rules
// TODO(mmihic): This should be driven off a RuleProvider, shard, etc.
type PolicyResolver interface {
	// ResolveTimeline resolves the retention Policy timeline for a given id
	ResolveTimeline(id string, from, until time.Time) ([]PolicyTimeRange, error)
}

// NewPolicyResolver returns a new PolicyResolver given a rule provider
func NewPolicyResolver(p RuleProvider, opts PolicyResolverOptions) (PolicyResolver, error) {
	if opts == nil {
		opts = NewPolicyResolverOptions()
	}

	c, log := opts.GetClock(), opts.GetLogger()
	if c == nil {
		c = clock.New()
	}

	if log == nil {
		log = xlog.NullLogger
	}

	return &policyResolver{
		log:   log,
		clock: c,
		p:     p,
	}, nil
}

type policyResolver struct {
	log   xlog.Logger
	clock clock.Clock
	p     RuleProvider
}

func (resolv policyResolver) ResolveTimeline(id string, from, until time.Time) ([]PolicyTimeRange, error) {
	if !from.Before(until) {
		return nil, errInvalidRange
	}

	now := resolv.clock.Now()
	rules, err := resolv.p.FindRules(id, from, until)
	if err != nil {
		return nil, err
	}

	resolv.log.Debugf("building query plan from %v to %v over %d rules", from, until, len(rules))

	var timeline []PolicyTimeRange

	// Assumes rules are sorted by cutoff time, with most recent cutoff time first.
	for _, r := range rules {
		if !r.CutoffTime().IsZero() && r.CutoffTime().Before(from) {
			// We've reached a rule that end before the start of the query - no more rules will apply
			resolv.log.Debugf("rule cutoff time %s before query start %s, stopping build", r.CutoffTime(), from)
			break
		}

		if !r.CutoverTime().IsZero() && r.CutoverTime().After(until) {
			// We've reached a rule that doesn't apply by the time the query ends - keep going until we find
			// an older rule which might apply
			resolv.log.Debugf("rule cutover time %s after query end %s, skipping rule", r.CutoverTime(), until)
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

		resolv.log.Debugf("rule applies from %s until %s", ruleStart, ruleEnd)

		// Determine queries based on the retention policies in the rule.  Assumes
		// that the retention policies are ordered by retention period, shortest
		// retention period first.  Stops as soon as we've reached the last policy
		// whose retention period falls within the rule or query bounds
		lastPolicyStart := ruleEnd
		policies := r.Policies()
		for n, policy := range policies {
			tl := PolicyTimeRange{
				Policy: policy,
				Range: xtime.Range{
					End:   lastPolicyStart,
					Start: now.Add(-policy.Period().Duration()),
				},
			}

			if !tl.Range.Start.Before(ruleEnd) {
				resolv.log.Debugf("policy %d (%s:%s) starts (%s) after end of rule (%s), does not apply",
					n, policy.Resolution(), policy.Period(), tl.Range.Start, ruleEnd)
				continue
			}

			if tl.Range.End.Before(ruleStart) {
				resolv.log.Debugf("policy %d (%s:%s) ends (%s) before the start of the rule (%s), no more policies apply",
					n, policy.Resolution(), policy.Period(), tl.Range.End, ruleStart)
				break
			}

			if tl.Range.Start.Before(ruleStart) {
				tl.Range.Start = ruleStart
			}

			if tl.Range.End.After(ruleEnd) {
				tl.Range.End = ruleEnd
			}

			resolv.log.Debugf("policy %d (%s:%s) applies from %s until %s (%s)",
				n, policy.Resolution(), policy.Period(),
				tl.Range.Start, tl.Range.End, tl.Range.End.Sub(tl.Range.Start))

			if tl.Range.IsEmpty() {
				resolv.log.Debugf("skipping policy %d; does not apply within query range", n)
				continue
			}

			lastPolicyStart = tl.Range.Start
			timeline = append(timeline, tl)
		}
	}

	if len(timeline) == 0 {
		return timeline, nil
	}

	// Coalesce timeline for the same retention policy
	lastTimeline := timeline[0]
	coalescedTimelines := make([]PolicyTimeRange, 0, len(timeline))
	for i := 1; i < len(timeline); i++ {
		if timeline[i].Policy.Equal(lastTimeline.Policy) {
			lastTimeline.Start = timeline[i].Start
			continue
		}

		coalescedTimelines = append(coalescedTimelines, lastTimeline)
		lastTimeline = timeline[i]
	}

	coalescedTimelines = append(coalescedTimelines, lastTimeline)
	return coalescedTimelines, nil
}

// policyResolverOptions are options to the PolicyResolver
type policyResolverOptions struct {
	log   xlog.Logger
	clock clock.Clock
}

func (opts policyResolverOptions) GetLogger() xlog.Logger { return opts.log }
func (opts policyResolverOptions) GetClock() clock.Clock  { return opts.clock }

func (opts policyResolverOptions) Logger(log xlog.Logger) PolicyResolverOptions {
	opts.log = log
	return opts
}

func (opts policyResolverOptions) Clock(clock clock.Clock) PolicyResolverOptions {
	opts.clock = clock
	return opts
}
