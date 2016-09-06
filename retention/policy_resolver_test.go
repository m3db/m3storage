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
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"

	"github.com/facebookgo/clock"
	"github.com/stretchr/testify/require"
)

func TestPolicyResolver_ResolveTimeline(t *testing.T) {
	const day = time.Hour * 24

	clock := clock.NewMock()
	clock.Add(time.Hour * 24 * 365)
	now := clock.Now()

	tests := []struct {
		name        string
		from, until time.Duration
		rules       []ruleSpec
		timeline    []timeRangeSpec
	}{
		// query whose end time is past the cutoff for the rule - should be
		// truncated to cutoff point
		{name: "query extends after end",
			from: day * 60, until: day * 1,
			rules: []ruleSpec{
				{policies: "1s:12h,10s:2d,1min:45d", cutoff: day * 5},
			},
			timeline: []timeRangeSpec{
				{"1min:45d", day * 45, day * 5},
			},
		},

		// query whose start time is before the cutover for the rule - should
		// be truncated to cutover point
		{name: "query extends before start",
			from: day * 60, until: day * 1,
			rules: []ruleSpec{
				{policies: "1s:12h,10s:2d,1min:45d", cutover: day * 30},
			},
			timeline: []timeRangeSpec{
				{"10s:2d", day * 2, day * 1},
				{"1min:45d", day * 30, day * 2},
			},
		},

		// rule with cutoff time, query entirely past cutoff time
		{name: "query entirely outside rules",
			from: day * 5, until: 0,
			rules: []ruleSpec{
				{policies: "1s:12h,10s:2d", cutoff: day * 30},
			},
			timeline: []timeRangeSpec{},
		},

		// rule with cutover and cutoff time, but query entirely within rule bounds
		{name: "query entirely within rules",
			from: day * 5, until: day * 1,
			rules: []ruleSpec{
				{policies: "1s:12h,10s:7d,1min:30d", cutoff: time.Hour * 2, cutover: day * 40},
			},
			timeline: []timeRangeSpec{
				{"10s:7d", day * 5, day * 1},
			},
		},

		// rule without cutover or cutoff time, query fully satisfied
		{name: "rules extend forever",
			from: day * 10, until: 0,
			rules: []ruleSpec{
				{policies: "1s:12h,10s:2d,1min:45d"},
			},
			timeline: []timeRangeSpec{
				{"1s:12h", time.Hour * 12, 0},
				{"10s:2d", day * 2, time.Hour * 12},
				{"1min:45d", day * 10, day * 2},
			},
		},

		// simply cutover between two rules
		{name: "simple cutover",
			from: day * 19, until: 0,
			rules: []ruleSpec{
				{policies: "1s:12h,10s:2d,1min:7d,5min:14d", cutover: time.Hour * 6},
				{policies: "10s:12h,1min:3d,5min:30d", cutoff: time.Hour * 6},
			},
			timeline: []timeRangeSpec{
				{"1s:12h", time.Hour * 6, 0},
				{"10s:12h", time.Hour * 12, time.Hour * 6},
				{"1min:3d", day * 3, time.Hour * 12},
				{"5min:30d", day * 19, day * 3},
			},
		},

		// multiple cutovers between rules
		{name: "multiple cutovers",
			from: day * 19, until: 0,
			rules: []ruleSpec{
				{policies: "10s:1d,1min:5d,5min:30d", cutover: time.Hour * 14},
				{policies: "1s:12h,10s:1d,1min:5d,5min:30d", cutoff: time.Hour * 14, cutover: day * 3},
				{policies: "10s:2d,1min:7d,5min:30d", cutoff: day * 3, cutover: day * 6},
				{policies: "10s:2d,5min:30d", cutoff: day * 6},
			},
			timeline: []timeRangeSpec{
				{"10s:1d", day, 0}, // NB(mmihic): Coalesced
				{"1min:5d", day * 3, day},
				{"1min:7d", day * 6, day * 3},
				{"5min:30d", day * 19, day * 6},
			},
		},

		// duplicate retention period
		{name: "duplicate retention period",
			from: day * 19, until: 0,
			rules: []ruleSpec{
				{policies: "10s:1d,1s:1d,1min:5d,5min:30d"},
			},
			timeline: []timeRangeSpec{
				{"1s:1d", day, 0},
				{"1min:5d", day * 5, day},
				{"5min:30d", day * 19, day * 5},
			},
		},
	}

	for _, test := range tests {
		p := fakeRuleProvider{
			rules: test.rules,
			clock: clock,
		}

		resolv, err := NewPolicyResolver(p, NewPolicyResolverOptions().
			Clock(clock).
			Logger(xlog.SimpleLogger))
		require.NoError(t, err)

		from, until := now.Add(-test.from), now.Add(-test.until)
		actualTimeline, err := resolv.ResolveTimeline("foo", from, until)
		require.NoError(t, err, "error resolving timeline for %s", test.name)

		expectedTimeline := buildExpectedTimeline(now, test.timeline)
		for i := range actualTimeline {
			if i == len(expectedTimeline) {
				require.True(t, false, "actual plan contains more elements than expected %d vs %d",
					len(actualTimeline), len(expectedTimeline))
			}

			expected, actual := expectedTimeline[i], actualTimeline[i]
			require.Equal(t, expected.Policy.String(), actual.Policy.String(),
				"invalid retention policy for %s[%d]", test.name, i)
			require.Equal(t, now.Sub(expected.Start).String(), now.Sub(actual.Start).String(),
				"invalid from for %s[%d]", test.name, i)
			require.Equal(t, now.Sub(expected.End).String(), now.Sub(actual.End).String(),
				"invalid until for %s[%d]", test.name, i)
		}
	}
}

// fakeRuleProvider is a fake implementation of RuleProvider
type fakeRuleProvider struct {
	rules []ruleSpec
	clock clock.Clock
}

func (p fakeRuleProvider) FindRules(id string, start, end time.Time) ([]Rule, error) {
	rules := make([]Rule, 0, len(p.rules))
	for _, spec := range p.rules {
		policies := MustParsePolicies(spec.policies)
		rule := NewRule().SetPolicies(policies)
		if spec.cutoff > 0 {
			rule.SetCutoffTime(p.clock.Now().Add(-spec.cutoff))
		}

		if spec.cutover > 0 {
			rule.SetCutoverTime(p.clock.Now().Add(-spec.cutover))
		}
		rules = append(rules, rule)
	}
	sort.Sort(RulesByCutoffTime(rules))
	return rules, nil
}

type ruleSpec struct {
	cutover, cutoff time.Duration
	policies        string
}

func buildExpectedTimeline(now time.Time, expected []timeRangeSpec) []PolicyTimeRange {
	tl := make([]PolicyTimeRange, 0, len(expected))
	for _, spec := range expected {
		tl = append(tl, PolicyTimeRange{
			Range: xtime.Range{
				Start: now.Add(-spec.from),
				End:   now.Add(-spec.until),
			},
			Policy: MustParsePolicy(spec.policy),
		})
	}

	return tl
}

type timeRangeSpec struct {
	policy      string
	from, until time.Duration
}
