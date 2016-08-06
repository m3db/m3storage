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
	"sort"
	"testing"
	"time"

	"github.com/facebookgo/clock"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetentionRulesByCutoffTime(t *testing.T) {
	now := time.Now()

	rules := []RetentionRule{
		NewRetentionRule().SetCutoffTime(now.Add(-time.Hour * 12)),
		NewRetentionRule(),
		NewRetentionRule().SetCutoffTime(now.Add(-time.Hour * 6)),
	}

	sort.Sort(RetentionRulesByCutoffTime(rules))
	assert.True(t, rules[0].CutoffTime().IsZero())
	assert.Equal(t, time.Hour*6, now.Sub(rules[1].CutoffTime()))
	assert.Equal(t, time.Hour*12, now.Sub(rules[2].CutoffTime()))
}

func TestParseRetentionPolicy(t *testing.T) {
	p, err := ParseRetentionPolicy("10s:2d")
	require.NoError(t, err)
	assert.Equal(t, (time.Second * 10).String(), p.Resolution().WindowSize().String())
	assert.Equal(t, xtime.Millisecond, p.Resolution().Precision())
	assert.Equal(t, (time.Hour * 24 * 2).String(), p.RetentionPeriod().Duration().String())

	p, err = ParseRetentionPolicy("1min@1s:30d")
	require.NoError(t, err)
	assert.Equal(t, (time.Minute).String(), p.Resolution().WindowSize().String())
	assert.Equal(t, xtime.Second, p.Resolution().Precision())
	assert.Equal(t, (time.Hour * 24 * 30).String(), p.RetentionPeriod().Duration().String())

	p, err = ParseRetentionPolicy("10monkeys:2d")
	require.Error(t, err)
	assert.Nil(t, p)

	p, err = ParseRetentionPolicy("10s:10monkeys")
	require.Error(t, err)
	assert.Nil(t, p)

	p, err = ParseRetentionPolicy("10s")
	require.Error(t, err)
	assert.Nil(t, p)
}

func TestParseRetentionPolicies(t *testing.T) {
	// NB(mmihic): These are out of order so will need to be sorted
	policies, err := ParseRetentionPolicies("5s:2d,10s:2d,1min:7d,500ms:6h,5min:14d")
	require.NoError(t, err)

	for n, expected := range []struct {
		resolution time.Duration
		retention  time.Duration
	}{
		{time.Millisecond * 500, time.Hour * 6},
		{time.Second * 5, time.Hour * 24 * 2},
		{time.Second * 10, time.Hour * 24 * 2},
		{time.Minute, time.Hour * 24 * 7},
		{time.Minute * 5, time.Hour * 24 * 14},
	} {
		require.Equal(t, expected.resolution.String(), policies[n].Resolution().WindowSize().String())
		require.Equal(t, expected.retention.String(), policies[n].RetentionPeriod().Duration().String())
	}
}

func TestBuildRetentionQueryPlan(t *testing.T) {
	const day = time.Hour * 24

	clock := clock.NewMock()
	clock.Add(time.Hour * 24 * 365)
	planner := retentionQueryPlanner{
		log:   xlog.SimpleLogger,
		clock: clock,
	}

	now := planner.clock.Now()
	tests := []struct {
		name        string
		from, until time.Duration
		rules       []rule
		results     []expectedQuery
	}{
		// query whose end time is past the cutoff for the rule - should be
		// truncated to cutoff point
		{name: "query extends after end",
			from: day * 60, until: day * 1,
			rules: []rule{
				{policies: "1s:12h,10s:2d,1min:45d", cutoff: day * 5},
			},
			results: []expectedQuery{
				{"1min:45d", day * 45, day * 5},
			},
		},

		// query whose start time is before the cutover for the rule - should
		// be truncated to cutover point
		{name: "query extends before start",
			from: day * 60, until: day * 1,
			rules: []rule{
				{policies: "1s:12h,10s:2d,1min:45d", cutover: day * 30},
			},
			results: []expectedQuery{
				{"10s:2d", day * 2, day * 1},
				{"1min:45d", day * 30, day * 2},
			},
		},

		// rule with cutoff time, query entirely past cutoff time
		{name: "query entirely outside rules",
			from: day * 5, until: 0,
			rules: []rule{
				{policies: "1s:12h,10s:2d", cutoff: day * 30},
			},
			results: []expectedQuery{},
		},

		// rule with cutover and cutoff time, but query entirely within rule bounds
		{name: "query entirely within rules",
			from: day * 5, until: day * 1,
			rules: []rule{
				{policies: "1s:12h,10s:7d,1min:30d", cutoff: time.Hour * 2, cutover: day * 40},
			},
			results: []expectedQuery{
				{"10s:7d", day * 5, day * 1},
			},
		},

		// rule without cutover or cutoff time, query fully satisfied
		{name: "rules extend forever",
			from: day * 10, until: 0,
			rules: []rule{
				{policies: "1s:12h,10s:2d,1min:45d"},
			},
			results: []expectedQuery{
				{"1s:12h", time.Hour * 12, 0},
				{"10s:2d", day * 2, time.Hour * 12},
				{"1min:45d", day * 10, day * 2},
			},
		},

		// simply cutover between two rules
		{name: "simple cutover",
			from: day * 19, until: 0,
			rules: []rule{
				{policies: "1s:12h,10s:2d,1min:7d,5min:14d", cutover: time.Hour * 6},
				{policies: "10s:12h,1min:3d,5min:30d", cutoff: time.Hour * 6},
			},
			results: []expectedQuery{
				{"1s:12h", time.Hour * 6, 0},
				{"10s:12h", time.Hour * 12, time.Hour * 6},
				{"1min:3d", day * 3, time.Hour * 12},
				{"5min:30d", day * 19, day * 3},
			},
		},

		// multiple cutovers between rules
		{name: "multiple cutovers",
			from: day * 19, until: 0,
			rules: []rule{
				{policies: "10s:1d,1min:5d,5min:30d", cutover: time.Hour * 14},
				{policies: "1s:12h,10s:1d,1min:5d,5min:30d", cutoff: time.Hour * 14, cutover: day * 3},
				{policies: "10s:2d,1min:7d,5min:30d", cutoff: day * 3, cutover: day * 6},
				{policies: "10s:2d,5min:30d", cutoff: day * 6},
			},
			results: []expectedQuery{
				{"10s:1d", day, 0}, // NB(mmihic): Coalesced
				{"1min:5d", day * 3, day},
				{"1min:7d", day * 6, day * 3},
				{"5min:30d", day * 19, day * 6},
			},
		},

		// duplicate retention period
		{name: "duplicate retention period",
			from: day * 19, until: 0,
			rules: []rule{
				{policies: "10s:1d,1s:1d,1min:5d,5min:30d"},
			},
			results: []expectedQuery{
				{"1s:1d", day, 0},
				{"1min:5d", day * 5, day},
				{"5min:30d", day * 19, day * 5},
			},
		},
	}

	for _, test := range tests {
		rules := buildRules(t, now, test.rules)
		expectedPlan := buildExpectedQueries(t, now, test.results)
		from, until := now.Add(-test.from), now.Add(-test.until)
		actualPlan, err := planner.buildRetentionQueryPlan(from, until, rules)
		require.NoError(t, err, "error building plan for %s", test.name)

		for i := range actualPlan {
			if i == len(expectedPlan) {
				require.True(t, false, "actual plan contains more elements than expected %d vs %d",
					len(actualPlan), len(expectedPlan))
			}

			expected, actual := expectedPlan[i], actualPlan[i]
			require.Equal(t, expected.RetentionPolicy.String(), actual.RetentionPolicy.String(),
				"invalid retention policy for %s[%d]", test.name, i)
			require.Equal(t, now.Sub(expected.Start).String(), now.Sub(actual.Start).String(),
				"invalid from for %s[%d]", test.name, i)
			require.Equal(t, now.Sub(expected.End).String(), now.Sub(actual.End).String(),
				"invalid until for %s[%d]", test.name, i)
		}
	}
}

func buildRules(t *testing.T, now time.Time, rulesSpec []rule) []RetentionRule {
	rules := make([]RetentionRule, 0, len(rulesSpec))
	for _, spec := range rulesSpec {
		policies, err := ParseRetentionPolicies(spec.policies)
		require.NoError(t, err, "invalid policies %s", spec.policies)

		rule := NewRetentionRule().SetRetentionPolicies(policies)
		if spec.cutoff > 0 {
			rule.SetCutoffTime(now.Add(-spec.cutoff))
		}

		if spec.cutover > 0 {
			rule.SetCutoverTime(now.Add(-spec.cutover))
		}
		rules = append(rules, rule)
	}
	sort.Sort(RetentionRulesByCutoffTime(rules))
	return rules
}

func buildExpectedQueries(t *testing.T, now time.Time, expected []expectedQuery) []query {
	queries := make([]query, 0, len(expected))
	for _, q := range expected {
		policy, err := ParseRetentionPolicy(q.policy)
		require.NoError(t, err, "invalid policy %s", q.policy)

		queries = append(queries, query{
			Range: xtime.Range{
				Start: now.Add(-q.from),
				End:   now.Add(-q.until),
			},
			RetentionPolicy: policy,
		})
	}

	return queries
}

type rule struct {
	cutover, cutoff time.Duration
	policies        string
}

type expectedQuery struct {
	policy      string
	from, until time.Duration
}
