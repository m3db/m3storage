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
	"time"
)

// A Rule defines the retention policies that apply to a set of
// metrics at a particular time.  Retention policy can change dynamically over
// time in response to user actions, so retention rules have a CutoverTime and
// CutoffTime which determine when the rules apply.  When determining which
// retention periods to query over, the storage manager determines which rule
// apply within the query time range, then builds a set of sub-queries covering
// each retention period
type Rule interface {
	// Policies are the retention policies at the time of the rule, ordered
	// by retention period (from shorted to longest)
	Policies() []Policy
	SetPolicies(p []Policy) Rule

	// CutoverTime is the time that the rule will begin to be applied
	CutoverTime() time.Time
	SetCutoverTime(t time.Time) Rule

	// CutoffTime is the time that the rule no longer applies
	CutoffTime() time.Time
	SetCutoffTime(t time.Time) Rule
}

// NewRule creates a new retention rule
func NewRule() Rule { return new(rule) }

// RulesByCutoffTime is a sort.Interface for sorting Rules by
// CutoffTime, with the latest cutoff time first
type RulesByCutoffTime []Rule

// Less compares two retention rules by their cutoff time
func (rr RulesByCutoffTime) Less(i, j int) bool {
	if rr[i].CutoffTime().IsZero() {
		return true
	}

	if rr[j].CutoffTime().IsZero() {
		return false
	}

	return rr[i].CutoffTime().After(rr[j].CutoffTime())
}

// Swap swaps two retention rules in the slice
func (rr RulesByCutoffTime) Swap(i, j int) { rr[i], rr[j] = rr[j], rr[i] }

// Len returns the length of the retention rule slice
func (rr RulesByCutoffTime) Len() int { return len(rr) }

// RuleProvider looks up retention rules for a given id and timerange
type RuleProvider interface {
	// FindRules returns the list of retention rules that apply
	// for the given id over the requested time range
	FindRules(id string, start, end time.Time) ([]Rule, error)
}

type rule struct {
	policies                []Policy
	cutoffTime, cutoverTime time.Time
}

func (r *rule) Policies() []Policy     { return r.policies }
func (r *rule) CutoverTime() time.Time { return r.cutoverTime }
func (r *rule) CutoffTime() time.Time  { return r.cutoffTime }

func (r *rule) SetPolicies(p []Policy) Rule {
	r.policies = p
	return r
}

func (r *rule) SetCutoverTime(t time.Time) Rule {
	r.cutoverTime = t
	return r
}

func (r *rule) SetCutoffTime(t time.Time) Rule {
	r.cutoffTime = t
	return r
}
