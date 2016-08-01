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
)

// A RetentionPolicy describes the resolution and retention period for a set of
// datapoints (e.g. 1min at 30d)
type RetentionPolicy interface {
	// Resolution is the resolution at which the datapoints will be stored
	Resolution() Resolution

	// RetentionPeriod is the amount of time to retain the datapoints
	RetentionPeriod() RetentionPeriod
}

// NewRetentionPolicy creates a new RetentionPolicy
func NewRetentionPolicy(r Resolution, p RetentionPeriod) RetentionPolicy {
	return retentionPolicy{r: r, p: p}
}

// A RetentionRule maps a set of metrics onto a RetentionPolicy.  Retention
// policy can change dynamically over time in response to user actions, so
// retention rules have a CutoverTime and CutoffTime which determine when the
// rules apply.  When determining which retention periods to query over, the
// storage manager determines which rule apply within the query time range,
// then builds a set of sub-queries covering each retention period
type RetentionRule interface {
	// RetentionPolicy is the retention policy of the rule
	RetentionPolicy() RetentionPolicy
	SetRetentionPolicy(p RetentionPolicy) RetentionRule

	// CutoverTime is the time that the rule will begin to be applied
	CutoverTime() time.Time
	SetCutoverTime(t time.Time) RetentionRule

	// CutoffTime is the time that the rule no longer applies
	CutoffTime() time.Time
	SetCutoffTime(t time.Time) RetentionRule
}

// RetentionRuleProvider looks up retention rules for a given id and timerange
type RetentionRuleProvider interface {
	// FindRetentionRules returns the list of retention rules that apply
	// for the given id over the requested time range
	FindRetentionRules(id string, start, end time.Time) ([]RetentionRule, error)
}

// NewRetentionRule creates a new retention rule
func NewRetentionRule() RetentionRule { return new(retentionRule) }

type retentionPolicy struct {
	r Resolution
	p RetentionPeriod
}

func (p retentionPolicy) Resolution() Resolution           { return p.r }
func (p retentionPolicy) RetentionPeriod() RetentionPeriod { return p.p }

type retentionRule struct {
	policy                  RetentionPolicy
	cutoffTime, cutoverTime time.Time
}

func (r *retentionRule) RetentionPolicy() RetentionPolicy { return r.policy }
func (r *retentionRule) CutoverTime() time.Time           { return r.cutoverTime }
func (r *retentionRule) CutoffTime() time.Time            { return r.cutoffTime }

func (r *retentionRule) SetRetentionPolicy(p RetentionPolicy) RetentionRule {
	r.policy = p
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
