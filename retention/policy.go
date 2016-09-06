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
	"fmt"
	"sort"
	"strings"

	"github.com/m3db/m3x/time"
)

// A Policy describes the resolution and retention period for a set of
// datapoints (e.g. 1min at 30d)
type Policy interface {
	fmt.Stringer

	// Resolution is the resolution at which the datapoints will be stored
	Resolution() Resolution

	// Period is the amount of time to retain the datapoints
	Period() Period

	// Equal checks whether this retention policy is equal to another
	Equal(other Policy) bool
}

// PoliciesByPeriod is a sort.Interface for sorting a slice of
// Policy objects by the duration of their retention period, shortest
// retention period first
type PoliciesByPeriod []Policy

// Less compares two retention policies by their retention period.  Policies
// with identical retention periods are sub-sorted by resolution, finest resolution first
func (rr PoliciesByPeriod) Less(i, j int) bool {
	d1, d2 := rr[i].Period().Duration(), rr[j].Period().Duration()
	if d1 < d2 {
		return true
	}

	if d1 > d2 {
		return false
	}

	return rr[i].Resolution().WindowSize() < rr[j].Resolution().WindowSize()
}

// Swap swaps two retention policies in the slice
func (rr PoliciesByPeriod) Swap(i, j int) { rr[i], rr[j] = rr[j], rr[i] }

// Len returns the length of the retention policies slice
func (rr PoliciesByPeriod) Len() int { return len(rr) }

// NewPolicy creates a new Policy
func NewPolicy(r Resolution, p Period) Policy {
	return policy{
		s: fmt.Sprintf("%s:%s", r.String(), p.String()),
		r: r,
		p: p,
	}
}

// ParsePolicy parses a retention policy in the form of resolution:period
func ParsePolicy(s string) (Policy, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid retention policy %s, expect in form 10s:7d", s)
	}

	r, err := ParseResolution(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid retention policy %s, invalid resolution %s", s, parts[0])
	}

	pDuration, err := xtime.ParseExtendedDuration(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid retention policy %s, invalid retention period %s", s, parts[1])
	}
	p := NewPeriod(pDuration)

	return NewPolicy(r, p), nil
}

// MustParsePolicy parses a retention policy, panicking if the policy
// cannot be parsed
func MustParsePolicy(s string) Policy {
	policy, err := ParsePolicy(s)
	if err != nil {
		panic(err)
	}

	return policy
}

// ParsePolicies parses a list of retention policies in stringified form
func ParsePolicies(s string) ([]Policy, error) {
	policySpecs := strings.Split(s, ",")
	policies := make([]Policy, 0, len(policySpecs))
	for _, spec := range policySpecs {
		p, err := ParsePolicy(spec)
		if err != nil {
			return nil, err
		}

		policies = append(policies, p)
	}

	sort.Sort(PoliciesByPeriod(policies))
	return policies, nil
}

// MustParsePolicies parses a list of retention policies, panicking if
// the policies cannot be parsed
func MustParsePolicies(s string) []Policy {
	policies, err := ParsePolicies(s)
	if err != nil {
		panic(err)
	}

	return policies
}

type policy struct {
	s string
	r Resolution
	p Period
}

func (p policy) String() string         { return p.s }
func (p policy) Resolution() Resolution { return p.r }
func (p policy) Period() Period         { return p.p }
func (p policy) Equal(other Policy) bool {
	return p.r.Equal(other.Resolution()) && p.p.Equal(other.Period())
}
