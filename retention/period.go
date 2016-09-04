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
	"time"
)

// A Period is a named amount of time to retain a given metric
type Period interface {
	fmt.Stringer

	// Duration is the duration of the retention period
	Duration() time.Duration

	// Equal checks whether to retention periods are equal
	Equal(other Period) bool
}

// NewPeriod creates a new Period with the given name and duration
func NewPeriod(duration time.Duration) Period {
	return period{
		duration: duration,
	}
}

// PeriodsByDuration is a sort.Interface for sorting Periods by
// Duration, with the shortest duration first
type PeriodsByDuration []Period

// Less compares two retention periods by their duration time
func (rr PeriodsByDuration) Less(i, j int) bool {
	return rr[i].Duration() < rr[j].Duration()
}

// Swap swaps two retention periods in the slice
func (rr PeriodsByDuration) Swap(i, j int) { rr[i], rr[j] = rr[j], rr[i] }

// Len returns the length of the retention rule slice
func (rr PeriodsByDuration) Len() int { return len(rr) }

type period struct {
	duration time.Duration
}

func (r period) String() string          { return r.duration.String() }
func (r period) Duration() time.Duration { return r.duration }
func (r period) Equal(other Period) bool {
	return r.duration == other.Duration()
}
