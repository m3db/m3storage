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

// A RetentionPeriod is a named amount of time to retain a given metric
type RetentionPeriod interface {
	// Name is the name of the retention period
	Name() string

	// Duration is the duration of the retention period
	Duration() time.Duration
}

// NewRetentionPeriod creates a new RetentionPeriod with the given name and duration
func NewRetentionPeriod(name string, duration time.Duration) RetentionPeriod {
	return retentionPeriod{
		name:     name,
		duration: duration,
	}
}

// RetentionPeriodsByDuration is a sort.Interface for sorting RetentionPeriods by
// Duration, with the shortest duration first
type RetentionPeriodsByDuration []RetentionPeriod

// Less compares two retention periods by their duration time
func (rr RetentionPeriodsByDuration) Less(i, j int) bool {
	return rr[i].Duration() < rr[j].Duration()
}

// Swap swaps two retention periods in the slice
func (rr RetentionPeriodsByDuration) Swap(i, j int) { rr[i], rr[j] = rr[j], rr[i] }

// Len returns the length of the retention rule slice
func (rr RetentionPeriodsByDuration) Len() int { return len(rr) }

type retentionPeriod struct {
	name     string
	duration time.Duration
}

func (r retentionPeriod) Name() string            { return r.name }
func (r retentionPeriod) Duration() time.Duration { return r.duration }
