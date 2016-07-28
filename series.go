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
	"math"
	"time"

	"github.com/m3db/m3x/close"
)

// A Series is a series of datapoints, bucketed into fixed time intervals
type Series interface {
	xclose.Closer

	Len() int                    // Len returns the number of datapoints in the series
	StepSize() time.Duration     // StepSize is the size of each "step" in the series
	StartTime() time.Time        // StartTime is the start time for the series
	EndTime() time.Time          // EndTime is the end time for the series
	ValueAt(n int) float64       // ValueAt returns the value at the given step
	StartTimeAt(n int) time.Time // StartTimeAt returns the start time for the given step
}

type series struct {
	values    SeriesValues
	stepSize  time.Duration
	startTime time.Time
	vp        SeriesValuesPool
}

func (s series) Len() int                    { return s.values.Len() }
func (s series) StepSize() time.Duration     { return s.stepSize }
func (s series) StartTime() time.Time        { return s.startTime }
func (s series) EndTime() time.Time          { return s.startTime.Add(time.Duration(s.Len()) * s.stepSize) }
func (s series) ValueAt(n int) float64       { return s.values.ValueAt(n) }
func (s series) StartTimeAt(n int) time.Time { return s.startTime.Add(time.Duration(n) * s.stepSize) }
func (s series) Close() error {
	if s.vp != nil {
		s.vp.Release(s.values)
	}

	return nil
}

// SeriesValues hold the underlying values in a series.  Values can often be large, and
// benefit from direct pooling.
type SeriesValues interface {
	Len() int                    // the number of values in the series
	SetValueAt(n int, v float64) // sets the value at the given location
	ValueAt(n int) float64       // returns the value at the given location
}

// Float64SeriesValues is a SeriesValues implementation based on a float64 slice
type Float64SeriesValues []float64

// Len returns the number of values in the series
func (s Float64SeriesValues) Len() int { return len(s) }

// SetValueAt sets the value at the given position
func (s Float64SeriesValues) SetValueAt(n int, v float64) { s[n] = v }

// ValueAt returns the value at the given position
func (s Float64SeriesValues) ValueAt(n int) float64 { return s[n] }

// Reset fills the series with NaNs
func (s Float64SeriesValues) Reset() {
	for n := range s {
		s[n] = math.NaN()
	}
}

// A SeriesValuesPool allows for pooling of the underlying SeriesValues, which
// are typically large and benefit from being held in some form of pool
type SeriesValuesPool interface {
	// New creates or reserves series values of the specified length
	New(len int) SeriesValues

	// Release releases previously allocated values
	Release(v SeriesValues)
}
