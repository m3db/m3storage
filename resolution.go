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

	"github.com/m3db/m3x/time"
)

// A Resolution is a named bucket size
type Resolution interface {
	// Name is the name of the Resolution
	Name() string

	// WindowSize is the size of the bucket represented by the resolution
	WindowSize() time.Duration

	// Precision defines the precision of datapoints stored at this resolution
	Precision() xtime.Unit

	// AlignToStart aligns the given time to the start of the bucket containing that time
	AlignToStart(t time.Time) time.Time

	// TimeRangeContaining returns the time range containing the given time at the resolution
	TimeRangeContaining(t time.Time) (time.Time, time.Time)

	// Equal compares this resolution to another
	Equal(other Resolution) bool
}

// NewResolution returns a new named resolution
func NewResolution(name string, windowSize time.Duration, precision xtime.Unit) Resolution {
	return resolution{
		name:       name,
		windowSize: windowSize,
		precision:  precision,
	}
}

type resolution struct {
	name       string
	windowSize time.Duration
	precision  xtime.Unit
}

func (r resolution) Name() string                       { return r.name }
func (r resolution) WindowSize() time.Duration          { return r.windowSize }
func (r resolution) Precision() xtime.Unit              { return r.precision }
func (r resolution) AlignToStart(t time.Time) time.Time { return t.Truncate(r.windowSize) }
func (r resolution) TimeRangeContaining(t time.Time) (time.Time, time.Time) {
	start := t.Truncate(r.windowSize)
	return start, start.Add(r.windowSize)
}
func (r resolution) Equal(other Resolution) bool {
	return r.windowSize == other.WindowSize() && r.precision == other.Precision()
}
