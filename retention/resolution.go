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
	"strings"
	"time"

	"github.com/m3db/m3x/time"
)

// A Resolution is a sample resolution for datapoints.
type Resolution interface {
	fmt.Stringer

	// WindowSize is the size of the bucket represented by the resolution
	WindowSize() time.Duration

	// Precision defines the precision of datapoints stored at this resolution
	Precision() xtime.Unit

	// AlignToStart aligns the given time to the start of the bucket containing that time
	AlignToStart(t time.Time) time.Time

	// WindowContaining returns the time range containing the given time at the resolution
	WindowContaining(t time.Time) xtime.Range

	// Equal compares this resolution to another
	Equal(other Resolution) bool
}

// NewResolution returns a new resolution
func NewResolution(windowSize time.Duration, precision xtime.Unit) Resolution {
	return resolution{
		s:          fmt.Sprintf("%s@1%s", windowSize.String(), precision.String()),
		windowSize: windowSize,
		precision:  precision,
	}
}

// ParseResolution parses a resolution string
func ParseResolution(s string) (Resolution, error) {
	precisionStart := strings.Index(s, "@")
	if precisionStart == -1 {
		windowSize, err := xtime.ParseExtendedDuration(s)
		if err != nil {
			return nil, err
		}

		return NewResolution(windowSize, xtime.Millisecond), nil
	}

	windowSize, err := xtime.ParseExtendedDuration(s[:precisionStart])
	if err != nil {
		return nil, err
	}

	dprecision, err := xtime.ParseExtendedDuration(s[precisionStart+1:])
	if err != nil {
		return nil, err
	}

	precision, err := xtime.UnitFromDuration(dprecision)
	if err != nil {
		return nil, err
	}

	return NewResolution(windowSize, precision), nil

}

type resolution struct {
	s          string
	windowSize time.Duration
	precision  xtime.Unit
}

func (r resolution) String() string                     { return r.s }
func (r resolution) WindowSize() time.Duration          { return r.windowSize }
func (r resolution) Precision() xtime.Unit              { return r.precision }
func (r resolution) AlignToStart(t time.Time) time.Time { return t.Truncate(r.windowSize) }
func (r resolution) WindowContaining(t time.Time) xtime.Range {
	start := t.Truncate(r.windowSize)
	return xtime.Range{
		Start: start,
		End:   start.Add(r.windowSize),
	}
}
func (r resolution) Equal(other Resolution) bool {
	return r.windowSize == other.WindowSize() && r.precision == other.Precision()
}
