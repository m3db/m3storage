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

// A Resolution is a named blah blah
type Resolution interface {
	Name() string                                           // the name of the resolution
	WindowSize() time.Duration                              // the size of the window represented by the resolution
	AlignToStart(t time.Time) time.Time                     // aligns the given time to the start
	TimeRangeContaining(t time.Time) (time.Time, time.Time) // returns the time range containing
	// the given time, aligned to the resolution
}

// NewResolution returns a new named resolution
func NewResolution(name string, windowSize time.Duration) Resolution {
	return resolution{
		name:       name,
		windowSize: windowSize,
	}
}

type resolution struct {
	name       string
	windowSize time.Duration
}

func (r resolution) Name() string                       { return r.name }
func (r resolution) WindowSize() time.Duration          { return r.windowSize }
func (r resolution) AlignToStart(t time.Time) time.Time { return t.Truncate(r.windowSize) }
func (r resolution) TimeRangeContaining(t time.Time) (time.Time, time.Time) {
	start := t.Truncate(r.windowSize)
	return start, start.Add(r.windowSize)
}
