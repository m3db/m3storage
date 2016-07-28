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

// The StorageManager is the main interface into the storage system, supporting
// reads and writes against multiple storage clusters
type StorageManager interface {
	// Read reads datapoints for the id between two times
	Read(id string, start, end time.Time, ds Downsampler) (ReadResult, error)

	// Write writes a datapoint for the id
	Write(id string, t time.Time, v float64) error
}

// A Downsampler combines multiple datapoints that appear within the
// same time interval to produce a single downsampled result
type Downsampler interface {
	// Init initializes the downsampler to store results in the given values
	Init(vals SeriesValues)

	// AddSample adds a datapoint sample to the given interval
	AddSample(n int, v float64)

	// Finish tells the downsampler we're complete and the final values
	// computed (if they are not already)
	Finish()
}

// ReadResult is the result of doing a read
type ReadResult interface {
	// Resolution is the resolution of the returned series
	Resolution() Resolution

	// Series contains the returned data
	Series() Series
}

// NewReadResult returns a new ReadResult for the given resolution and series
func NewReadResult(r Resolution, s Series) ReadResult {
	return readResult{
		r: r,
		s: s,
	}
}

type readResult struct {
	r Resolution
	s Series
}

func (r readResult) Resolution() Resolution { return r.r }
func (r readResult) Series() Series         { return r.s }
