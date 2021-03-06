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

	"github.com/m3db/m3storage/retention"
)

// The Manager is the main interface into the storage system, supporting
// queries against multiple storage clusters
type Manager interface {
	// Query reads datapoints for the id between two times
	Query(id string, start, end time.Time, ds Downsampler) (QueryResult, error)
}

// QueryResult is the result of doing a read
type QueryResult interface {
	// Resolution is the resolution of the returned series
	Resolution() retention.Resolution

	// Series contains the returned data
	Series() Series
}

// NewQueryResult returns a new QueryResult for the given resolution and series
func NewQueryResult(r retention.Resolution, s Series) QueryResult {
	return queryResult{r: r, s: s}
}

type queryResult struct {
	r retention.Resolution
	s Series
}

func (r queryResult) Resolution() retention.Resolution { return r.r }
func (r queryResult) Series() Series                   { return r.s }
