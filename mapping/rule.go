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

package mapping

import (
	"time"
)

// A Rule defines which cluster holds the datapoints for a shard within
// a given timeframe
type Rule interface {
	// Cluster is the name of the cluster that is targeted by this mapping
	Cluster() string

	// Database is the name of the database that is targeted by this mapping
	Database() string

	// CutoffTime defines the time that reads from Cluster should stop.  Will
	// inherently fall after the WriteCutoverTime, to account for configuration
	// changes not arriving at all writers simultaneously
	CutoffTime() time.Time

	// ReadCutoverTime defines the time that reads to Cluster should begin
	ReadCutoverTime() time.Time

	// WriteCutoverTime defines the time that writes to Cluster should begin.
	// Will inherently fall after ReadCutoverTime, to account for configuration
	// changes reaching writers ahead of readers.
	WriteCutoverTime() time.Time
}
