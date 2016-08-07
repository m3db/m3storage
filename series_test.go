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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSeries(t *testing.T) {
	now := time.Now().Truncate(time.Minute)
	vals := Float64SeriesValues([]float64{100, math.NaN(), 24.5, 6, 92, 34.62})
	series := NewSeries(now, time.Second*10, vals, nil)

	require.Equal(t, 6, series.Len())
	require.Equal(t, now.String(), series.StartTime().String())
	require.Equal(t, now.Add(time.Minute).String(), series.EndTime().String())
	require.Equal(t, time.Second*10, series.StepSize())
	require.Equal(t, now.Add(time.Second*20), series.StartTimeAt(2))
	require.Equal(t, 34.62, series.ValueAt(5))

	series.Close()
}
