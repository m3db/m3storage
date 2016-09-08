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

package ts

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNaNSafe(t *testing.T) {
	f := NaNSafe(func(a, b float64) float64 { return a + b })
	tests := []struct {
		a, b, result float64
	}{
		{math.NaN(), math.NaN(), math.NaN()},
		{17.0, math.NaN(), 17.0},
		{math.NaN(), 23.0, 23.0},
		{27.5, 32.6, 60.1},
	}

	for n, test := range tests {
		result := f(test.a, test.b)
		if math.IsNaN(test.result) {
			assert.True(t, math.IsNaN(result), "expected NaN for %d, got %f", n, result)
		} else {
			assert.Equal(t, test.result, f(test.a, test.b), "incorrect result for test %d", n)
		}
	}
}

func TestSum(t *testing.T) {
	testDownsample(t, Sum(),
		[]float64{9, math.NaN(), 75.5, 5},
		[]float64{math.NaN(), 11, 18, 7},
		[]Sample{
			{Sum: 12},
			{Sum: math.NaN()},
			{Sum: 6},
			{Sum: 4},
		},

		[]float64{21, 11, 99.5, 16})

}

func TestMean(t *testing.T) {
	testDownsample(t, Mean(),
		[]float64{9, math.NaN(), 75.5, 5},
		[]float64{math.NaN(), 11, 18, 7},
		[]Sample{
			{Count: 0},
			{Count: 1, Mean: 12},
			{Count: 2, Mean: 4},
			{Count: 0},
		},

		[]float64{9, 11.5, 32.5, 6})
}

func TestMax(t *testing.T) {
	testDownsample(t, Max(),
		[]float64{9, math.NaN(), 75.5, 5},
		[]float64{math.NaN(), 11, 18, 7},
		[]Sample{
			{Max: 7},
			{Max: 13},
			{Max: math.NaN()},
			{Max: 4},
		},

		[]float64{9, 13, 75.5, 7})
}

func TestMin(t *testing.T) {
	testDownsample(t, Min(),
		[]float64{9, math.NaN(), 75.5, 5},
		[]float64{math.NaN(), 11, 18, 7},
		[]Sample{
			{Min: 7},
			{Min: 24},
			{Min: math.NaN()},
			{Min: 12},
		},

		[]float64{7, 11, 18, 5})
}

func TestCount(t *testing.T) {
	testDownsample(t, Count(),
		[]float64{9, math.NaN(), 75.5, 5},
		[]float64{math.NaN(), 11, 18, 7},
		[]Sample{
			{Count: 3},
			{Count: 0},
			{Count: 1},
			{Count: 9},
		},

		[]float64{4, 1, 3, 11})
}

func testDownsample(t *testing.T, d Downsampler, a, b []float64, samples []Sample, results []float64) {
	require.Equal(t, len(a), len(b))
	require.Equal(t, len(a), len(results))
	require.Equal(t, len(a), len(samples))

	vals := make(Float64SeriesValues, len(a))
	vals.Reset()

	d.Reset(vals)
	for n := range a {
		d.AddDatapoint(n, a[n])
		d.AddDatapoint(n, b[n])
		d.AddSample(n, samples[n])
	}

	d.Finish()
	for n := range vals {
		assert.Equal(t, results[n], vals[n], "wrong value at %d", n)
	}

}
