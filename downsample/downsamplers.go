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

package downsample

import (
	"math"

	"github.com/m3db/m3storage"
)

// NaNSafe returns a variant of the provided function that can account for NaN
// values in either of the parameters
func NaNSafe(f func(a, b float64) float64) func(float64, float64) float64 {
	return func(a, b float64) float64 {
		if math.IsNaN(a) {
			return b
		}

		if math.IsNaN(b) {
			return a
		}

		return f(a, b)
	}
}

// With returns a downsampler that uses the provided function to
// combine the new sample with the existing downsampled value
func With(f func(float64, float64) float64) storage.Downsampler {
	return &simpleDownsampler{f: f}
}

// Min returns a Downsampler that picks the minimum value as the sample
func Min() storage.Downsampler { return With(NaNSafe(math.Min)) }

// Max returns a Downsampler that picks the maximum value as the sample
func Max() storage.Downsampler { return With(NaNSafe(math.Max)) }

// Count returns a Downsampler that counts the number of samples in the interval
func Count() storage.Downsampler {
	return With(func(a, b float64) float64 {
		if math.IsNaN(b) {
			return a
		}

		if math.IsNaN(a) {
			return 1
		}

		return a + 1
	})
}

// Sum returns a Downsampler that adds the samples togher
func Sum() storage.Downsampler {
	return With(NaNSafe(func(a, b float64) float64 { return a + b }))
}

// Mean returns a Downsampler that calculates the mean of all samples in the interval
func Mean() storage.Downsampler { return new(meanDownsampler) }

type simpleDownsampler struct {
	f    func(float64, float64) float64
	vals storage.SeriesValues
}

func (d *simpleDownsampler) Init(vals storage.SeriesValues) { d.vals = vals }
func (d *simpleDownsampler) Finish()                        {}
func (d *simpleDownsampler) AddSample(n int, v float64) {
	cur := d.vals.ValueAt(n)
	d.vals.SetValueAt(n, d.f(cur, v))
}

type meanDownsampler struct {
	vals   storage.SeriesValues
	counts []int
}

func (d *meanDownsampler) Init(vals storage.SeriesValues) {
	// TODO(mmihic): Pool counts array
	d.vals = vals
	d.counts = make([]int, vals.Len())
}

func (d *meanDownsampler) AddSample(n int, v float64) {
	if math.IsNaN(v) {
		return
	}

	count := d.counts[n]
	if count == 0 {
		d.vals.SetValueAt(n, v)
		d.counts[n] = 1
		return
	}

	cur := d.vals.ValueAt(n)
	d.counts[n]++
	d.vals.SetValueAt(n, (cur*float64(count)+v)/float64(count+1))
}

func (d *meanDownsampler) Finish() {}
