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

// With returns a downsampler that uses the provided functions to combine new
// datapoints and new samples with the existing downsampled value
func With(f func(float64, float64) float64, fromSample func(Sample) float64) Downsampler {
	return &simpleDownsampler{f: f, fromSample: fromSample}
}

// Min returns a Downsampler that picks the minimum value as the sample
func Min() Downsampler {
	return With(NaNSafe(math.Min), func(s Sample) float64 { return s.Min })
}

// Max returns a Downsampler that picks the maximum value as the sample
func Max() Downsampler { return With(NaNSafe(math.Max), func(s Sample) float64 { return s.Max }) }

// Count returns a Downsampler that counts the number of samples in the interval
func Count() Downsampler { return new(countDownsampler) }

// Sum returns a Downsampler that adds the samples togher
func Sum() Downsampler {
	return With(
		NaNSafe(func(a, b float64) float64 { return a + b }),
		func(s Sample) float64 { return s.Sum })
}

// Mean returns a Downsampler that calculates the mean of all samples in the interval
func Mean() Downsampler { return new(meanDownsampler) }

type simpleDownsampler struct {
	f          func(float64, float64) float64
	fromSample func(Sample) float64
	vals       SeriesValues
}

func (d *simpleDownsampler) Reset(vals SeriesValues) { d.vals = vals }
func (d *simpleDownsampler) Finish()                 {}
func (d *simpleDownsampler) AddDatapoint(n int, v float64) {
	cur := d.vals.ValueAt(n)
	d.vals.SetValueAt(n, d.f(cur, v))
}
func (d *simpleDownsampler) AddSample(n int, s Sample) {
	d.AddDatapoint(n, d.fromSample(s))
}

type countDownsampler struct {
	vals SeriesValues
}

func (d *countDownsampler) Reset(vals SeriesValues) {
	d.vals = vals
}

func (d *countDownsampler) AddDatapoint(n int, v float64) {
	if math.IsNaN(v) {
		return
	}

	d.addCount(n, 1)
}

func (d *countDownsampler) AddSample(n int, s Sample) {
	d.addCount(n, s.Count)
}

func (d *countDownsampler) addCount(n int, c int) {
	cur := d.vals.ValueAt(n)
	if math.IsNaN(cur) {
		d.vals.SetValueAt(n, float64(c))
	} else {
		d.vals.SetValueAt(n, cur+float64(c))
	}
}

func (d *countDownsampler) Finish() {}

type meanDownsampler struct {
	vals   SeriesValues
	counts []int
}

func (d *meanDownsampler) Reset(vals SeriesValues) {
	// TODO(mmihic): Pool counts array
	d.vals = vals
	d.counts = make([]int, vals.Len())
}

func (d *meanDownsampler) AddDatapoint(n int, v float64) {
	if math.IsNaN(v) {
		return
	}

	d.addMean(n, v, 1)
}

func (d *meanDownsampler) AddSample(n int, s Sample) {
	d.addMean(n, s.Mean, s.Count)
}

func (d *meanDownsampler) addMean(n int, sampleMean float64, sampleCount int) {
	if sampleCount == 0 {
		return
	}

	count := d.counts[n]
	if count == 0 {
		d.vals.SetValueAt(n, sampleMean)
		d.counts[n] = sampleCount
		return
	}

	cur := d.vals.ValueAt(n)
	d.counts[n] += sampleCount
	d.vals.SetValueAt(n, (cur*float64(count)+sampleMean)/float64(count+1))
}

func (d *meanDownsampler) Finish() {}
