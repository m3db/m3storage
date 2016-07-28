package downsample

import (
	"math"

	"github.com/m3db/m3storage"
)

// NanSafe returns a variant of the provided function that can account for NaN
// values in either of the parameters
func NaNSafe(f func(a, b float64) float64) func(float64, float64) float64 {
	return func(a, b float64) float64 {
		if math.IsNaN(a) {
			return b
		}

		if math.IsNan(b) {
			return a
		}

		return f(a, b)
	}
}

// DownsampleWith returns a downsampler that uses the provided function to
// combine the new sample with the existing downsampled value
func DownsampleWith(f func(float64, float64) float64) Downsampler {
	return &simpleDownsampler{f: f}
}

// Min returns a Downsampler that picks the minimum value as the sample
func Min() Downsampler { return DownsampleWith(NaNSafe(math.Min)) }

// Max returns a Downsampler that picks the maximum value as the sample
func Max() Downsampler { return DownsampleWith(NaNSafe(math.Max)) }

// Count returns a Downsampler that counts the number of samples in the interval
func Count() Downsampler {
	return DownsampleWith(func(a, b float64) float64 {
		if math.IsNaN(a) {
			return 1
		}

		return a + 1
	})
}

// Mean returns a Downsampler that calculates the mean of all samples in the interval
func Mean() Downsampler { return new(meanDownsampler) }

type simpleDownsampler struct {
	f    func(float64, float64) float64
	vals storage.SeriesValues
}

func (d *simpleDownsampler) Init(vals storage.SeriesValues) { d.vals = vals }
func (d *simpleDownsampler) Finish()                        {}
func (d *simpleDownsampler) AddSample(n int, v float64) {
	cur := vals.ValueAt(n)
	vals.SetValueAt(n, d.f(cur, v))
}

type meanDownsampler struct {
	vals   storage.SeriesValues
	counts []int
}

func (d *meanDownsampler) Init(vals storage.SeriesValues) {
	// TODO(mmihic): Pool counts array
	d.vals, d.counts = vals, make([]int, vals.Len())
}

func (d *meanDownsampler) AddSample(n int, v float64) {

	count := d.counts[n]
	if count == 0 {
		d.vals.SetValueAt(n, v)
		d.counts[n] = 1
		return
	}

	d.counts[n] += 1
	d.vals.SetValueAt(n, (a*float64(count)+b)/float64(count+1))
}

func (d *meanDownsampler) Finish() {}
