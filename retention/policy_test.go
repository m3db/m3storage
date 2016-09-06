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
	"testing"
	"time"

	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePolicy(t *testing.T) {
	p, err := ParsePolicy("10s:2d")
	require.NoError(t, err)
	assert.Equal(t, (time.Second * 10).String(), p.Resolution().WindowSize().String())
	assert.Equal(t, xtime.Millisecond, p.Resolution().Precision())
	assert.Equal(t, (time.Hour * 24 * 2).String(), p.Period().Duration().String())

	p, err = ParsePolicy("1min@1s:30d")
	require.NoError(t, err)
	assert.Equal(t, (time.Minute).String(), p.Resolution().WindowSize().String())
	assert.Equal(t, xtime.Second, p.Resolution().Precision())
	assert.Equal(t, (time.Hour * 24 * 30).String(), p.Period().Duration().String())

	p, err = ParsePolicy("10monkeys:2d")
	require.Error(t, err)
	assert.Nil(t, p)

	p, err = ParsePolicy("10s:10monkeys")
	require.Error(t, err)
	assert.Nil(t, p)

	p, err = ParsePolicy("10s")
	require.Error(t, err)
	assert.Nil(t, p)
}

func TestParsePolicies(t *testing.T) {
	// NB(mmihic): These are out of order so will need to be sorted
	policies, err := ParsePolicies("5s:2d,10s:2d,1min:7d,500ms:6h,5min:14d")
	require.NoError(t, err)

	for n, expected := range []struct {
		resolution time.Duration
		retention  time.Duration
	}{
		{time.Millisecond * 500, time.Hour * 6},
		{time.Second * 5, time.Hour * 24 * 2},
		{time.Second * 10, time.Hour * 24 * 2},
		{time.Minute, time.Hour * 24 * 7},
		{time.Minute * 5, time.Hour * 24 * 14},
	} {
		require.Equal(t, expected.resolution.String(), policies[n].Resolution().WindowSize().String())
		require.Equal(t, expected.retention.String(), policies[n].Period().Duration().String())
	}
}
