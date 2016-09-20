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

package conn

import (
	"testing"
	"time"

	"github.com/m3db/m3storage/cluster"
	"github.com/m3db/m3storage/generated/proto/configtest"
	"github.com/m3db/m3storage/retention"
	"github.com/m3db/m3x/time"

	"github.com/facebookgo/clock"
	"github.com/stretchr/testify/require"
)

func TestFakeDriver(t *testing.T) {
	clock := clock.NewMock()
	clock.Add(time.Hour)

	var (
		r           = retention.NewResolution(time.Second, xtime.Millisecond)
		testStart   = clock.Now().Truncate(time.Minute)
		clusterType = cluster.NewType("fake")
		d           = NewFakeDriver(clusterType, time.Second*10, nil)
		c, _        = d.Open(configtest.NewTestConfig("h1"))
	)

	// Confirm basics
	require.Equal(t, clusterType, d.Type())
	cfgType := d.ConfigType()
	_, ok := cfgType.(*configtest.TestConfig)
	require.True(t, ok)

	// No writes == no datapoints
	iter, err := c.Read("foo", r, testStart, testStart.Add(time.Hour))
	require.NoError(t, err)
	require.False(t, iter.Next())

	// No series == no datapoints
	require.NoError(t, c.Write("bar", r, testStart.Add(-time.Minute), 200))
	iter, err = c.Read("foo", r, testStart, testStart.Add(time.Hour))
	require.NoError(t, err)
	require.False(t, iter.Next())

	// Fill in a series, in reverse order.  Will be sorted on retrieval
	for i := 0; i <= 120; i++ {
		writeTime := testStart.Add(time.Minute * time.Duration(i))
		require.NoError(t, c.Write("foo", r, writeTime, float64(i)))
	}

	tests := []struct {
		scenario          string
		start, end        time.Duration
		expectLen         int
		expectFirstOffset time.Duration
		expectFirstValue  float64
	}{
		{"middle - exactly aligned with timestamps",
			time.Minute * 4, time.Minute * 12,
			8, time.Minute * 4, 4},
		{"middle - slighly misaligned with timestamps",
			time.Minute*4 - time.Second*10, time.Minute*12 + time.Second*30,
			9, time.Minute * 4, 4},
		{"start before start",
			time.Second * -1, time.Minute * 3,
			3, 0, 0},
		{"end after end",
			time.Second, time.Hour * 24,
			120, time.Minute, 1},
		{"start after end",
			time.Hour * 24, time.Hour * 58,
			0, 0, 0},
		{"end before start",
			time.Minute * -30, time.Minute * -15,
			0, 0, 0},
	}

	for _, test := range tests {
		iter, err := c.Read("foo", r, testStart.Add(test.start), testStart.Add(test.end))
		require.NoError(t, err, "unexpected error for %s", test.scenario)
		require.NotNil(t, iter, "nil iter for %s", test.scenario)

		for i := 0; i < test.expectLen; i++ {
			require.True(t, iter.Next(), "too few datapoints for %s: %d", test.scenario, i)

			var (
				actualV, actualT = iter.Current()
				expectT          = testStart.Add(test.expectFirstOffset + time.Duration(i)*time.Minute)
				expectV          = test.expectFirstValue + float64(i)
			)

			require.Equal(t, expectT.String(), actualT.String(), "wrong ts for %s: %d", test.scenario, i)
			require.Equal(t, expectV, actualV, "wrong val for %s: %d", test.scenario, i)
		}

		require.False(t, iter.Next(), "too many datapoints for %s", test.scenario)
	}

	// Reconfigure with different host
	c, _ = d.Reconfigure(c, configtest.NewTestConfig("h2"))

	// Should not be able to find series due to pointing to a different host
	iter, err = c.Read("foo", r, testStart, testStart.Add(time.Hour*48))
	require.NoError(t, err)
	require.False(t, iter.Next())

	// Reconfigure back to the original host, should be able to find the series again
	c, _ = d.Reconfigure(c, configtest.NewTestConfig("h1"))
	_, err = c.Read("foo", r, testStart, testStart.Add(time.Hour*24))
	require.Nil(t, err)

	// Try to create a new connection with multiple hosts - should fail
	c2, err := d.Open(configtest.NewTestConfig("h1", "h2", "h3"))
	require.Equal(t, errFakeOneHostOnly, err)
	require.Nil(t, c2)

	// Close the driver and confirm the connections are unusable
	require.NoError(t, d.Close())
	require.Equal(t, errFakeClosed, c.Write("foo", r, testStart.Add(time.Hour*24), -38))

	iter, err = c.Read("foo", r, testStart, testStart.Add(time.Hour*48))
	require.Equal(t, errFakeClosed, err)
	require.Nil(t, iter)

}
