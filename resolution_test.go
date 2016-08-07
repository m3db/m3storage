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
	"testing"
	"time"

	"github.com/m3db/m3x/time"
	"github.com/stretchr/testify/require"
)

func TestParseResolution(t *testing.T) {
	tests := []struct {
		s string
		r Resolution
	}{
		{"10s@1s", NewResolution(time.Second*10, xtime.Second)},
		{"2d@1ns", NewResolution(time.Hour*24*2, xtime.Nanosecond)},
		{"1min", NewResolution(time.Minute, xtime.Millisecond)},
	}

	for _, test := range tests {
		r, err := ParseResolution(test.s)
		require.NoError(t, err, "received error parsing %s", test.s)
		require.Equal(t, test.r.WindowSize().String(), r.WindowSize().String(), "bad window size for %s", test.s)
		require.Equal(t, test.r.Precision(), r.Precision(), "incorrect precision for %s", test.s)
	}
}

func TestParseResolutionErrors(t *testing.T) {
	tests := []string{
		"10seconds@1s",
		"10s@2s",
		"10s@2minutes",
	}

	for _, test := range tests {
		_, err := ParseResolution(test)
		require.Error(t, err, "expected error for %s", test)
	}
}

func TestResolutionAlignToStart(t *testing.T) {
	now := time.Time{} // Start of epoch
	tests := []struct {
		r        Resolution
		toAlign  time.Time
		expected time.Time
	}{
		{NewResolution(time.Second*10, xtime.Millisecond),
			now.Add(time.Second * 12),
			now.Add(time.Second * 10)},
		{NewResolution(time.Second*10, xtime.Millisecond),
			now.Add(time.Minute*24 + time.Second*14),
			now.Add(time.Minute*24 + time.Second*10)},
		{NewResolution(time.Minute, xtime.Millisecond),
			now.Add(time.Minute * 12),
			now.Add(time.Minute * 12)},
		{NewResolution(time.Minute, xtime.Millisecond),
			now.Add(time.Minute*12 + time.Second*14),
			now.Add(time.Minute * 12)},
	}

	for _, test := range tests {
		actual := test.r.AlignToStart(test.toAlign)
		require.Equal(t, actual.String(), test.expected.String())
	}
}

func TestResolutionWindowContaining(t *testing.T) {
	now := time.Time{} // Start of epoch
	tests := []struct {
		r                          Resolution
		t                          time.Time
		expectedStart, expectedEnd time.Time
	}{
		{NewResolution(time.Second*10, xtime.Millisecond),
			now.Add(time.Second * 12),
			now.Add(time.Second * 10), now.Add(time.Second * 20)},
		{NewResolution(time.Second*10, xtime.Millisecond),
			now.Add(time.Minute*24 + time.Second*14),
			now.Add(time.Minute*24 + time.Second*10), now.Add(time.Minute*24 + time.Second*20)},
		{NewResolution(time.Minute, xtime.Millisecond),
			now.Add(time.Minute * 12),
			now.Add(time.Minute * 12), now.Add(time.Minute * 13)},
		{NewResolution(time.Minute, xtime.Millisecond),
			now.Add(time.Minute*12 + time.Second*14),
			now.Add(time.Minute * 12), now.Add(time.Minute * 13)},
	}

	for _, test := range tests {
		actual := test.r.WindowContaining(test.t)
		require.Equal(t, actual.Start.String(), test.expectedStart.String())
		require.Equal(t, actual.End.String(), test.expectedEnd.String())
	}

}
