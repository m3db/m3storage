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
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3storage/generated/proto/configtest"
	"github.com/m3db/m3storage/retention"
	"github.com/m3db/m3x/log"

	"github.com/facebookgo/clock"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestConnectionManager_GetConnection(t *testing.T) {
}

func TestConnectionManager_ConcurrentGetNewConnection(t *testing.T) {
}

func TestConnectionManager_GetConnectionWatchFails(t *testing.T) {
}

func TestConnectionManager_GetConnectionDriverFails(t *testing.T) {
}

func TestConnectionManager_GetConnectionClosedManager(t *testing.T) {
}

func TestConnectionManager_GetConnectionUnsupportedType(t *testing.T) {
}

func TestConnectionManager_GetConnectionManagerClosedWhileDriverWorking(t *testing.T) {
}

func TestConnectionManager_GetConnectionUnmarshalConfigError(t *testing.T) {
}

type simpleStorageType string

func (t simpleStorageType) Name() string { return string(t) }

const fakeStorageType simpleStorageType = "fake"

type fakeDriver struct {
	opens        uint32
	reconfigures uint32
}

func (d *fakeDriver) ConfigType() proto.Message { return &configtest.TestConfig{} }
func (d *fakeDriver) Type() Type                { return fakeStorageType }
func (d *fakeDriver) Close() error              { return nil }
func (d *fakeDriver) OpenConnection(cfg proto.Message) (Connection, error) {
	atomic.AddUint32(&d.opens, 1)
	return newFakeConnection(cfg.(*configtest.TestConfig))
}

func (d *fakeDriver) ReconfigureConnection(conn Connection, cfg proto.Message) (Connection, error) {
	atomic.AddUint32(&d.reconfigures, 1)
	return newFakeConnection(cfg.(*configtest.TestConfig))
}

func newFakeConnection(c *configtest.TestConfig) (Connection, error) {
	hosts := make(map[string]struct{}, len(c.Hosts))
	for _, h := range c.Hosts {
		hosts[h] = struct{}{}
	}

	return &fakeConnection{
		TestConfig: *c,
		hosts:      hosts,
	}, nil
}

type fakeConnection struct {
	configtest.TestConfig
	closed uint32
	hosts  map[string]struct{}
}

func (conn *fakeConnection) Read(id string, r retention.Resolution, start, end time.Time) (SeriesIter, error) {
	return &fakeSeriesIter{
		next: start.Truncate(r.WindowSize()),
		end:  end.Truncate(r.WindowSize()),
		v:    conn.TestConfig.BaseValue,
		incv: conn.TestConfig.IncValue,
		inct: time.Duration(conn.TestConfig.StepSize) * time.Second,
	}, nil
}

func (conn *fakeConnection) Write(id string, r retention.Resolution, t time.Time, v float64) error {
	return nil
}

func (conn *fakeConnection) Close() error {
	atomic.AddUint32(&conn.closed, 1)
	return nil
}

type fakeSeriesIter struct {
	next, end time.Time
	t         time.Time
	v         float64
	inct      time.Duration
	incv      float64
}

func (iter *fakeSeriesIter) Next() bool {
	if iter.next.After(iter.end) {
		return false
	}

	iter.t, iter.next = iter.next, iter.next.Add(iter.inct)
	iter.v = iter.v + iter.incv
	return true
}

func (iter *fakeSeriesIter) Current() (float64, time.Time) { return iter.v, iter.t }
func (iter *fakeSeriesIter) Close() error                  { return nil }
