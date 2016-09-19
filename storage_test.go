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

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3storage/cluster"
	"github.com/m3db/m3storage/conn"
	"github.com/m3db/m3storage/ts"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/sync"

	"github.com/facebookgo/clock"
	"github.com/stretchr/testify/require"
)

func TestNewManagerError(t *testing.T) {
	var (
		st = cluster.NewType("my-storage")
		d  = conn.NewFakeDriver(st, time.Second)
	)

	tests := []struct {
		err  error
		opts ManagerOptions
	}{
		{errManagerOptionsKVStoreRequired, NewManagerOptions().
			PlacementKey("foo").
			IDSharding(Murmur3Sharding).
			Clock(clock.NewMock()).
			ValuesPool(ts.DirectAllocValuesPool).
			WorkerPool(xsync.NewWorkerPool(5)).
			Drivers(d).
			Logger(xlog.NullLogger)},

		{errManagerOptionsPlacementKeyRequired, NewManagerOptions().
			KV(kv.NewFakeStore()).
			IDSharding(Murmur3Sharding).
			Clock(clock.NewMock()).
			ValuesPool(ts.DirectAllocValuesPool).
			WorkerPool(xsync.NewWorkerPool(5)).
			Drivers(d).
			Logger(xlog.NullLogger)},

		{errManagerOptionsLoggerRequired, NewManagerOptions().
			KV(kv.NewFakeStore()).
			PlacementKey("foo").
			IDSharding(Murmur3Sharding).
			Clock(clock.NewMock()).
			ValuesPool(ts.DirectAllocValuesPool).
			WorkerPool(xsync.NewWorkerPool(5)).
			Drivers(d).
			Logger(nil)},

		{errManagerOptionsClockRequired, NewManagerOptions().
			KV(kv.NewFakeStore()).
			PlacementKey("foo").
			IDSharding(Murmur3Sharding).
			Clock(nil).
			ValuesPool(ts.DirectAllocValuesPool).
			WorkerPool(xsync.NewWorkerPool(5)).
			Drivers(d).
			Logger(xlog.NullLogger)},

		{errManagerOptionsIDShardingRequired, NewManagerOptions().
			KV(kv.NewFakeStore()).
			PlacementKey("foo").
			Clock(clock.NewMock()).
			ValuesPool(ts.DirectAllocValuesPool).
			WorkerPool(xsync.NewWorkerPool(5)).
			IDSharding(nil).
			Drivers(d).
			Logger(xlog.NullLogger)},

		{errManagerOptionsWorkerPoolRequired, NewManagerOptions().
			KV(kv.NewFakeStore()).
			PlacementKey("foo").
			IDSharding(Murmur3Sharding).
			Clock(clock.NewMock()).
			ValuesPool(ts.DirectAllocValuesPool).
			Drivers(d).
			Logger(xlog.NullLogger)},

		{errManagerOptionsValuesPoolRequired, NewManagerOptions().
			KV(kv.NewFakeStore()).
			PlacementKey("foo").
			IDSharding(Murmur3Sharding).
			Clock(clock.NewMock()).
			WorkerPool(xsync.NewWorkerPool(5)).
			Drivers(d).
			Logger(xlog.NullLogger)},

		{errManagerOptionsDriversRequired, NewManagerOptions().
			KV(kv.NewFakeStore()).
			PlacementKey("foo").
			IDSharding(Murmur3Sharding).
			Clock(clock.NewMock()).
			ValuesPool(ts.DirectAllocValuesPool).
			WorkerPool(xsync.NewWorkerPool(5)).
			Logger(xlog.NullLogger)},
	}

	for _, test := range tests {
		mgr, err := NewManager(test.opts)
		require.Equal(t, test.err, err)
		require.Nil(t, mgr)
	}
}
