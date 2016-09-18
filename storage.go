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
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3storage/cluster"
	"github.com/m3db/m3storage/conn"
	"github.com/m3db/m3storage/mapping"
	"github.com/m3db/m3storage/retention"
	"github.com/m3db/m3storage/ts"
	"github.com/m3db/m3x/close"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/sync"

	"github.com/facebookgo/clock"
	"github.com/spaolacci/murmur3"
)

var (
	errManagerOptionsKVStoreRequired      = errors.New("kv store required")
	errManagerOptionsPlacementKeyRequired = errors.New("placement config key required")
	errManagerOptionsDriversRequired      = errors.New("drivers required")
	errManagerOptionsLoggerRequired       = errors.New("logger required")
	errManagerOptionsClockRequired        = errors.New("clock required")
	errManagerOptionsIDShardingRequired   = errors.New("sharding required")
	errManagerOptionsWorkerPoolRequired   = errors.New("worker pool required")
	errManagerOptionsValuesPoolRequired   = errors.New("values pool required")
)

// IDSharding maps an id to a virtual shard
type IDSharding interface {
	// Shard maps the id to a virtual shard
	Shard(id string) uint32
}

// IDShardingFunc is a stateless function that can act as an IDSharding
type IDShardingFunc func(string) uint32

// Shard maps the id to a virtual shard
func (f IDShardingFunc) Shard(id string) uint32 { return f(id) }

// Murmur3Sharding is the default sharding function
var Murmur3Sharding = IDShardingFunc(func(id string) uint32 {
	return murmur3.Sum32([]byte(id))
})

// ManagerOptions are options for creating a manager
type ManagerOptions interface {
	KV(kv kv.Store) ManagerOptions
	GetKV() kv.Store

	PlacementKey(key string) ManagerOptions
	GetPlacementKey() string

	Logger(log xlog.Logger) ManagerOptions
	GetLogger() xlog.Logger

	Clock(clock clock.Clock) ManagerOptions
	GetClock() clock.Clock

	ValuesPool(vals ts.SeriesValuesPool) ManagerOptions
	GetValuesPool() ts.SeriesValuesPool

	IDSharding(sharding IDSharding) ManagerOptions
	GetIDSharding() IDSharding

	WorkerPool(workers xsync.WorkerPool) ManagerOptions
	GetWorkerPool() xsync.WorkerPool

	Drivers(d []conn.Driver) ManagerOptions
	GetDrivers() []conn.Driver

	Validate() error
}

// NewManagerOptions creates a new default set of manager options
func NewManagerOptions() ManagerOptions {
	return managerOptions{
		logger:   xlog.NullLogger,
		clock:    clock.New(),
		sharding: Murmur3Sharding,
	}
}

// FetchResult is the result of doing a read
type FetchResult interface {
	// Resolution is the resolution of the returned series
	Resolution() retention.Resolution

	// Series contains the returned data
	Series() ts.Series
}

// NewFetchResult returns a new FetchResult for the given resolution and series
func NewFetchResult(r retention.Resolution, s ts.Series) FetchResult {
	return fetchResult{r: r, s: s}
}

// The Manager is the main interface into the storage system, supporting
// queries against multiple storage clusters
type Manager interface {
	xclose.Closer

	// Fetch reads datapoints for the id between two times
	Fetch(id string, r retention.Resolution, start, end time.Time, ds ts.Downsampler) (FetchResult, error)
}

type manager struct {
	log      xlog.Logger
	vals     ts.SeriesValuesPool
	cp       cluster.Provider
	mp       mapping.Provider
	cm       conn.Manager
	sharding IDSharding
	workers  xsync.WorkerPool
	qp       *queryPlanner
}

// NewManager creates a new manager
func NewManager(opts ManagerOptions) (Manager, error) {
	if opts == nil {
		opts = NewManagerOptions()
	}

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	clusterUpdateCh := make(chan cluster.Cluster, 5) // TODO(mmihic): control buffer size
	mp, err := mapping.NewProvider(opts.GetPlacementKey(), opts.GetKV(), mapping.NewProviderOptions().
		Logger(opts.GetLogger()).
		Clock(opts.GetClock()).
		ClusterUpdateCh(clusterUpdateCh))
	if err != nil {
		return nil, err
	}

	cp, err := cluster.NewProvider(clusterUpdateCh, cluster.NewProviderOptions().
		Logger(opts.GetLogger()))
	if err != nil {
		return nil, err
	}

	// TODO(mmihic): Set watch timeout
	cm, err := conn.NewManager(conn.NewManagerOptions().
		Drivers(opts.GetDrivers()).
		Logger(opts.GetLogger()).
		Provider(cp))
	if err != nil {
		return nil, err
	}

	return &manager{
		log:      opts.GetLogger(),
		vals:     opts.GetValuesPool(),
		cp:       cp,
		mp:       mp,
		cm:       cm,
		sharding: opts.GetIDSharding(),
		workers:  opts.GetWorkerPool(),
		qp:       newQueryPlanner(mp, opts.GetClock(), opts.GetLogger()),
	}, nil
}

// Fetch fetches the results for the given id and time range
func (mgr *manager) Fetch(
	id string, r retention.Resolution,
	start, end time.Time, ds ts.Downsampler) (FetchResult, error) {

	// Figure out which queries to execute
	shard := mgr.sharding.Shard(id)
	queries, err := mgr.qp.plan(shard, start, end)
	if err != nil {
		return nil, err
	}

	// Run queries in the background
	// TODO(mmihic): Consider whether to specify a timeout for the goroutine
	var (
		errors = make([]error, len(queries))
		iters  = make([]ts.SeriesIter, len(queries))
	)

	var wg sync.WaitGroup
	for n, q := range queries {
		n, q := n, q
		wg.Add(1)
		mgr.workers.Go(func() {
			defer wg.Done()

			iter, err := mgr.execFetch(id, r, q)
			if err != nil {
				errors[n] = err
				return
			}

			iters[n] = iter
		})
	}

	wg.Wait()

	var merr xerrors.MultiError
	for _, err := range errors {
		if err != nil {
			merr.Add(err)
		}
	}

	if !merr.Empty() {
		return nil, merr
	}

	return mgr.mergeResults(start, end, r, ds, iters)
}

// execFetch executes a fetch query
func (mgr *manager) execFetch(id string, r retention.Resolution, q query) (ts.SeriesIter, error) {
	conn, err := mgr.cm.GetConn(q.database, q.cluster)
	if err != nil {
		return nil, err
	}

	return conn.Read(id, r, q.Range.Start, q.Range.End)
}

// mergeSeries merges the results of multiple fetches
func (mgr *manager) mergeResults(start, end time.Time,
	r retention.Resolution, ds ts.Downsampler, iters []ts.SeriesIter) (FetchResult, error) {
	var (
		finalResolution = r
		finalStepSize   = r.WindowSize()
	)
	for _, iter := range iters {
		r := iter.CoarsestResolution()
		if r.WindowSize() > finalStepSize {
			finalResolution, finalStepSize = r, r.WindowSize()
		}
	}

	var (
		seriesStart, seriesEnd = start.Truncate(finalStepSize), end.Truncate(finalStepSize).Add(finalStepSize)
		numSteps               = int(seriesEnd.Sub(seriesStart) / finalStepSize) // TODO(mmihic): Round up
		vals                   = mgr.vals.New(numSteps)
	)

	// Fill in the values...
	ds.Reset(vals)
	for _, iter := range iters {
		for iter.Next() {
			// TODO(mmihic): Calculate which step this falls into
		}
		iter.Close()
	}
	ds.Finish()

	// ...and turn it into a series
	series := ts.NewSeries(seriesStart, finalStepSize, vals, mgr.vals)
	return NewFetchResult(finalResolution, series), nil
}

// Close closes the manager
func (mgr *manager) Close() error {
	if err := mgr.mp.Close(); err != nil {
		return err
	}

	if err := mgr.cm.Close(); err != nil {
		return err
	}

	if err := mgr.cp.Close(); err != nil {
		return err
	}

	return nil
}

type fetchResult struct {
	r retention.Resolution
	s ts.Series
}

func (r fetchResult) Resolution() retention.Resolution { return r.r }
func (r fetchResult) Series() ts.Series                { return r.s }

type managerOptions struct {
	logger   xlog.Logger
	workers  xsync.WorkerPool
	vals     ts.SeriesValuesPool
	sharding IDSharding
	clock    clock.Clock
	kv       kv.Store
	d        []conn.Driver
	key      string
}

func (opts managerOptions) GetIDSharding() IDSharding          { return opts.sharding }
func (opts managerOptions) GetLogger() xlog.Logger             { return opts.logger }
func (opts managerOptions) GetWorkerPool() xsync.WorkerPool    { return opts.workers }
func (opts managerOptions) GetClock() clock.Clock              { return opts.clock }
func (opts managerOptions) GetValuesPool() ts.SeriesValuesPool { return opts.vals }
func (opts managerOptions) GetKV() kv.Store                    { return opts.kv }
func (opts managerOptions) GetPlacementKey() string            { return opts.key }
func (opts managerOptions) GetDrivers() []conn.Driver          { return opts.d }

func (opts managerOptions) IDSharding(sharding IDSharding) ManagerOptions {
	opts.sharding = sharding
	return opts
}

func (opts managerOptions) Clock(clock clock.Clock) ManagerOptions {
	opts.clock = clock
	return opts
}

func (opts managerOptions) WorkerPool(workers xsync.WorkerPool) ManagerOptions {
	opts.workers = workers
	return opts
}

func (opts managerOptions) Logger(logger xlog.Logger) ManagerOptions {
	opts.logger = logger
	return opts
}

func (opts managerOptions) Drivers(d []conn.Driver) ManagerOptions {
	opts.d = d
	return opts
}

func (opts managerOptions) KV(kv kv.Store) ManagerOptions {
	opts.kv = kv
	return opts
}

func (opts managerOptions) PlacementKey(key string) ManagerOptions {
	opts.key = key
	return opts
}

func (opts managerOptions) ValuesPool(vals ts.SeriesValuesPool) ManagerOptions {
	opts.vals = vals
	return opts
}

func (opts managerOptions) Validate() error {
	if opts.logger == nil {
		return errManagerOptionsLoggerRequired
	}

	if opts.clock == nil {
		return errManagerOptionsClockRequired
	}

	if opts.sharding == nil {
		return errManagerOptionsIDShardingRequired
	}

	if opts.workers == nil {
		return errManagerOptionsWorkerPoolRequired
	}

	if opts.vals == nil {
		return errManagerOptionsValuesPoolRequired
	}

	if opts.key == "" {
		return errManagerOptionsPlacementKeyRequired
	}

	if opts.kv == nil {
		return errManagerOptionsKVStoreRequired
	}

	if len(opts.d) == 0 {
		return errManagerOptionsDriversRequired
	}

	return nil
}
