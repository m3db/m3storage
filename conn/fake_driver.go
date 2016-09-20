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
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/m3db/m3storage/cluster"
	"github.com/m3db/m3storage/generated/proto/configtest"
	"github.com/m3db/m3storage/retention"
	"github.com/m3db/m3storage/ts"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"

	"github.com/golang/protobuf/proto"
)

var (
	errFakeOneHostOnly = errors.New("one host only")
	errFakeClosed      = errors.New("driver closed")
)

// FakeDriverOptions are options to the fake driver
type FakeDriverOptions struct {
	Logger xlog.Logger
}

// NewFakeDriver returns a new fake Driver
func NewFakeDriver(clusterType cluster.Type, windowSize time.Duration, opts *FakeDriverOptions) Driver {
	if opts == nil {
		opts = new(FakeDriverOptions)
	}

	log := opts.Logger
	if log == nil {
		log = xlog.NullLogger
	}

	return &fakeDriver{
		clusterType: clusterType,
		r:           retention.NewResolution(windowSize, xtime.Millisecond),
		hosts:       make(map[string]map[string][]datapoint),
		log:         log,
	}
}

type fakeDriver struct {
	sync.RWMutex
	clusterType cluster.Type
	r           retention.Resolution
	hosts       map[string]map[string][]datapoint
	closed      bool
	log         xlog.Logger
}

func (d *fakeDriver) read(h, id string, start, end time.Time) (ts.SeriesIter, error) {
	d.log.Debugf("read %s from %s (%s to %s)", id, h, start.String(), end.String())

	d.RLock()
	defer d.RUnlock()

	if d.closed {
		return nil, errFakeClosed
	}

	host := d.hosts[h]
	if host == nil {
		return emptyFakeIter{r: d.r}, nil
	}

	series := host[id]
	if series == nil {
		return emptyFakeIter{r: d.r}, nil
	}

	var (
		istart = sort.Search(len(series), func(i int) bool { return !series[i].t.Before(start) })
		iend   = sort.Search(len(series), func(i int) bool { return !series[i].t.Before(end) })
	)

	if istart >= len(series) && iend >= len(series) {
		return emptyFakeIter{r: d.r}, nil
	}

	if istart >= len(series) {
		istart = 0
	}

	return &fakeIter{
		r:      d.r,
		points: series[istart:iend],
		cur:    0,
		next:   0}, nil
}

func (d *fakeDriver) write(h, id string, t time.Time, v float64) error {
	d.log.Debugf("write %s to %s (%s:%f)", id, h, t.String(), v)

	d.Lock()
	defer d.Unlock()
	if d.closed {
		return errFakeClosed
	}

	host := d.hosts[h]
	if host == nil {
		host = make(map[string][]datapoint)
		d.hosts[h] = host
	}

	host[id] = append(host[id], datapoint{t: t, v: v})
	sort.Sort(datapointsByTime(host[id]))
	return nil
}

func (d *fakeDriver) Open(msg proto.Message) (Conn, error) {
	cfg := msg.(*configtest.TestConfig)
	if len(cfg.Hosts) != 1 {
		return nil, errFakeOneHostOnly
	}

	return &fakeConn{
		host: cfg.Hosts[0],
		d:    d,
	}, nil
}

func (d *fakeDriver) Reconfigure(_ Conn, msg proto.Message) (Conn, error) {
	return d.Open(msg)
}

func (d *fakeDriver) Close() error {
	d.Lock()
	d.closed = true
	d.Unlock()
	return nil
}

func (d *fakeDriver) Type() cluster.Type        { return d.clusterType }
func (d *fakeDriver) ConfigType() proto.Message { return new(configtest.TestConfig) }

// fakeConn is a fake implementation of Conn which talks to the fake driver
type fakeConn struct {
	host string
	d    *fakeDriver
}

func (c *fakeConn) Read(id string, r retention.Resolution, start, end time.Time) (ts.SeriesIter, error) {
	return c.d.read(c.host, id, start, end)
}

func (c *fakeConn) Write(id string, r retention.Resolution, t time.Time, v float64) error {
	return c.d.write(c.host, id, t, v)
}

func (c *fakeConn) Close() error { return nil }

// fakeIter is a fake implementation of ts.SeriesIter that works off datapoints
type fakeIter struct {
	points    []datapoint
	r         retention.Resolution
	cur, next int
}

func (i *fakeIter) CoarsestResolution() retention.Resolution {
	return i.r
}

func (i *fakeIter) Next() bool {
	if i.next >= len(i.points) {
		return false
	}

	if i.next == 0 {
		i.next = 1
		return true
	}

	i.cur, i.next = i.next, i.next+1
	return true
}

func (i *fakeIter) Current() (float64, time.Time) {
	if i.cur >= len(i.points) {
		return 0, time.Time{}
	}

	p := i.points[i.cur]
	return p.v, p.t
}

func (i *fakeIter) Close() error { return nil }

// emptyFakeIter is an empty iterator
type emptyFakeIter struct {
	r retention.Resolution
}

func (i emptyFakeIter) Next() bool                               { return false }
func (i emptyFakeIter) Current() (float64, time.Time)            { return 0, time.Time{} }
func (i emptyFakeIter) Close() error                             { return nil }
func (i emptyFakeIter) CoarsestResolution() retention.Resolution { return i.r }

// datapoint is a stored datapoint
type datapoint struct {
	t time.Time
	v float64
}

type datapointsByTime []datapoint

func (d datapointsByTime) Len() int           { return len(d) }
func (d datapointsByTime) Less(i, j int) bool { return d[i].t.Before(d[j].t) }
func (d datapointsByTime) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
