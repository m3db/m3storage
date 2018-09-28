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

package cluster

import (
	"errors"
	"sync"

	"github.com/m3db/m3x/close"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/watch"
)

var (
	errClosed = errors.New("closed")

	errProviderLogRequired = errors.New("log is required")
)

// A Provider provides information about cluster configurations
type Provider interface {
	xclose.Closer

	// WatchCluster returns the config for a cluster as a watch that can be used
	// to listen for updates to that cluster.  Callers must wait on the watch
	// channel before attempting to access the Cluster
	WatchCluster(database, cluster string) (Watch, error)
}

// ProviderOptions are options to a provider
type ProviderOptions interface {
	// Logger is the logger to use
	Logger(log xlog.Logger) ProviderOptions
	GetLogger() xlog.Logger

	// Validate validates the options
	Validate() error
}

// NewProviderOptions creates new default provider options
func NewProviderOptions() ProviderOptions {
	return providerOptions{
		log: xlog.NullLogger,
	}
}

// NewProvider instantiates a new provider around an input channel.
func NewProvider(updateCh <-chan Cluster, opts ProviderOptions) (Provider, error) {
	if opts == nil {
		opts = NewProviderOptions()
	}

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	p := &provider{
		updateCh: updateCh,
		close:    make(chan struct{}),
		done:     make(chan struct{}),
		log:      opts.GetLogger(),
		c:        make(map[string]*watchedCluster),
	}

	go p.run()
	return p, nil
}

type provider struct {
	sync.RWMutex

	c        map[string]*watchedCluster
	closed   bool
	updateCh <-chan Cluster
	close    chan struct{}
	done     chan struct{}
	log      xlog.Logger
}

func (p *provider) WatchCluster(database, cluster string) (Watch, error) {
	wc, err := p.findOrCreateWatchedCluster(database, cluster)
	if err != nil {
		return nil, err
	}

	_, w, err := wc.w.Watch()
	if err != nil {
		return nil, err
	}

	return NewWatch(w), nil
}

func (p *provider) Close() error {
	p.Lock()
	if p.closed {
		return nil
	}
	p.closed = true
	p.Unlock()
	close(p.close)

	<-p.done
	return nil
}

func (p *provider) run() {
	for {
		select {
		case c := <-p.updateCh:
			p.update(c)
		case <-p.close:
			close(p.done)
			return
		}
	}
}

func (p *provider) update(c Cluster) {
	wc, err := p.findOrCreateWatchedCluster(c.Database(), c.Name())
	if err != nil {
		p.log.Errorf("could not create watched cluster %s:%s: %v", c.Database(), c.Name(), err)
		return
	}

	if wc.v >= c.Config().Version() {
		p.log.Warnf("ignoring outdated config %d for cluster %s:%s",
			c.Config().Version(), c.Database(), c.Name())
		return
	}

	// NB(mmihic): Updates only ever come through the background goroutine, so
	// this is safe to perform outside of the lock
	p.log.Infof("applying config %d for cluster %s:%s",
		c.Config().Version(), c.Database(), c.Name())
	wc.v = c.Config().Version()
	wc.w.Update(c)
}

func (p *provider) findOrCreateWatchedCluster(database, cluster string) (*watchedCluster, error) {
	key := FmtKey(database, cluster)

	p.RLock()
	if p.closed {
		return nil, errClosed
	}
	wc := p.c[key]
	p.RUnlock()

	if wc != nil {
		return wc, nil
	}

	p.Lock()
	defer p.Unlock()
	if p.closed {
		return nil, errClosed
	}

	wc = p.c[key]
	if wc == nil {
		wc = &watchedCluster{
			v: -1, // guarantee updates
			w: xwatch.NewWatchable(),
		}
		p.c[key] = wc
	}
	return wc, nil
}

// A watchedCluster tracks a cluster that is being watched
type watchedCluster struct {
	v int
	w xwatch.Watchable
}

// providerOptions are options to a provider
type providerOptions struct {
	log xlog.Logger
}

func (opts providerOptions) GetLogger() xlog.Logger { return opts.log }
func (opts providerOptions) Logger(log xlog.Logger) ProviderOptions {
	opts.log = log
	return opts
}

func (opts providerOptions) Validate() error {
	if opts.log == nil {
		return errProviderLogRequired
	}

	return nil
}
