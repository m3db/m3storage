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
	"github.com/m3db/m3x/close"
	"github.com/m3db/m3x/watch"
)

// A Watch watches for config changes on a cluster
type Watch interface {
	xclose.Closer

	// C is the channel receiving notifications of config changes
	C() <-chan struct{}

	// Get returns the current state of the cluster
	Get() Cluster
}

// NewWatch wraps a type-specific Watch around a generic Watch
func NewWatch(w xwatch.Watch) Watch { return &watch{w} }

type watch struct {
	xwatch.Watch
}

func (w *watch) C() <-chan struct{} { return w.Watch.C() }

func (w *watch) Get() Cluster {
	if c := w.Watch.Get(); c != nil {
		return c.(Cluster)
	}

	return nil
}

func (w *watch) Close() error {
	w.Watch.Close()
	return nil
}
