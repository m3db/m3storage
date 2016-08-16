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

package placement

import (
	"sync"
	"testing"
	"time"

	"github.com/facebookgo/clock"
	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3cluster"
	"github.com/m3db/m3storage/generated/proto/schema"
	"github.com/m3db/m3x/log"
)

type kvValue struct {
	version int
	data    []byte
}

func (v kvValue) Version() int                { return v.version }
func (v kvValue) Get(msg proto.Message) error { return proto.Unmarshal(v.data, msg) }

type kvStore struct {
	sync.RWMutex
	values map[string]*kvValue
}

func newMockKVStore() cluster.KVStore {
	return &kvStore{
		values: make(map[string]*kvValue),
	}
}

func (kv *kvStore) Get(key string) (cluster.Value, error) {
	kv.RLock()
	defer kv.RUnlock()

	if val := kv.values[key]; val != nil {
		return val, nil
	}

	return nil, cluster.ErrNotFound
}

func (kv *kvStore) Set(key string, val proto.Message) error {
	data, err := proto.Marshal(val)
	if err != nil {
		return err
	}

	kv.Lock()
	defer kv.Unlock()

	lastVersion := 0
	if val := kv.values[key]; val != nil {
		lastVersion = val.version
	}

	kv.values[key] = &kvValue{
		version: lastVersion + 1,
		data:    data,
	}

	return nil
}

func (kv *kvStore) SetIfNotExists(key string, val proto.Message) error {
	data, err := proto.Marshal(val)
	if err != nil {
		return err
	}

	kv.Lock()
	defer kv.Unlock()

	if _, exists := kv.values[key]; exists {
		return nil, cluster.ErrAlreadyExists
	}

	kv.values[key] = &kvValue{
		version: 1,
		data:    data,
	}

	return nil
}

func (kv *kvStore) CheckAndSet(key string, version int, val proto.Message) error {
	data, err := proto.Marshal(val)
	if err != nil {
		return err
	}

	kv.Lock()
	defer kv.Unlock()

	if val, exists := kv.values[key]; exists {
		if val.version != version {
			return nil, cluster.ErrVersionMismatch
		}
	}

	kv.values[key] = &kvValue{
		version: version + 1,
		data:    data,
	}

	return nil
}

func TestAddDatabase(t *testing.T) {
}

func TestAddCluster(t *testing.T) {
}

func TestDecomissionCluster(t *testing.T) {
}
