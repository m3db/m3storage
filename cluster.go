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
	"time"

	"github.com/golang/protobuf/proto"
)

// A VersionedConfig is a versioned chunk of configuration
type VersionedConfig interface {
	// Version is the version of the configuration
	Version() int

	// Data unmarshals the actual data into the given value
	Data(v proto.Message) error
}

// NewVersionedConfig creates a new versioned configuration
func NewVersionedConfig(version int, data []byte) VersionedConfig {
	return versionedConfig{
		version: version,
		data:    data,
	}
}

// A Type defines a type of storage (m3db, hbase, etc)
type Type interface {
	// Name is the name of the storage class
	Name() string
	SetName(s string) Type
}

// A Database holds datapoints up to a certain amount of time
type Database interface {
	Name() string
	MaxRetention() time.Duration
}

// A Cluster defines a cluster
type Cluster interface {
	Name() string            // the name of the cluster
	Type() Type              // the cluster's storage type
	Config() VersionedConfig // the cluster's configuration
	Weight() int             // the weight of the cluster
	Datatabase() string      // the database to which the cluster belongs
}

// NewCluster returns a new Cluster with the given name, storage type, and config
func NewCluster(name string, typ Type, db string, config VersionedConfig) Cluster {
	return cluster{
		name:   name,
		typ:    typ,
		config: config,
		db:     db,
	}
}

type cluster struct {
	name   string
	typ    Type
	config VersionedConfig
	db     string
}

func (c cluster) Name() string            { return c.name }
func (c cluster) Type() Type              { return c.typ }
func (c cluster) Config() VersionedConfig { return c.config }
func (c cluster) Database() string        { return c.db }

type versionedConfig struct {
	version int
	data    []byte
}

func (c versionedConfig) Version() int { return c.version }
func (c versionedConfig) Data(v proto.Message) error {
	return proto.Unmarshal(c.data, v)
}
