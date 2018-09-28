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
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
)

// A Config is configuration for a cluster
type Config interface {
	// Version is the version of the configuration
	Version() int

	// Unmarshal unmarshals the configuration
	Unmarshal(c proto.Message) error
}

// NewConfig returns a new Config object
func NewConfig(v int, bytes []byte) Config {
	return config{
		v:     v,
		bytes: bytes,
	}
}

type config struct {
	v     int
	bytes []byte
}

func (c config) Version() int                      { return c.v }
func (c config) Unmarshal(cfg proto.Message) error { return proto.Unmarshal(c.bytes, cfg) }

// A Type defines a type of storage (m3db, hbase, etc)
type Type interface {
	// Name is the name of the storage type
	Name() string
}

// NewType returns a new cluster Type
func NewType(t string) Type { return clusterType{t} }

type clusterType struct {
	string
}

func (t clusterType) Name() string { return t.string }

// A Database holds datapoints up to a certain amount of time
type Database interface {
	// Name is the name of the database
	Name() string

	// MaxRetention is the maximum amount of type datapoints will be retained in
	// this database
	MaxRetention() time.Duration
}

// NewDatabase returns a new Database
func NewDatabase(name string, maxRetention time.Duration) Database {
	return database{
		name:         name,
		maxRetention: maxRetention,
	}
}

type database struct {
	name         string
	maxRetention time.Duration
}

func (db database) Name() string                { return db.name }
func (db database) MaxRetention() time.Duration { return db.maxRetention }

// A Cluster defines a cluster of nodes within a database
type Cluster interface {
	// Name is the name of the cluster
	Name() string

	// Type is the storage type for the cluster
	Type() Type

	// Config is the cluster's configuration
	Config() Config

	// Database is the name of the database to which the cluster belongs
	Database() string

	// Key returns a key that can uniquely identify this cluster across databases
	Key() string
}

// NewCluster returns a new Cluster
func NewCluster(name string, clusterType Type, database string, config Config) Cluster {
	return cluster{
		name:        name,
		clusterType: clusterType,
		config:      config,
		database:    database,
	}
}

type cluster struct {
	name        string
	database    string
	config      Config
	clusterType Type
}

func (c cluster) Name() string     { return c.name }
func (c cluster) Database() string { return c.database }
func (c cluster) Config() Config   { return c.config }
func (c cluster) Type() Type       { return c.clusterType }
func (c cluster) Key() string      { return FmtKey(c.database, c.name) }

// FmtKey returns a key that can be used to uniquely identify a cluster+database
func FmtKey(database, cluster string) string {
	return fmt.Sprintf("%s-%s", database, cluster)
}
