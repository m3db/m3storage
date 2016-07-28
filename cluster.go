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
	"github.com/golang/protobuf/proto"
)

// A VersionedConfig is a versioned chunk of configuration
type VersionedConfig interface {
	// Version is the version of the configuration
	Version() int

	// Data is the actual configuration data
	Data() proto.Message
}

// NewVersionedConfig creates a new versioned configuration
func NewVersionedConfig(version int, data proto.Message) VersionedConfig {
	return versionedConfig{
		version: version,
		data:    data,
	}
}

// A Cluster defines a cluster
type Cluster interface {
	Name() string            // the name of the cluster
	StorageClass() string    // the cluster's storage class
	Config() VersionedConfig // the cluster's configuration
}

// NewCluster returns a new Cluster with the given name, storage class, and config
func NewCluster(name, storageClass string, config VersionedConfig) Cluster {
	return cluster{
		name:         name,
		storageClass: storageClass,
		config:       config,
	}
}

type cluster struct {
	name         string
	storageClass string
	config       VersionedConfig
}

func (c cluster) Name() string            { return c.name }
func (c cluster) StorageClass() string    { return c.storageClass }
func (c cluster) Config() VersionedConfig { return c.config }

type versionedConfig struct {
	version int
	data    proto.Message
}

func (c versionedConfig) Version() int        { return c.version }
func (c versionedConfig) Data() proto.Message { return c.data }
