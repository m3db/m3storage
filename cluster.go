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

// A Config is configuration for a cluster
type Config interface {
	// Version is the version of the configuration
	Version() int

	// Unmarshal unmarshals the configuration
	Unmarshal(c proto.Message) error
}

// A Type defines a type of storage (m3db, hbase, etc)
type Type interface {
	// Name is the name of the storage type
	Name() string
}

// A Database holds datapoints up to a certain amount of time
type Database interface {
	// Name is the name of the database
	Name() string

	// MaxRetention is the maximum amount of type datapoints will be retained in
	// this database
	MaxRetention() time.Duration
}

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
}
