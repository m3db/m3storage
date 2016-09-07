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
	"time"

	"github.com/m3db/m3storage/cluster"
	"github.com/m3db/m3storage/retention"
	"github.com/m3db/m3storage/ts"
	"github.com/m3db/m3x/close"

	"github.com/golang/protobuf/proto"
)

// A Conn is a connection to a storage cluster, which can be used to
// read and write datapoints to that cluster
type Conn interface {
	xclose.Closer

	// Read reads datapoints for the given id at the given time range and desired
	// resolution.  The connection is not obligated to return datapoints at that
	// resolution
	Read(id string, r retention.Resolution, start, end time.Time) (ts.SeriesIter, error)

	// Write writes a datapoint for the given id at the given time range, desired
	// retention period, and resolution
	Write(id string, r retention.Resolution, t time.Time, value float64) error
}

// A Driver is used to create connections to a cluster of a given storage class
type Driver interface {
	xclose.Closer

	// ConfigType returns an empty proto.Message representing the configuration
	// type used by the driver
	ConfigType() proto.Message

	// Type is the type of storage supported by the driver
	Type() cluster.Type

	// Open opens a connection with the provided config
	Open(config proto.Message) (Conn, error)

	// Reconfigure applies a new configuration to an existing connection.
	// Connections could be heavyweight objects, and drivers may wish to optimize
	// reconfiguration to avoid creating and destroying them.  Drivers that do
	// not support dynamic reconfiguration can create a new Connection and
	// dispose of the old connection
	Reconfigure(existing Conn, newConfig proto.Message) (Conn, error)
}
