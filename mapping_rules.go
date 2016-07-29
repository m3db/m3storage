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
)

// A MappingRule defines a rule for mapping a shard to a storage cluster
type MappingRule interface {
	// ReadCutoverTime is the time at which reads should begin to be applied
	// against this cluster
	ReadCutoverTime() time.Time
	SetReadCutoverTime(t time.Time) MappingRule

	// WriteCutoverTime is the time at which writes should begin to be applied
	// against this cluster
	WriteCutoverTime() time.Time
	SetWriteCutoverTime(t time.Time) MappingRule

	// ReadCutoffTime is the time at which reads should stop being applied to
	// this cluster
	ReadCutoffTime() time.Time
	SetReadCutoffTime(t time.Time) MappingRule

	// RetentionPeriod is the length of time
	RetentionPeriod() time.Duration
	SetRetentionPeriod(p time.Duration) MappingRule

	// Resolution is the resolution at which datapoints are stored under this
	// rule
	Resolution() Resolution
	SetResolution(r Resolution) MappingRule

	// Cluster is the cluster that holds the datapoints
	Cluster() Cluster
	SetCluster(c Cluster) MappingRule
}

// NewMappingRule creates a new MappingRule
func NewMappingRule() MappingRule { return new(mappingRule) }

// A ClusterConfigChange is a change to a cluster configuration
type ClusterConfigChange interface {
	ChangedCluster() Cluster // ChangedCluster is the new cluster that has changed
}

// NewClusterConfigChange creates a new ClusterConfigChange message
func NewClusterConfigChange(c Cluster) ClusterConfigChange {
	return clusterConfigChange{
		c: c,
	}
}

// A Mapping is a set of rules mapping shards onto a storage cluster.
type Mapping interface {
	// RulesForShard returns the mapping rules that currently apply to the given shard, plus
	// a channel that can be used to receive new mapping rules
	RulesForShard(n int) ([]MappingRule, <-chan []MappingRule, error)

	// Clusters returns all of the currently defined clusters, plus a channel
	// that can be used to receive cluster changes (new clusters, changes to
	// cluster configurations)
	Clusters() ([]Cluster, <-chan ClusterConfigChange, error)
}

type mappingRule struct {
	readCutoverTime  time.Time
	writeCutoverTime time.Time
	readCutoffTime   time.Time
	retentionPeriod  time.Duration
	resolution       Resolution
	cluster          Cluster
}

func (sr *mappingRule) ReadCutoverTime() time.Time     { return sr.readCutoverTime }
func (sr *mappingRule) ReadCutoffTime() time.Time      { return sr.readCutoffTime }
func (sr *mappingRule) WriteCutoverTime() time.Time    { return sr.writeCutoverTime }
func (sr *mappingRule) RetentionPeriod() time.Duration { return sr.retentionPeriod }
func (sr *mappingRule) Resolution() Resolution         { return sr.resolution }
func (sr *mappingRule) Cluster() Cluster               { return sr.cluster }

func (sr *mappingRule) SetReadCutoverTime(t time.Time) MappingRule {
	sr.readCutoverTime = t
	return sr
}
func (sr *mappingRule) SetReadCutoffTime(t time.Time) MappingRule {
	sr.readCutoffTime = t
	return sr
}
func (sr *mappingRule) SetWriteCutoverTime(t time.Time) MappingRule {
	sr.writeCutoverTime = t
	return sr
}
func (sr *mappingRule) SetRetentionPeriod(p time.Duration) MappingRule {
	sr.retentionPeriod = p
	return sr
}
func (sr *mappingRule) SetResolution(r Resolution) MappingRule {
	sr.resolution = r
	return sr
}
func (sr *mappingRule) SetCluster(c Cluster) MappingRule {
	sr.cluster = c
	return sr
}

type clusterConfigChange struct {
	c Cluster
}

func (c clusterConfigChange) ChangedCluster() Cluster { return c.c }
