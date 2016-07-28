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

// A StorageMappingRule defines a rule for mapping a shard to a storage cluster
type StorageMappingRule interface {
	// ReadCutoverTime is the time at which reads should begin to be applied
	// against this cluster
	ReadCutoverTime() time.Time
	SetReadCutoverTime(t time.Time) StorageMappingRule

	// WriteCutoverTime is the time at which writes should begin to be applied
	// against this cluster
	WriteCutoverTime() time.Time
	SetWriteCutoverTime(t time.Time) StorageMappingRule

	// ReadCutoffTime is the time at which reads should stop being applied to
	// this cluster
	ReadCutoffTime() time.Time
	SetReadCutoffTime(t time.Time) StorageMappingRule

	// RetentionPeriod is the length of time
	RetentionPeriod() time.Duration
	SetRetentionPeriod(p time.Duration) StorageMappingRule

	// Resolution is the resolution at which datapoints are stored under this
	// rule
	Resolution() Resolution
	SetResolution(r Resolution) StorageMappingRule

	// Cluster is the cluster that holds the datapoints
	Cluster() Cluster
	SetCluster(c Cluster) StorageMappingRule
}

// NewStorageMappingRule creates a new StorageMappingRule
func NewStorageMappingRule() StorageMappingRule { return new(storageMappingRule) }

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

// A StorageMapping is a set of rules mapping shards onto a storage cluster.
type StorageMapping interface {
	// RulesForShard returns the mapping rules that currently apply to the given shard, plus
	// a channel that can be used to receive new mapping rules
	RulesForShard(n int) ([]StorageMappingRule, <-chan []StorageMappingRule, error)

	// Clusters returns all of the currently defined clusters, plus a channel
	// that can be used to receive cluster changes (new clusters, changes to
	// cluster configurations)
	Clusters() ([]Cluster, <-chan ClusterConfigChange, error)
}

type storageMappingRule struct {
	readCutoverTime  time.Time
	writeCutoverTime time.Time
	readCutoffTime   time.Time
	retentionPeriod  time.Duration
	resolution       Resolution
	cluster          Cluster
}

func (sr *storageMappingRule) ReadCutoverTime() time.Time     { return sr.readCutoverTime }
func (sr *storageMappingRule) ReadCutoffTime() time.Time      { return sr.readCutoffTime }
func (sr *storageMappingRule) WriteCutoverTime() time.Time    { return sr.writeCutoverTime }
func (sr *storageMappingRule) RetentionPeriod() time.Duration { return sr.retentionPeriod }
func (sr *storageMappingRule) Resolution() Resolution         { return sr.resolution }
func (sr *storageMappingRule) Cluster() Cluster               { return sr.cluster }

func (sr *storageMappingRule) SetReadCutoverTime(t time.Time) StorageMappingRule {
	sr.readCutoverTime = t
	return sr
}
func (sr *storageMappingRule) SetReadCutoffTime(t time.Time) StorageMappingRule {
	sr.readCutoffTime = t
	return sr
}
func (sr *storageMappingRule) SetWriteCutoverTime(t time.Time) StorageMappingRule {
	sr.writeCutoverTime = t
	return sr
}
func (sr *storageMappingRule) SetRetentionPeriod(p time.Duration) StorageMappingRule {
	sr.retentionPeriod = p
	return sr
}
func (sr *storageMappingRule) SetResolution(r Resolution) StorageMappingRule {
	sr.resolution = r
	return sr
}
func (sr *storageMappingRule) SetCluster(c Cluster) StorageMappingRule {
	sr.cluster = c
	return sr
}

type clusterConfigChange struct {
	c Cluster
}

func (c clusterConfigChange) ChangedCluster() Cluster { return c.c }
