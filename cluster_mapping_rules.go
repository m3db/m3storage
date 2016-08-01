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

// A ClusterMappingRule defines a rule for mapping a shard to a storage cluster
type ClusterMappingRule interface {
	// ReadCutoverTime is the time at which reads should begin to be applied
	// against this cluster
	ReadCutoverTime() time.Time
	SetReadCutoverTime(t time.Time) ClusterMappingRule

	// WriteCutoverTime is the time at which writes should begin to be applied
	// against this cluster
	WriteCutoverTime() time.Time
	SetWriteCutoverTime(t time.Time) ClusterMappingRule

	// ReadCutoffTime is the time at which reads should stop being applied to
	// this cluster
	ReadCutoffTime() time.Time
	SetReadCutoffTime(t time.Time) ClusterMappingRule

	// RetentionPeriod is the length of time datapoints stored by this rule are retained
	RetentionPeriod() RetentionPeriod
	SetRetentionPeriod(p RetentionPeriod) ClusterMappingRule

	// Cluster is the cluster that holds the datapoints
	Cluster() Cluster
	SetCluster(c Cluster) ClusterMappingRule
}

// NewClusterMappingRule creates a new ClusterMappingRule
func NewClusterMappingRule() ClusterMappingRule { return new(clusterMappingRule) }

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

// A ClusterMappingRuleProvider provides cluster mapping rules
type ClusterMappingRuleProvider interface {
	// RulesForShard returns the mapping rules that currently apply to the given shard, plus
	// a channel that can be used to receive new mapping rules
	RulesForShard(n int) ([]ClusterMappingRule, <-chan []ClusterMappingRule, error)
}

type clusterMappingRule struct {
	readCutoverTime  time.Time
	writeCutoverTime time.Time
	readCutoffTime   time.Time
	retentionPeriod  RetentionPeriod
	resolution       Resolution
	cluster          Cluster
}

func (sr *clusterMappingRule) ReadCutoverTime() time.Time       { return sr.readCutoverTime }
func (sr *clusterMappingRule) ReadCutoffTime() time.Time        { return sr.readCutoffTime }
func (sr *clusterMappingRule) WriteCutoverTime() time.Time      { return sr.writeCutoverTime }
func (sr *clusterMappingRule) RetentionPeriod() RetentionPeriod { return sr.retentionPeriod }
func (sr *clusterMappingRule) Resolution() Resolution           { return sr.resolution }
func (sr *clusterMappingRule) Cluster() Cluster                 { return sr.cluster }

func (sr *clusterMappingRule) SetReadCutoverTime(t time.Time) ClusterMappingRule {
	sr.readCutoverTime = t
	return sr
}
func (sr *clusterMappingRule) SetReadCutoffTime(t time.Time) ClusterMappingRule {
	sr.readCutoffTime = t
	return sr
}
func (sr *clusterMappingRule) SetWriteCutoverTime(t time.Time) ClusterMappingRule {
	sr.writeCutoverTime = t
	return sr
}
func (sr *clusterMappingRule) SetRetentionPeriod(p RetentionPeriod) ClusterMappingRule {
	sr.retentionPeriod = p
	return sr
}
func (sr *clusterMappingRule) SetResolution(r Resolution) ClusterMappingRule {
	sr.resolution = r
	return sr
}
func (sr *clusterMappingRule) SetCluster(c Cluster) ClusterMappingRule {
	sr.cluster = c
	return sr
}

type clusterConfigChange struct {
	c Cluster
}

func (c clusterConfigChange) ChangedCluster() Cluster { return c.c }
