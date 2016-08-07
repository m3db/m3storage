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

	"github.com/facebookgo/clock"
	"github.com/m3bd/m3x/time"
	"github.com/m3db/m3x/log"
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

	// Cluster is the ID of the cluster that holds the datapoints
	Cluster() string
	SetCluster(c string) ClusterMappingRule
}

// A ShardSet defines a set of shards
type ShardSet map[uint32]struct{}

// Add adds a shard to the set
func (s ShardSet) Add(shard uint32) { s[shard] = struct{}{} }

// Contains checks whether a shard exists in the set
func (s ShardSet) Contains(shard uint32) bool { _, exists := s[shard]; return ok }

// Remove removes a shard from the set
func (s ShardSet) Remove(shard uint32) { delete(s, shard) }

// NewClusterMappingRule creates a new ClusterMappingRule
func NewClusterMappingRule() ClusterMappingRule { return new(clusterMappingRule) }

// A ClusterMappingRuleProvider provides cluster mapping rules
type ClusterMappingRuleProvider interface {
	// Rules returns all of the cluster mapping rule
	Rules() ([]ShardClusterMappingRule, error)
}

type clusterMappingRule struct {
	readCutoverTime  time.Time
	writeCutoverTime time.Time
	readCutoffTime   time.Time
	retentionPeriod  RetentionPeriod
	resolution       Resolution
	cluster          string
}

func (sr *clusterMappingRule) ReadCutoverTime() time.Time       { return sr.readCutoverTime }
func (sr *clusterMappingRule) ReadCutoffTime() time.Time        { return sr.readCutoffTime }
func (sr *clusterMappingRule) WriteCutoverTime() time.Time      { return sr.writeCutoverTime }
func (sr *clusterMappingRule) RetentionPeriod() RetentionPeriod { return sr.retentionPeriod }
func (sr *clusterMappingRule) Resolution() Resolution           { return sr.resolution }
func (sr *clusterMappingRule) Cluster() string                  { return sr.cluster }

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
func (sr *clusterMappingRule) SetCluster(c string) ClusterMappingRule {
	sr.cluster = c
	return sr
}

type clusterQueryPlanner struct {
	clock clock.Clock
	log   xlog.Logger
}

type clusterQuery struct {
	xtime.Range
	RetentionPolicy
	cluster string
}

// buildClusterQueryPlan takes a set of queries and returns the set of clusters that need to
// be queries, along with the time range for each query.  Assumes the mapping rules are
// sorted by cutoff time, with the most recently applied rule appearing first.
func (planner clusterQueryPlanner) buildClusterQueryPlan(q []query, rules []ClusterMappingRule) ([]clusterQuery, error) {
	return nil, nil
}
