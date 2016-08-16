// Copyright (c) 2016 Uber Technologies, Inc.
//
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

// Code generated by protoc-gen-go.
// source: schema.proto
// DO NOT EDIT!

/*
Package schema is a generated protocol buffer package.

It is generated from these files:
	schema.proto

It has these top-level messages:
	Placement
	PlacementChanges
	DatabaseAdd
	Database
	ClusterShardAssignment
	DatabaseLayout
	Cluster
	DatabaseChanges
	ClusterJoin
	ClusterDecommission
*/
package schema

import proto "github.com/golang/protobuf/proto"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal

// ClusterStatus is the status of a cluster
type ClusterStatus int32

const (
	ClusterStatus_UNKNOWN         ClusterStatus = 0
	ClusterStatus_ACTIVE          ClusterStatus = 1
	ClusterStatus_DECOMMISSIONING ClusterStatus = 2
)

var ClusterStatus_name = map[int32]string{
	0: "UNKNOWN",
	1: "ACTIVE",
	2: "DECOMMISSIONING",
}
var ClusterStatus_value = map[string]int32{
	"UNKNOWN":         0,
	"ACTIVE":          1,
	"DECOMMISSIONING": 2,
}

func (x ClusterStatus) String() string {
	return proto.EnumName(ClusterStatus_name, int32(x))
}

// Placement defines an entire storage placement
type Placement struct {
	Databases      map[string]*Database `protobuf:"bytes,1,rep,name=databases" json:"databases,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	PendingChanges *PlacementChanges    `protobuf:"bytes,2,opt,name=pending_changes" json:"pending_changes,omitempty"`
}

func (m *Placement) Reset()         { *m = Placement{} }
func (m *Placement) String() string { return proto.CompactTextString(m) }
func (*Placement) ProtoMessage()    {}

func (m *Placement) GetDatabases() map[string]*Database {
	if m != nil {
		return m.Databases
	}
	return nil
}

func (m *Placement) GetPendingChanges() *PlacementChanges {
	if m != nil {
		return m.PendingChanges
	}
	return nil
}

// PlacementChanges defines overall changes to the placement (database adds and removes)
type PlacementChanges struct {
	DatabaseAdds *DatabaseAdd `protobuf:"bytes,1,opt,name=database_adds" json:"database_adds,omitempty"`
}

func (m *PlacementChanges) Reset()         { *m = PlacementChanges{} }
func (m *PlacementChanges) String() string { return proto.CompactTextString(m) }
func (*PlacementChanges) ProtoMessage()    {}

func (m *PlacementChanges) GetDatabaseAdds() *DatabaseAdd {
	if m != nil {
		return m.DatabaseAdds
	}
	return nil
}

// DatabaseAdd is an update that adds a database
type DatabaseAdd struct {
	Database *Database `protobuf:"bytes,1,opt,name=database" json:"database,omitempty"`
}

func (m *DatabaseAdd) Reset()         { *m = DatabaseAdd{} }
func (m *DatabaseAdd) String() string { return proto.CompactTextString(m) }
func (*DatabaseAdd) ProtoMessage()    {}

func (m *DatabaseAdd) GetDatabase() *Database {
	if m != nil {
		return m.Database
	}
	return nil
}

// Database defines a single database
type Database struct {
	Name                string           `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	NumShards           int32            `protobuf:"varint,2,opt,name=num_shards" json:"num_shards,omitempty"`
	MaxRetentionInSecs  []int32          `protobuf:"varint,3,rep,name=max_retention_in_secs" json:"max_retention_in_secs,omitempty"`
	ReadCutoverTime     int64            `protobuf:"varint,4,opt,name=read_cutover_time" json:"read_cutover_time,omitempty"`
	WriteCutoverTime    int64            `protobuf:"varint,5,opt,name=write_cutover_time" json:"write_cutover_time,omitempty"`
	CutoverCompleteTime int64            `protobuf:"varint,6,opt,name=cutover_complete_time" json:"cutover_complete_time,omitempty"`
	CurrentLayout       *DatabaseLayout  `protobuf:"bytes,7,opt,name=current_layout" json:"current_layout,omitempty"`
	PendingChanges      *DatabaseChanges `protobuf:"bytes,8,opt,name=pending_changes" json:"pending_changes,omitempty"`
	CreatedAt           int64            `protobuf:"varint,9,opt,name=created_at" json:"created_at,omitempty"`
	LastUpdatedAt       int64            `protobuf:"varint,10,opt,name=last_updated_at" json:"last_updated_at,omitempty"`
}

func (m *Database) Reset()         { *m = Database{} }
func (m *Database) String() string { return proto.CompactTextString(m) }
func (*Database) ProtoMessage()    {}

func (m *Database) GetCurrentLayout() *DatabaseLayout {
	if m != nil {
		return m.CurrentLayout
	}
	return nil
}

func (m *Database) GetPendingChanges() *DatabaseChanges {
	if m != nil {
		return m.PendingChanges
	}
	return nil
}

// ClusterShardAssignment captures the shards currently assigned to a cluster
type ClusterShardAssignment struct {
	Shards []uint32 `protobuf:"varint,1,rep,name=shards" json:"shards,omitempty"`
}

func (m *ClusterShardAssignment) Reset()         { *m = ClusterShardAssignment{} }
func (m *ClusterShardAssignment) String() string { return proto.CompactTextString(m) }
func (*ClusterShardAssignment) ProtoMessage()    {}

// DatabaseLayout defines the current layout of the database
type DatabaseLayout struct {
	Clusters        map[string]*Cluster                `protobuf:"bytes,1,rep,name=clusters" json:"clusters,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	ShardAssigments map[string]*ClusterShardAssignment `protobuf:"bytes,2,rep,name=shard_assigments" json:"shard_assigments,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	GeneratedAt     int64                              `protobuf:"varint,9,opt,name=generated_at" json:"generated_at,omitempty"`
}

func (m *DatabaseLayout) Reset()         { *m = DatabaseLayout{} }
func (m *DatabaseLayout) String() string { return proto.CompactTextString(m) }
func (*DatabaseLayout) ProtoMessage()    {}

func (m *DatabaseLayout) GetClusters() map[string]*Cluster {
	if m != nil {
		return m.Clusters
	}
	return nil
}

func (m *DatabaseLayout) GetShardAssigments() map[string]*ClusterShardAssignment {
	if m != nil {
		return m.ShardAssigments
	}
	return nil
}

// Cluster is the immutable metadata for a cluster
type Cluster struct {
	Name      string        `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Type      string        `protobuf:"bytes,2,opt,name=type" json:"type,omitempty"`
	Status    ClusterStatus `protobuf:"varint,3,opt,name=status,enum=schema.ClusterStatus" json:"status,omitempty"`
	CreatedAt int64         `protobuf:"varint,4,opt,name=created_at" json:"created_at,omitempty"`
}

func (m *Cluster) Reset()         { *m = Cluster{} }
func (m *Cluster) String() string { return proto.CompactTextString(m) }
func (*Cluster) ProtoMessage()    {}

// DatabaseChanges capture pending changes to the database
type DatabaseChanges struct {
	Joins   []*ClusterJoin         `protobuf:"bytes,1,rep,name=joins" json:"joins,omitempty"`
	Decomms []*ClusterDecommission `protobuf:"bytes,2,rep,name=decomms" json:"decomms,omitempty"`
}

func (m *DatabaseChanges) Reset()         { *m = DatabaseChanges{} }
func (m *DatabaseChanges) String() string { return proto.CompactTextString(m) }
func (*DatabaseChanges) ProtoMessage()    {}

func (m *DatabaseChanges) GetJoins() []*ClusterJoin {
	if m != nil {
		return m.Joins
	}
	return nil
}

func (m *DatabaseChanges) GetDecomms() []*ClusterDecommission {
	if m != nil {
		return m.Decomms
	}
	return nil
}

// ClusterJoin captures the data required to join a cluster to a database
type ClusterJoin struct {
	Cluster *Cluster `protobuf:"bytes,1,opt,name=cluster" json:"cluster,omitempty"`
}

func (m *ClusterJoin) Reset()         { *m = ClusterJoin{} }
func (m *ClusterJoin) String() string { return proto.CompactTextString(m) }
func (*ClusterJoin) ProtoMessage()    {}

func (m *ClusterJoin) GetCluster() *Cluster {
	if m != nil {
		return m.Cluster
	}
	return nil
}

// ClusterDecommission captures the data required to decommission a cluster
type ClusterDecommission struct {
	ClusterName string `protobuf:"bytes,1,opt,name=cluster_name" json:"cluster_name,omitempty"`
}

func (m *ClusterDecommission) Reset()         { *m = ClusterDecommission{} }
func (m *ClusterDecommission) String() string { return proto.CompactTextString(m) }
func (*ClusterDecommission) ProtoMessage()    {}

func init() {
	proto.RegisterEnum("schema.ClusterStatus", ClusterStatus_name, ClusterStatus_value)
}
