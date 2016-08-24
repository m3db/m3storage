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
	DatabaseProperties
	Database
	ClusterShardAssignment
	ClusterProperties
	Cluster
	DatabaseChanges
	ClusterJoin
	ClusterDecommission
	CutoverRule
	CutoffRule
	ClusterMappingRuleSet
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
	Databases map[string]*Database `protobuf:"bytes,1,rep,name=databases" json:"databases,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
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

// PlacementChanges defines overall changes to the placement (database adds and removes)
type PlacementChanges struct {
	DatabaseAdds    map[string]*DatabaseAdd     `protobuf:"bytes,1,rep,name=database_adds" json:"database_adds,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	DatabaseChanges map[string]*DatabaseChanges `protobuf:"bytes,2,rep,name=database_changes" json:"database_changes,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
}

func (m *PlacementChanges) Reset()         { *m = PlacementChanges{} }
func (m *PlacementChanges) String() string { return proto.CompactTextString(m) }
func (*PlacementChanges) ProtoMessage()    {}

func (m *PlacementChanges) GetDatabaseAdds() map[string]*DatabaseAdd {
	if m != nil {
		return m.DatabaseAdds
	}
	return nil
}

func (m *PlacementChanges) GetDatabaseChanges() map[string]*DatabaseChanges {
	if m != nil {
		return m.DatabaseChanges
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

// DatabaseProperties are user specifiable properties for a Database
type DatabaseProperties struct {
	Name               string `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	NumShards          int32  `protobuf:"varint,2,opt,name=num_shards" json:"num_shards,omitempty"`
	MaxRetentionInSecs int32  `protobuf:"varint,3,opt,name=max_retention_in_secs" json:"max_retention_in_secs,omitempty"`
}

func (m *DatabaseProperties) Reset()         { *m = DatabaseProperties{} }
func (m *DatabaseProperties) String() string { return proto.CompactTextString(m) }
func (*DatabaseProperties) ProtoMessage()    {}

// Database defines a single database
type Database struct {
	Name                string                             `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	NumShards           int32                              `protobuf:"varint,2,opt,name=num_shards" json:"num_shards,omitempty"`
	MaxRetentionInSecs  int32                              `protobuf:"varint,3,opt,name=max_retention_in_secs" json:"max_retention_in_secs,omitempty"`
	CreatedAt           int64                              `protobuf:"varint,4,opt,name=created_at" json:"created_at,omitempty"`
	LastUpdatedAt       int64                              `protobuf:"varint,5,opt,name=last_updated_at" json:"last_updated_at,omitempty"`
	DecommissionedAt    int64                              `protobuf:"varint,6,opt,name=decommissioned_at" json:"decommissioned_at,omitempty"`
	ReadCutoverTime     int64                              `protobuf:"varint,7,opt,name=read_cutover_time" json:"read_cutover_time,omitempty"`
	WriteCutoverTime    int64                              `protobuf:"varint,8,opt,name=write_cutover_time" json:"write_cutover_time,omitempty"`
	CutoverCompleteTime int64                              `protobuf:"varint,9,opt,name=cutover_complete_time" json:"cutover_complete_time,omitempty"`
	Clusters            map[string]*Cluster                `protobuf:"bytes,10,rep,name=clusters" json:"clusters,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	ShardAssignments    map[string]*ClusterShardAssignment `protobuf:"bytes,11,rep,name=shard_assignments" json:"shard_assignments,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	GeneratedAt         int64                              `protobuf:"varint,12,opt,name=generated_at" json:"generated_at,omitempty"`
	Version             int32                              `protobuf:"varint,13,opt,name=version" json:"version,omitempty"`
	MappingRules        []*ClusterMappingRuleSet           `protobuf:"bytes,14,rep,name=mapping_rules" json:"mapping_rules,omitempty"`
}

func (m *Database) Reset()         { *m = Database{} }
func (m *Database) String() string { return proto.CompactTextString(m) }
func (*Database) ProtoMessage()    {}

func (m *Database) GetClusters() map[string]*Cluster {
	if m != nil {
		return m.Clusters
	}
	return nil
}

func (m *Database) GetShardAssignments() map[string]*ClusterShardAssignment {
	if m != nil {
		return m.ShardAssignments
	}
	return nil
}

func (m *Database) GetMappingRules() []*ClusterMappingRuleSet {
	if m != nil {
		return m.MappingRules
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

// ClusterProperties are the user specifiable properties for a Cluster
type ClusterProperties struct {
	Name   string `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Type   string `protobuf:"bytes,2,opt,name=type" json:"type,omitempty"`
	Weight uint32 `protobuf:"varint,3,opt,name=weight" json:"weight,omitempty"`
}

func (m *ClusterProperties) Reset()         { *m = ClusterProperties{} }
func (m *ClusterProperties) String() string { return proto.CompactTextString(m) }
func (*ClusterProperties) ProtoMessage()    {}

// Cluster is the immutable metadata for a cluster
type Cluster struct {
	Name      string        `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Type      string        `protobuf:"bytes,2,opt,name=type" json:"type,omitempty"`
	Weight    uint32        `protobuf:"varint,3,opt,name=weight" json:"weight,omitempty"`
	Status    ClusterStatus `protobuf:"varint,4,opt,name=status,enum=schema.ClusterStatus" json:"status,omitempty"`
	CreatedAt int64         `protobuf:"varint,5,opt,name=created_at" json:"created_at,omitempty"`
}

func (m *Cluster) Reset()         { *m = Cluster{} }
func (m *Cluster) String() string { return proto.CompactTextString(m) }
func (*Cluster) ProtoMessage()    {}

// DatabaseChanges capture pending changes to the database
type DatabaseChanges struct {
	Joins   map[string]*ClusterJoin         `protobuf:"bytes,1,rep,name=joins" json:"joins,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	Decomms map[string]*ClusterDecommission `protobuf:"bytes,2,rep,name=decomms" json:"decomms,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
}

func (m *DatabaseChanges) Reset()         { *m = DatabaseChanges{} }
func (m *DatabaseChanges) String() string { return proto.CompactTextString(m) }
func (*DatabaseChanges) ProtoMessage()    {}

func (m *DatabaseChanges) GetJoins() map[string]*ClusterJoin {
	if m != nil {
		return m.Joins
	}
	return nil
}

func (m *DatabaseChanges) GetDecomms() map[string]*ClusterDecommission {
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

// CutoverRule is a rule transition shards onto a given cluster
type CutoverRule struct {
	ClusterName      string   `protobuf:"bytes,1,opt,name=cluster_name" json:"cluster_name,omitempty"`
	Shards           []uint32 `protobuf:"varint,2,rep,name=shards" json:"shards,omitempty"`
	ReadCutoverTime  int64    `protobuf:"varint,3,opt,name=read_cutover_time" json:"read_cutover_time,omitempty"`
	WriteCutoverTime int64    `protobuf:"varint,4,opt,name=write_cutover_time" json:"write_cutover_time,omitempty"`
}

func (m *CutoverRule) Reset()         { *m = CutoverRule{} }
func (m *CutoverRule) String() string { return proto.CompactTextString(m) }
func (*CutoverRule) ProtoMessage()    {}

// CutoffRule is a rule tranistioning shards off a given
// cluster
type CutoffRule struct {
	ClusterName string   `protobuf:"bytes,1,opt,name=cluster_name" json:"cluster_name,omitempty"`
	Shards      []uint32 `protobuf:"varint,2,rep,name=shards" json:"shards,omitempty"`
	CutoffTime  int64    `protobuf:"varint,3,opt,name=cutoff_time" json:"cutoff_time,omitempty"`
}

func (m *CutoffRule) Reset()         { *m = CutoffRule{} }
func (m *CutoffRule) String() string { return proto.CompactTextString(m) }
func (*CutoffRule) ProtoMessage()    {}

// ClusterMappingRuleSet is a set of cluster mapping rules built off a
// particular version
type ClusterMappingRuleSet struct {
	ForVersion int32          `protobuf:"varint,1,opt,name=for_version" json:"for_version,omitempty"`
	Cutovers   []*CutoverRule `protobuf:"bytes,3,rep,name=cutovers" json:"cutovers,omitempty"`
	Cutoffs    []*CutoffRule  `protobuf:"bytes,4,rep,name=cutoffs" json:"cutoffs,omitempty"`
}

func (m *ClusterMappingRuleSet) Reset()         { *m = ClusterMappingRuleSet{} }
func (m *ClusterMappingRuleSet) String() string { return proto.CompactTextString(m) }
func (*ClusterMappingRuleSet) ProtoMessage()    {}

func (m *ClusterMappingRuleSet) GetCutovers() []*CutoverRule {
	if m != nil {
		return m.Cutovers
	}
	return nil
}

func (m *ClusterMappingRuleSet) GetCutoffs() []*CutoffRule {
	if m != nil {
		return m.Cutoffs
	}
	return nil
}

func init() {
	proto.RegisterEnum("schema.ClusterStatus", ClusterStatus_name, ClusterStatus_value)
}
