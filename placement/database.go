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

package placement

import (
	"errors"
	"sort"
	"sync"

	"github.com/m3db/m3storage/generated/proto/schema"
	"github.com/m3db/m3x/log"
)

var (
	errInvalidTransition = errors.New("invalid shard transition")
)

// database are the mappings for a given database.
type database struct {
	sync.RWMutex
	name               string
	version            int32
	maxRetentionInSecs int32
	clusters           *clusters
	mappings           *mappings
	log                xlog.Logger
}

// newDatabase returns a newly initialized database
func newDatabase(dbConfig *schema.Database, log xlog.Logger) (*database, error) {
	db := &database{
		name:               dbConfig.Properties.Name,
		version:            dbConfig.Version,
		maxRetentionInSecs: dbConfig.Properties.MaxRetentionInSecs,
		clusters:           newClusters(log),
		mappings:           newMappings(dbConfig.Properties.Name, log),
		log:                log,
	}

	sort.Sort(mappingRuleSetsByVersion(dbConfig.MappingRules))

	// Apply current mapping rules
	clusterVersions := make(map[string]int)
	for _, rules := range dbConfig.MappingRules {
		if err := db.mappings.apply(rules, clusterVersions); err != nil {
			return nil, err
		}
	}

	// Create known clusters
	for cname, cConfig := range dbConfig.Clusters {
		db.clusters.update(newCluster(db.name, cConfig, clusterVersions[cname]))
	}

	return db, nil
}

// update updates the database with new mapping information
func (db *database) update(dbConfig *schema.Database) error {
	// Short circuit if we are already at the proper version
	db.RLock()
	if db.version >= dbConfig.Version {
		db.RUnlock()
		return nil
	}

	// Clone mapping information so that users performing lookups don't block
	// while we're building the updates
	newMappings := db.mappings.clone()
	db.RUnlock()

	// Apply new rules, also determining which cluster versions have changed
	sort.Sort(mappingRuleSetsByVersion(dbConfig.MappingRules))
	newClusterVersions := make(map[string]int)
	for _, rules := range dbConfig.MappingRules {
		if rules.ForVersion <= db.version {
			continue
		}

		if err := newMappings.apply(rules, newClusterVersions); err != nil {
			return err
		}
	}

	// Update clusters which are either new or have changed (protected by a
	// separate mutex so read-time callers are not blocked unless they need a
	// config change)...
	for cname, version := range newClusterVersions {
		cConfig := dbConfig.Clusters[cname]
		db.clusters.update(newCluster(db.name, cConfig, version))
	}

	// ...and swap out the mappings.  NB(mmihic): We do this after updating
	// the clusters so that new clusters are available before anyone tries
	// to use them as a result of the updated mappings.
	db.Lock()
	db.version = dbConfig.Version
	db.mappings = newMappings
	db.Unlock()

	return nil
}

// sort.Interface for sorting ClusterMappingRuleSets by version order
type mappingRuleSetsByVersion []*schema.ClusterMappingRuleSet

func (m mappingRuleSetsByVersion) Len() int      { return len(m) }
func (m mappingRuleSetsByVersion) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
func (m mappingRuleSetsByVersion) Less(i, j int) bool {
	return m[i].ForVersion < m[j].ForVersion
}
