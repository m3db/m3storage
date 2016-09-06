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

package mapping

import (
	"sort"
	"sync"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3storage/cluster"
	"github.com/m3db/m3storage/generated/proto/schema"
	"github.com/m3db/m3x/close"
	"github.com/m3db/m3x/log"

	"github.com/facebookgo/clock"
)

// A RuleIter is an iterator over Rules.  Allows providers to control how these
// are stored internally
type RuleIter interface {
	xclose.Closer

	// Next moves to the next mapping, returning false if there are no more
	// mappings
	Next() bool

	// Current returns the current mapping
	Current() Rule
}

// A Provider provides shard mapping rules
type Provider interface {
	xclose.Closer

	// FetchRules returns the active cluster mappings for fetching from a given
	// shard
	FetchRules(shard uint32, start, end time.Time) (RuleIter, error)
}

// ProviderOptions are options to the provider
type ProviderOptions struct {
	Logger          xlog.Logger
	Clock           clock.Clock
	ClusterUpdateCh chan<- cluster.Cluster
}

// NewProvider creates a new mapping provider
func NewProvider(key string, kvStore kv.Store, opts *ProviderOptions) (Provider, error) {
	if opts == nil {
		opts = &ProviderOptions{}
	}

	log := opts.Logger
	if log == nil {
		log = xlog.NullLogger
	}

	c := opts.Clock
	if c == nil {
		c = clock.New()
	}

	w, err := kvStore.Watch(key)
	if err != nil {
		return nil, err
	}

	p := &provider{
		byName:          make(map[string]*databaseRules),
		w:               w,
		log:             log,
		clock:           c,
		done:            make(chan struct{}),
		clusterUpdateCh: opts.ClusterUpdateCh,
	}

	go p.watchPlacementChanges()
	return p, nil
}

type provider struct {
	sync.RWMutex
	byName      map[string]*databaseRules
	byRetention []*databaseRules

	w               kv.ValueWatch
	clock           clock.Clock
	log             xlog.Logger
	done            chan struct{}
	clusterUpdateCh chan<- cluster.Cluster
}

// FetchRules returns an iterator over the rules for fetching from a given shard
func (p *provider) FetchRules(shard uint32, start, end time.Time) (RuleIter, error) {
	// Figure out how far back this query goes
	queryAgeInSecs := int32(p.clock.Now().Sub(end) / time.Second)

	// Figure out which database mapping to use given the age of the datapoints being queried
	db := p.findDatabaseRules(queryAgeInSecs)
	if db == nil {
		// No database mappings - return an empty iterator
		return &ruleIter{}, nil
	}

	// Figure out which active mapping contains that shard
	return &ruleIter{
		prior: db.findRulesForShard(uint(shard)),
	}, nil
}

// Close closes the provider
func (p *provider) Close() error {
	p.w.Close()
	<-p.done
	return nil
}

// findDatabaseRules finds the rules that apply to queries at a certain range
func (p *provider) findDatabaseRules(queryAgeInSecs int32) *databaseRules {
	p.RLock()
	defer p.RUnlock()

	if len(p.byRetention) == 0 {
		return nil
	}

	for _, db := range p.byRetention {
		if queryAgeInSecs <= db.maxRetentionInSecs {
			return db
		}
	}

	// If the queryAge falls outside all database retention periods, use the one
	// with the longest retention
	return p.byRetention[len(p.byRetention)-1]
}

// watchPlacementChanges is a background goroutine that watches for placement changes
func (p *provider) watchPlacementChanges() {
	var placement schema.Placement
	for range p.w.C() {
		val := p.w.Get()
		if err := val.Unmarshal(&placement); err != nil {
			p.log.Errorf("could not unmarshal placement data: %v", err)
			continue
		}

		p.log.Infof("received placement version %d", val.Version())
		p.update(&placement)
	}
	close(p.done)
}

// update updates the current rules based on a given placement
func (p *provider) update(pl *schema.Placement) error {
	// Avoid the need for upstream code to synchronize on the database rules
	// by first making a copy, then updating the copy in place,
	// then swapping out the pointers atomically
	p.RLock()
	byName := make(map[string]*databaseRules, len(p.byName))
	byRetention := make([]*databaseRules, len(p.byRetention))
	for n, db := range p.byRetention {
		byRetention[n], byName[db.name] = db, db
	}
	p.RUnlock()

	// Update the clone with the new settings
	for _, dbConfig := range pl.Databases {
		if existing := byName[dbConfig.Properties.Name]; existing != nil {
			// first apply cluster updates...
			p.applyClusterUpdates(dbConfig, existing.version)

			// ...then apply shard transitions
			if err := existing.applyNewTransitions(dbConfig.Version, dbConfig.MappingRules); err != nil {
				return err
			}
			continue
		}

		// This is a new database - create and apply all rules...
		db, err := newDatabaseRules(dbConfig, p.log)
		if err != nil {
			return err
		}

		// ...report all clusters...
		p.applyClusterUpdates(dbConfig, -1)

		// ...and add to the list of databases we are tracking
		byName[dbConfig.Properties.Name] = db
		byRetention = append(byRetention, db)
	}

	sort.Sort(databaseRulesByMaxRetention(byRetention))

	// Swap out the rules pointers so that callers can resolve new mappings
	p.Lock()
	p.byRetention, p.byName = byRetention, byName
	p.Unlock()
	return nil
}

// applyClusterUpdates sends cluster configuration updates to upstream listeners
func (p *provider) applyClusterUpdates(dbConfig *schema.Database, fromVersion int32) {
	if p.clusterUpdateCh == nil {
		// No-one is interested in updates
		return
	}

	// Calculate the latest version of all of the cluster configurations
	newClusterVersions := make(map[string]int32)
	for _, r := range dbConfig.MappingRules {
		if r.ForVersion < fromVersion {
			continue
		}

		for _, updated := range r.ClusterConfigUpdates {
			newClusterVersions[updated.ClusterName] = r.ForVersion
		}

		for _, joined := range r.ClusterJoins {
			newClusterVersions[joined.ClusterName] = r.ForVersion
		}
	}

	// Send new cluster configuration to subscribers
	for cname, version := range newClusterVersions {
		// NB(mmihic): This intentionally blocks - we don't want to miss cluster updates, and
		// will hold off processing more rules until the latest updates are delivered
		c := dbConfig.Clusters[cname]
		p.clusterUpdateCh <- cluster.NewCluster(
			c.Properties.Name,
			cluster.NewType(c.Properties.Type),
			cluster.NewConfig(int(version), c.Config),
			dbConfig.Properties.Name)
	}
}

// sort.Interface for sorting database rules by retention period
type databaseRulesByMaxRetention []*databaseRules

func (dbs databaseRulesByMaxRetention) Len() int      { return len(dbs) }
func (dbs databaseRulesByMaxRetention) Swap(i, j int) { dbs[i], dbs[j] = dbs[j], dbs[i] }
func (dbs databaseRulesByMaxRetention) Less(i, j int) bool {
	return dbs[i].maxRetentionInSecs < dbs[j].maxRetentionInSecs
}
