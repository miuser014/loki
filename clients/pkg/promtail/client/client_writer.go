package client

import (
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/loki/clients/pkg/promtail/api"
	"github.com/grafana/loki/clients/pkg/promtail/wal"
	"github.com/grafana/loki/pkg/util"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/tsdb/record"
	"sync"
)

// seriesCache implement a thread-safe cache of decoded series.
type seriesCache struct {
	cache  map[uint64]model.LabelSet
	rwLock sync.RWMutex
}

func newSeriesCache() *seriesCache {
	return &seriesCache{
		cache: map[uint64]model.LabelSet{},
	}
}

func (sc *seriesCache) Get(ref uint64) (model.LabelSet, bool) {
	sc.rwLock.RLock()
	defer sc.rwLock.RUnlock()
	set, ok := sc.cache[ref]
	return set, ok
}

func (sc *seriesCache) Set(ref uint64, set model.LabelSet) {
	sc.rwLock.Lock()
	defer sc.rwLock.Unlock()
	sc.cache[ref] = set
}

// clientWriteTo implements a wal.WriteTo that re-builds entries with the stored series, and the received entries. After,
// sends each to the provided Client channel.
type clientWriteTo struct {
	series   *seriesCache
	logger   log.Logger
	toClient chan<- api.Entry
}

// newClientWriteTo creates a new clientWriteTo
func newClientWriteTo(toClient chan<- api.Entry, cache *seriesCache, logger log.Logger) *clientWriteTo {
	return &clientWriteTo{
		series:   cache,
		toClient: toClient,
		logger:   logger,
	}
}

func (c *clientWriteTo) StoreSeries(series record.RefSeries) {
	c.series.Set(uint64(series.Ref), util.MapToModelLabelSet(series.Labels.Map()))
}

func (c *clientWriteTo) Append(entries wal.RefEntries) error {
	var entry api.Entry
	if l, ok := c.series.Get(uint64(entries.Ref)); ok {
		entry.Labels = l
		for _, e := range entries.Entries {
			entry.Entry = e
			c.toClient <- entry
		}
	} else {
		level.Debug(c.logger).Log("series for entry not found")
	}
	return nil
}
