package edge

import (
	"sync"

	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
)

// Stats for a given group for this edge
type Stats struct {
	Collected int64
	Emitted   int64
	Tags      models.Tags
	Dims      models.Dimensions
}

type StatsEdge struct {
	edge Edge

	Collected *expvar.Int
	Emitted   *expvar.Int

	mu         sync.RWMutex
	groupStats map[models.GroupID]*Stats
}

func (e *StatsEdge) Collect(m Message) error {
	err := e.edge.Collect(m)
	e.Collected.Add(1)
	// How do we handle stats by group for all messages
	// Do all messages have a group?
	//   No barrier messages apply to all groups.
	// My guess is we need to use an outside wrapper.
	e.incCollected(p.Group, p.Tags, p.Dimensions, 1)
	return err
}

func (e *StatsEdge) Next() (m Message, ok bool) {
	m, ok = e.edge.Next()
	if ok {
		e.Emitted.Add(1)
	}
	return
}

func (e *StatsEdge) incCollected(group models.GroupID, tags models.Tags, dims models.Dimensions) {
	// Manually unlock below as defer was too much of a performance hit
	e.mu.Lock()

	if stats, ok := e.groupStats[group]; ok {
		stats.collected += count
	} else {
		stats = &edgeStat{
			collected: count,
			tags:      tags,
			dims:      dims,
		}
		e.groupStats[group] = stats
	}
	e.mu.Unlock()
}
