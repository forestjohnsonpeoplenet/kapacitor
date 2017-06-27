package edge

import (
	"sync"

	expvar "github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

// StatsEdge is an edge that tracks various statistics about message passing through the edge.
type StatsEdge interface {
	Edge
	// Collected returns the number of messages collected by this edge.
	Collected() int64
	// Emitted returns the number of messages emitted by this edge.
	Emitted() int64
	// CollectedVar is an exported var the represents the number of messages collected by this edge.
	CollectedVar() expvar.IntVar
	// EmittedVar is an exported var the represents the number of messages emitted by this edge.
	EmittedVar() expvar.IntVar
	// ReadGroupStats allows for the reading of the current statistics by group.
	ReadGroupStats(func(*GroupStats))
}

//  GroupStats represents the statistics for a specific group.
type GroupStats struct {
	GroupInfo GroupInfo
	Collected int64
	Emitted   int64
}

// NewStatsEdge creates an edge that tracks statistics about the message passing through the edge.
func NewStatsEdge(e Edge) StatsEdge {
	switch e.Type() {
	case pipeline.StreamEdge:
		return &streamStatsEdge{
			statsEdge: statsEdge{
				edge:       e,
				groupStats: make(map[models.GroupID]*GroupStats),
				collected:  new(expvar.Int),
				emitted:    new(expvar.Int),
			},
		}
	case pipeline.BatchEdge:
		return &batchStatsEdge{
			statsEdge: statsEdge{
				edge:       e,
				groupStats: make(map[models.GroupID]*GroupStats),
				collected:  new(expvar.Int),
				emitted:    new(expvar.Int),
			},
		}
	}
	return nil
}

type statsEdge struct {
	edge Edge

	collected *expvar.Int
	emitted   *expvar.Int

	mu         sync.RWMutex
	groupStats map[models.GroupID]*GroupStats
}

func (e *statsEdge) Collected() int64 {
	return e.collected.IntValue()
}
func (e *statsEdge) Emitted() int64 {
	return e.emitted.IntValue()
}

func (e *statsEdge) CollectedVar() expvar.IntVar {
	return e.collected
}
func (e *statsEdge) EmittedVar() expvar.IntVar {
	return e.emitted
}

func (e *statsEdge) Close() error {
	return e.edge.Close()
}
func (e *statsEdge) Abort() {
	e.edge.Abort()
}

// ReadGroupStats calls f for each of the group stats.
func (e *statsEdge) ReadGroupStats(f func(groupStat *GroupStats)) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, stats := range e.groupStats {
		f(stats)
	}
}

func (e *statsEdge) incCollected(info GroupInfo, count int64) {
	// Manually unlock below as defer was too much of a performance hit
	e.mu.Lock()

	if stats, ok := e.groupStats[info.Group]; ok {
		stats.Collected += count
	} else {
		stats = &GroupStats{
			Collected: count,
			GroupInfo: info,
		}
		e.groupStats[info.Group] = stats
	}
	e.mu.Unlock()
}

// Increment the emitted count of the group for this edge.
func (e *statsEdge) incEmitted(info GroupInfo, count int64) {
	// Manually unlock below as defer was too much of a performance hit
	e.mu.Lock()

	if stats, ok := e.groupStats[info.Group]; ok {
		stats.Emitted += count
	} else {
		stats = &GroupStats{
			Emitted:   count,
			GroupInfo: info,
		}
		e.groupStats[info.Group] = stats
	}
	e.mu.Unlock()
}

type batchStatsEdge struct {
	statsEdge

	currentGroup GroupInfo
	size         int64
}

func (e *batchStatsEdge) Collect(m Message) error {
	if err := e.edge.Collect(m); err != nil {
		return err
	}
	switch b := m.Value().(type) {
	case BeginBatchMessage:
		g := b.GroupInfo()
		e.currentGroup = g
		e.size = 0
	case PointMessage:
		e.size++
	case EndBatchMessage:
		e.collected.Add(1)
		e.incCollected(e.currentGroup, e.size)
	case BufferedBatchMessage:
		e.collected.Add(1)
		e.incCollected(b.Begin.GroupInfo(), int64(len(b.Points)))
	default:
		// Do not count other messages
		// TODO(nathanielc): How should we count other messages?
	}
	return nil
}

func (e *batchStatsEdge) Emit() (m Message, ok bool) {
	m, ok = e.edge.Emit()
	if ok && m.Type() == EndBatch {
		e.emitted.Add(1)
	}
	return
}

func (e *batchStatsEdge) Type() pipeline.EdgeType {
	return e.edge.Type()
}

type streamStatsEdge struct {
	statsEdge
}

func (e *streamStatsEdge) Collect(m Message) error {
	if err := e.edge.Collect(m); err != nil {
		return err
	}
	if m.Type() == Point {
		e.collected.Add(1)
		e.incCollected(m.Value().(PointMessage).GroupInfo(), 1)
	}
	return nil
}

func (e *streamStatsEdge) Emit() (m Message, ok bool) {
	m, ok = e.edge.Emit()
	if ok && m.Type() == Point {
		e.emitted.Add(1)
	}
	return
}

func (e *streamStatsEdge) Type() pipeline.EdgeType {
	return e.edge.Type()
}
