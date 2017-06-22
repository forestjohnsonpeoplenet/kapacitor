package edge

import (
	"expvar"
	"sync"

	kexpvar "github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

type GroupInfo struct {
	Group models.GroupID
	Tags  models.Tags
	Dims  models.Dimensions
}

type GroupStats struct {
	Collected int64
	Emitted   int64
	GroupInfo GroupInfo
}

type StatsEdge interface {
	Edge
	Collected() int64
	Emitted() int64
	CollectedVar() expvar.Var
	EmittedVar() expvar.Var
	ReadGroupStats(func(*GroupStats))
}

type statsEdge struct {
	edge Edge

	collected *kexpvar.Int
	emitted   *kexpvar.Int

	mu         sync.RWMutex
	groupStats map[models.GroupID]*GroupStats
}

func (e *statsEdge) Collected() int64 {
	return e.collected.IntValue()
}
func (e *statsEdge) Emitted() int64 {
	return e.emitted.IntValue()
}

func (e *statsEdge) CollectedVar() expvar.Var {
	return e.collected
}
func (e *statsEdge) EmittedVar() expvar.Var {
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

func NewStatsEdge(e Edge) StatsEdge {
	switch e.Type() {
	case pipeline.StreamEdge:
		return NewStreamStatsEdge(e)
	case pipeline.BatchEdge:
		return NewBatchStatsEdge(e)
	}
	return nil
}

type BatchStatsEdge struct {
	statsEdge

	currentGroup GroupInfo
	size         int64
}

func NewBatchStatsEdge(e Edge) *BatchStatsEdge {
	return &BatchStatsEdge{
		statsEdge: statsEdge{
			edge:       e,
			groupStats: make(map[models.GroupID]*GroupStats),
			collected:  new(kexpvar.Int),
			emitted:    new(kexpvar.Int),
		},
	}
}

func (e *BatchStatsEdge) Collect(m Message) error {
	if err := e.edge.Collect(m); err != nil {
		return err
	}
	switch m.Type() {
	case BeginBatch:
		g := m.(BeginBatchMessage).GroupInfo()
		e.currentGroup = g
		e.size = 0
	case Point:
		e.size++
	case EndBatch:
		e.collected.Add(1)
		e.incCollected(e.currentGroup, e.size)
	}
	return nil
}

func (e *BatchStatsEdge) Next() (m Message, ok bool) {
	m, ok = e.edge.Next()
	if ok && m.Type() == EndBatch {
		e.emitted.Add(1)
	}
	return
}

func (e *BatchStatsEdge) Type() pipeline.EdgeType {
	return e.edge.Type()
}

type StreamStatsEdge struct {
	statsEdge
}

func NewStreamStatsEdge(e Edge) *StreamStatsEdge {
	return &StreamStatsEdge{
		statsEdge: statsEdge{
			edge:       e,
			groupStats: make(map[models.GroupID]*GroupStats),
			collected:  new(kexpvar.Int),
			emitted:    new(kexpvar.Int),
		},
	}
}

func (e *StreamStatsEdge) Collect(m Message) error {
	if err := e.edge.Collect(m); err != nil {
		return err
	}
	if m.Type() == Point {
		e.collected.Add(1)
		e.incCollected(m.(PointMessage).GroupInfo(), 1)
	}
	return nil
}

func (e *StreamStatsEdge) Next() (m Message, ok bool) {
	m, ok = e.edge.Next()
	if ok && m.Type() == Point {
		e.emitted.Add(1)
	}
	return
}

func (e *StreamStatsEdge) Type() pipeline.EdgeType {
	return e.edge.Type()
}
