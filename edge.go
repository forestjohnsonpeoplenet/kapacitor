package kapacitor

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/server/vars"
)

const (
	statCollected = "collected"
	statEmitted   = "emitted"

	defaultEdgeBufferSize = 1000
)

var ErrAborted = errors.New("edged aborted")

type StreamCollector interface {
	CollectPoint(models.Point) error
	Close() error
}

type BatchCollector interface {
	CollectBatch(models.Batch) error
	Close() error
}

type Edge struct {
	edge.StatsEdge

	mu     sync.Mutex
	closed bool

	typ pipeline.EdgeType

	statsKey string
	statMap  *expvar.Map
	logger   *log.Logger
}

func newEdge(taskName, parentName, childName string, t pipeline.EdgeType, size int, logService LogService) *Edge {
	var e edge.StatsEdge
	switch t {
	case pipeline.StreamEdge:
		e = edge.NewStreamStatsEdge(edge.NewChannelEdge(defaultEdgeBufferSize))
	case pipeline.BatchEdge:
		e = edge.NewBatchStatsEdge(edge.NewChannelEdge(defaultEdgeBufferSize))
	}
	tags := map[string]string{
		"task":   taskName,
		"parent": parentName,
		"child":  childName,
		"type":   t.String(),
	}
	key, sm := vars.NewStatistic("edges", tags)
	sm.Set(statCollected, e.CollectedVar())
	sm.Set(statEmitted, e.EmittedVar())
	name := fmt.Sprintf("%s|%s->%s", taskName, parentName, childName)
	return &Edge{
		StatsEdge: e,
		statsKey:  key,
		statMap:   sm,
		typ:       t,
		logger:    logService.NewLogger(fmt.Sprintf("[edge:%s] ", name), log.LstdFlags),
	}
}

func (e *Edge) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	vars.DeleteStatistic(e.statsKey)
	e.logger.Printf("D! closing c: %d e: %d",
		e.Collected(),
		e.Emitted(),
	)
	return e.StatsEdge.Close()
}

type LegacyEdge struct {
	e edge.Edge

	typ pipeline.EdgeType

	logger *log.Logger
}

func NewLegacyEdge(e *Edge) *LegacyEdge {
	return &LegacyEdge{
		e:      e,
		typ:    e.typ,
		logger: e.logger,
	}
}

func NewLegacyEdges(edges []*Edge) []*LegacyEdge {
	legacyEdges := make([]*LegacyEdge, len(edges))
	for i := range edges {
		legacyEdges[i] = NewLegacyEdge(edges[i])
	}
	return legacyEdges
}

func (e *LegacyEdge) Close() error {
	return e.e.Close()
}

// Abort all next and collect calls.
// Items in flight may or may not be processed.
func (e *LegacyEdge) Abort() {
	e.e.Abort()
}

func (e *LegacyEdge) Next() (p models.PointInterface, ok bool) {
	if e.typ == pipeline.StreamEdge {
		return e.NextPoint()
	}
	return e.NextBatch()
}

func (e *LegacyEdge) NextPoint() (models.Point, bool) {
	for m, ok := e.e.Next(); ok; m, ok = e.e.Next() {
		if t := m.Type(); t != edge.Point {
			e.logger.Printf("E! legacy edge does not support edge message of type %v", t)
			continue
		}
		p, ok := m.(edge.PointMessage)
		if !ok {
			e.logger.Printf("E! unexpected message type %T", m)
			continue
		}
		return models.Point(p), true
	}
	return models.Point{}, false
}

func (e *LegacyEdge) NextBatch() (models.Batch, bool) {
	b := models.Batch{}
	for m, ok := e.e.Next(); ok; m, ok = e.e.Next() {
		if t := m.Type(); t != edge.BeginBatch {
			e.logger.Printf("E! legacy edge does not support edge message of type %v", t)
			continue
		}
		begin := m.(edge.BeginBatchMessage)
		b.Name = begin.Name
		b.Group = begin.Group
		b.Tags = begin.Tags
		b.ByName = begin.Dimensions.ByName
		b.Points = make([]models.BatchPoint, 0, begin.SizeHint)
		break
	}
	finished := false
	for m, ok := e.e.Next(); ok; m, ok = e.e.Next() {
		t := m.Type()
		if t == edge.EndBatch {
			end := m.(edge.EndBatchMessage)
			b.TMax = end.TMax
			finished = true
			break
		}
		if t := m.Type(); t != edge.Point {
			e.logger.Printf("E! legacy edge does not support edge message of type %v", t)
			continue
		}
		p := m.(edge.PointMessage)
		b.Points = append(b.Points, models.BatchPoint{
			Time:   p.Time,
			Fields: p.Fields,
			Tags:   p.Tags,
		})
	}
	return b, finished
}

func (e *LegacyEdge) CollectPoint(p models.Point) error {
	return e.e.Collect((edge.PointMessage)(p))
}

func (e *LegacyEdge) CollectBatch(b models.Batch) error {
	if err := e.e.Collect(edge.BeginBatchMessage{Name: b.Name,
		Group:      b.Group,
		Tags:       b.Tags,
		Dimensions: b.PointDimensions(),
		SizeHint:   len(b.Points),
	}); err != nil {
		return err
	}
	for _, bp := range b.Points {
		if err := e.e.Collect(edge.PointMessage{
			Time:   bp.Time,
			Fields: bp.Fields,
			Tags:   bp.Tags,
		}); err != nil {
			return err
		}
	}
	return e.e.Collect(edge.EndBatchMessage{
		TMax: b.TMax,
	})
}
