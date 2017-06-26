package kapacitor

import (
	"errors"
	"fmt"
	"io/ioutil"
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

	statsKey string
	statMap  *expvar.Map
	logger   *log.Logger
}

func newEdge(taskName, parentName, childName string, t pipeline.EdgeType, size int, logService LogService) *Edge {
	e := edge.NewStatsEdge(edge.NewChannelEdge(t, defaultEdgeBufferSize))
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

	// collect serializes all calls to collect messages.
	collect sync.Mutex
	// next serializes all calls to get next messages.
	next sync.Mutex

	logger *log.Logger
}

func NewLegacyEdge(e edge.Edge) *LegacyEdge {
	var logger *log.Logger
	if el, ok := e.(*Edge); ok {
		logger = el.logger
	} else {
		// This should not be a possible branch,
		// as all edges passed to NewLegacyEdge are expected to be *Edge.
		logger = log.New(ioutil.Discard, "", 0)
	}
	return &LegacyEdge{
		e:      e,
		logger: logger,
	}
}

func NewLegacyEdges(edges []edge.StatsEdge) []*LegacyEdge {
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
	if e.e.Type() == pipeline.StreamEdge {
		return e.NextPoint()
	}
	return e.NextBatch()
}

func (e *LegacyEdge) NextPoint() (models.Point, bool) {
	e.next.Lock()
	defer e.next.Unlock()
	for m, ok := e.e.Emit(); ok; m, ok = e.e.Emit() {
		if t := m.Type(); t != edge.Point {
			e.logger.Printf("E! legacy edge expected message of type edge.PointMessage, got message of type %v", t)
			continue
		}
		p, ok := m.Value().(edge.PointMessage)
		if !ok {
			e.logger.Printf("E! unexpected message type %T", m)
			continue
		}
		return models.Point(p), true
	}
	return models.Point{}, false
}

func (e *LegacyEdge) NextBatch() (models.Batch, bool) {
	e.next.Lock()
	defer e.next.Unlock()
	b := models.Batch{}

BEGIN:
	for m, ok := e.e.Emit(); ok; m, ok = e.e.Emit() {
		switch msg := m.Value().(type) {
		case edge.BeginBatchMessage:
			b.Name = msg.Name
			b.Group = msg.Group
			b.Tags = msg.Tags
			b.ByName = msg.Dimensions.ByName
			b.Points = make([]models.BatchPoint, 0, msg.SizeHint)
			break BEGIN
		case edge.BufferedBatchMessage:
			b.Name = msg.Begin.Name
			b.Group = msg.Begin.Group
			b.Tags = msg.Begin.Tags
			b.ByName = msg.Begin.Dimensions.ByName
			b.TMax = msg.End.TMax
			b.Points = make([]models.BatchPoint, len(msg.Points))
			for i, bp := range msg.Points {
				b.Points[i] = models.BatchPoint{
					Time:   bp.Time,
					Fields: bp.Fields,
					Tags:   bp.Tags,
				}
			}
			return b, true
		default:
			e.logger.Printf("E! legacy edge expected message of type edge.BatchBegin or edge.BufferedBatch, got message of type %v", m.Type())
			continue BEGIN
		}
	}
	finished := false
MESSAGES:
	for m, ok := e.e.Emit(); ok; m, ok = e.e.Emit() {
		switch msg := m.Value().(type) {
		case edge.EndBatchMessage:
			b.TMax = msg.TMax
			finished = true
			break MESSAGES
		case edge.BatchPointMessage:
			b.Points = append(b.Points, models.BatchPoint{
				Time:   msg.Time,
				Fields: msg.Fields,
				Tags:   msg.Tags,
			})
		default:
			e.logger.Printf("E! legacy edge expected message of type edge.EndBatch or edge.BatchPoint, got message of type %v", m.Type())
			continue MESSAGES
		}
	}
	return b, finished
}

func (e *LegacyEdge) CollectPoint(p models.Point) error {
	e.collect.Lock()
	defer e.collect.Unlock()
	return e.e.Collect((edge.PointMessage)(p))
}

func (e *LegacyEdge) CollectBatch(b models.Batch) error {
	e.collect.Lock()
	defer e.collect.Unlock()
	if err := e.e.Collect(edge.BeginBatchMessage{
		Name:       b.Name,
		Group:      b.Group,
		Tags:       b.Tags,
		Dimensions: b.PointDimensions(),
		SizeHint:   len(b.Points),
	}); err != nil {
		return err
	}
	for _, bp := range b.Points {
		if err := e.e.Collect(edge.BatchPointMessage{
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
