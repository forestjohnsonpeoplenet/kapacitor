package kapacitor

import (
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/pkg/errors"
)

// tmpl -- go get github.com/benbjohnson/tmpl
//go:generate tmpl -data=@tmpldata.json influxql.gen.go.tmpl

type createReduceContextFunc func(c baseReduceContext) reduceContext

type InfluxQLNode struct {
	node
	n                      *pipeline.InfluxQLNode
	createFn               createReduceContextFunc
	isStreamTransformation bool

	currentKind reflect.Kind
}

func newInfluxQLNode(et *ExecutingTask, n *pipeline.InfluxQLNode, l *log.Logger) (*InfluxQLNode, error) {
	m := &InfluxQLNode{
		node: node{Node: n, et: et, logger: l},
		n:    n,
		isStreamTransformation: n.ReduceCreater.IsStreamTransformation,
	}
	m.node.runF = m.runInfluxQL
	return m, nil
}

// TODO: remove bulk interfaces
type reduceContext interface {
	AggregatePoint(name string, p edge.FieldsTagsTimeGetter) error
	EmitPoint() (edge.PointMessage, error)
	EmitBatch() edge.BufferedBatchMessage
}

type baseReduceContext struct {
	as            string
	field         string
	name          string
	groupInfo     edge.GroupInfo
	time          time.Time
	pointTimes    bool
	topBottomInfo *pipeline.TopBottomCallInfo
}

func (n *InfluxQLNode) runInfluxQL([]byte) error {
	consumer := edge.NewGroupedConsumer(
		n.ins[0],
		n,
	)
	n.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())
	return consumer.Consume()
}

func (n *InfluxQLNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	return edge.NewReceiverFromForwardReceiverWithStats(
		n.outs,
		edge.NewTimedForwardReceiver(n.timer, n.newGroup(first)),
	), nil
}

func (n *InfluxQLNode) newGroup(first edge.PointMeta) edge.ForwardReceiver {
	bc := baseReduceContext{
		as:         n.n.As,
		field:      n.n.Field,
		name:       first.Name(),
		groupInfo:  first.GroupInfo(),
		time:       first.Time(),
		pointTimes: n.n.PointTimes || n.isStreamTransformation,
	}
	g := influxqlGroup{
		n:  n,
		bc: bc,
	}
	if n.isStreamTransformation {
		return &influxqlStreamingTransformGroup{
			influxqlGroup: g,
		}
	}
	return &g
}

func (n *InfluxQLNode) DeleteGroup(group models.GroupID) {
}

type influxqlGroup struct {
	n *InfluxQLNode

	bc baseReduceContext
	rc reduceContext

	batchSize int
	name      string
	begin     edge.BeginBatchMessage
}

func (g *influxqlGroup) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	g.begin = begin
	g.batchSize = 0
	g.rc = nil
	return nil, nil
}

func (g *influxqlGroup) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	if g.rc == nil {
		if err := g.realizeReduceContextFromFields(bp.Fields()); err != nil {
			g.n.incrementErrorCount()
			g.n.logger.Println("E!", err)
			return nil, nil
		}
	}
	g.batchSize++
	if err := g.rc.AggregatePoint(g.begin.Name(), bp); err != nil {
		g.n.incrementErrorCount()
		g.n.logger.Println("E! failed to aggregate point in batch:", err)
	}
	return nil, nil
}

func (g *influxqlGroup) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	if g.batchSize == 0 && !g.n.n.ReduceCreater.IsEmptyOK {
		// Do not call Emit on the reducer since it can't handle empty batches.
		return nil, nil
	}
	if g.rc == nil {
		// Assume float64 type since we do not have any data.
		if err := g.realizeReduceContext(reflect.Float64); err != nil {
			return nil, err
		}
	}
	m, err := g.n.emit(g.rc)
	if err != nil {
		g.n.incrementErrorCount()
		g.n.logger.Println("E! failed to emit batch:", err)
		return nil, nil
	}
	return m, nil
}

func (g *influxqlGroup) Point(p edge.PointMessage) (edge.Message, error) {
	g.n.logger.Println("D! Point:", p.Time(), g.bc.time)
	if p.Time().Equal(g.bc.time) {
		g.aggregatePoint(p)
	} else {
		g.n.logger.Println("D!, time elapsed resetting context")
		// Time has elapsed, emit current context
		var msg edge.Message
		if g.rc != nil {
			m, err := g.n.emit(g.rc)
			if err != nil {
				g.n.incrementErrorCount()
				g.n.logger.Println("E! failed to emit stream:", err)
			}
			msg = m
		}

		// Reset context
		g.bc.name = p.Name()
		g.bc.time = p.Time()
		g.rc = nil

		// Aggregate the current point
		g.aggregatePoint(p)

		return msg, nil
	}
	return nil, nil
}

func (g *influxqlGroup) aggregatePoint(p edge.PointMessage) {
	g.n.logger.Println("D! aggregatePoint", p)
	if g.rc == nil {
		if err := g.realizeReduceContextFromFields(p.Fields()); err != nil {
			g.n.incrementErrorCount()
			g.n.logger.Println("E!", err)
		}
	}
	err := g.rc.AggregatePoint(p.Name(), p)
	if err != nil {
		g.n.incrementErrorCount()
		g.n.logger.Println("E! failed to aggregate point:", err)
	}
}

func (g *influxqlGroup) getFieldKind(fields models.Fields) (reflect.Kind, error) {
	f, exists := fields[g.bc.field]
	if !exists {
		return reflect.Invalid, fmt.Errorf("field %q missing from point", g.bc.field)
	}

	return reflect.TypeOf(f).Kind(), nil
}
func (g *influxqlGroup) realizeReduceContextFromFields(fields models.Fields) error {
	k, err := g.getFieldKind(fields)
	if err != nil {
		return err
	}
	return g.realizeReduceContext(k)
}

func (g *influxqlGroup) realizeReduceContext(kind reflect.Kind) error {
	createFn, err := g.n.getCreateFn(kind)
	if err != nil {
		return err
	}
	g.rc = createFn(g.bc)
	return nil
}

func (g *influxqlGroup) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}

type influxqlStreamingTransformGroup struct {
	influxqlGroup
}

func (g *influxqlStreamingTransformGroup) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	g.begin = begin.ShallowCopy()
	g.begin.SetSizeHint(0)
	g.batchSize = 0
	g.rc = nil
	return begin, nil
}

func (g *influxqlStreamingTransformGroup) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	if g.rc == nil {
		if err := g.realizeReduceContextFromFields(bp.Fields()); err != nil {
			g.n.incrementErrorCount()
			g.n.logger.Println("E!", err)
			return nil, nil
		}
	}
	g.batchSize++
	if err := g.rc.AggregatePoint(g.begin.Name(), bp); err != nil {
		g.n.incrementErrorCount()
		g.n.logger.Println("E! failed to aggregate batch point:", err)
	}
	if ep, err := g.rc.EmitPoint(); err != nil {
		g.n.incrementErrorCount()
		g.n.logger.Println("E! failed to emit batch point:", err)
	} else {
		return edge.NewBatchPointMessage(
			ep.Fields(),
			ep.Tags(),
			ep.Time(),
		), nil
	}
	return nil, nil
}

func (g *influxqlStreamingTransformGroup) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	if g.batchSize == 0 && !g.n.n.ReduceCreater.IsEmptyOK {
		// End the batch now.
		// Do not call Emit on the reducer since it can't handle empty batches.
		return end, nil
	}
	return g.influxqlGroup.EndBatch(end)
}

func (g *influxqlStreamingTransformGroup) Point(p edge.PointMessage) (edge.Message, error) {
	if g.rc == nil {
		if err := g.realizeReduceContextFromFields(p.Fields()); err != nil {
			g.n.incrementErrorCount()
			g.n.logger.Println("E!", err)
			// Skip point
			return nil, nil
		}
	}
	err := g.rc.AggregatePoint(p.Name(), p)
	if err != nil {
		g.n.incrementErrorCount()
		g.n.logger.Println("E! failed to aggregate point:", err)
	}

	m, err := g.n.emit(g.rc)
	if err != nil {
		g.n.incrementErrorCount()
		g.n.logger.Println("E! failed to emit stream:", err)
		return nil, nil
	}
	return m, nil
}

func (g *influxqlStreamingTransformGroup) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}

//func (n *InfluxQLNode) asdf() error {
//	var mu sync.RWMutex
//	contexts := make(map[models.GroupID]reduceContext)
//	valueF := func() int64 {
//		mu.RLock()
//		l := len(contexts)
//		mu.RUnlock()
//		return int64(l)
//	}
//	n.statMap.Set(statCardinalityGauge, expvar.NewIntFuncGauge(valueF))
//
//	var kind reflect.Kind
//	for p, ok := n.legacyIns[0].NextPoint(); ok; {
//		n.timer.Start()
//		mu.RLock()
//		context := contexts[p.Group]
//		mu.RUnlock()
//		// First point in window
//		if context == nil {
//			// Create new context
//			c := baseReduceContext{
//				as:         n.n.As,
//				field:      n.n.Field,
//				name:       p.Name,
//				group:      p.Group,
//				dimensions: p.Dimensions,
//				tags:       p.PointTags(),
//				time:       p.Time,
//				pointTimes: n.n.PointTimes || n.isStreamTransformation,
//			}
//
//			f, exists := p.Fields[c.field]
//			if !exists {
//				n.incrementErrorCount()
//				n.logger.Printf("E! field %s missing from point, skipping point", c.field)
//				p, ok = n.legacyIns[0].NextPoint()
//				n.timer.Stop()
//				continue
//			}
//
//			k := reflect.TypeOf(f).Kind()
//			kindChanged := k != kind
//			kind = k
//
//			createFn, err := n.getCreateFn(kindChanged, kind)
//			if err != nil {
//				return err
//			}
//
//			context = createFn(c)
//			mu.Lock()
//			contexts[p.Group] = context
//			mu.Unlock()
//
//		}
//		if n.isStreamTransformation {
//			err := context.AggregatePoint(&p)
//			if err != nil {
//				n.incrementErrorCount()
//				n.logger.Println("E! failed to aggregate point:", err)
//			}
//			p, ok = n.legacyIns[0].NextPoint()
//
//			err = n.emit(context)
//			if err != nil && err != ErrEmptyEmit {
//				n.incrementErrorCount()
//				n.logger.Println("E! failed to emit stream:", err)
//			}
//		} else {
//			if p.Time.Equal(context.Time()) {
//				err := context.AggregatePoint(&p)
//				if err != nil {
//					n.incrementErrorCount()
//					n.logger.Println("E! failed to aggregate point:", err)
//				}
//				// advance to next point
//				p, ok = n.legacyIns[0].NextPoint()
//			} else {
//				err := n.emit(context)
//				if err != nil {
//					n.incrementErrorCount()
//					n.logger.Println("E! failed to emit stream:", err)
//				}
//
//				// Nil out reduced point
//				mu.Lock()
//				contexts[p.Group] = nil
//				mu.Unlock()
//				// do not advance,
//				// go through loop again to initialize new iterator.
//			}
//		}
//		n.timer.Stop()
//	}
//	return nil
//}

//func (n *InfluxQLNode) runBatchInfluxQL() error {
//	var kind reflect.Kind
//	kindChanged := true
//	for b, ok := n.legacyIns[0].NextBatch(); ok; b, ok = n.legacyIns[0].NextBatch() {
//		n.timer.Start()
//		// Create new base context
//		c := baseReduceContext{
//			as:         n.n.As,
//			field:      n.n.Field,
//			name:       b.Name,
//			group:      b.Group,
//			dimensions: b.PointDimensions(),
//			tags:       b.Tags,
//			time:       b.TMax,
//			pointTimes: n.n.PointTimes || n.isStreamTransformation,
//		}
//		if len(b.Points) == 0 {
//			if !n.n.ReduceCreater.IsEmptyOK {
//				// If the reduce does not handle empty batches continue
//				n.timer.Stop()
//				continue
//			}
//			if kind == reflect.Invalid {
//				// If we have no points and have never seen a point assume float64
//				kind = reflect.Float64
//			}
//		} else {
//			f, ok := b.Points[0].Fields[c.field]
//			if !ok {
//				n.incrementErrorCount()
//				n.logger.Printf("E! field %s missing from point, skipping batch", c.field)
//				n.timer.Stop()
//				continue
//			}
//			k := reflect.TypeOf(f).Kind()
//			kindChanged = k != kind
//			kind = k
//		}
//		createFn, err := n.getCreateFn(kindChanged, kind)
//		if err != nil {
//			return err
//		}
//
//		context := createFn(c)
//		if n.isStreamTransformation {
//			// We have a stream transformation, so treat the batch as if it were a stream
//			// Create a new batch for emitting
//			eb := b
//			eb.Points = make([]models.BatchPoint, 0, len(b.Points))
//			for _, bp := range b.Points {
//				p := models.Point{
//					Name:   b.Name,
//					Time:   bp.Time,
//					Fields: bp.Fields,
//					Tags:   bp.Tags,
//				}
//				if err := context.AggregatePoint(&p); err != nil {
//					n.incrementErrorCount()
//					n.logger.Println("E! failed to aggregate batch point:", err)
//				}
//				if ep, err := context.EmitPoint(); err != nil && err != ErrEmptyEmit {
//					n.incrementErrorCount()
//					n.logger.Println("E! failed to emit batch point:", err)
//				} else if err != ErrEmptyEmit {
//					eb.Points = append(eb.Points, models.BatchPoint{
//						Time:   ep.Time,
//						Fields: ep.Fields,
//						Tags:   ep.Tags,
//					})
//				}
//			}
//			// Emit the complete batch
//			n.timer.Pause()
//			for _, out := range n.legacyOuts {
//				if err := out.CollectBatch(eb); err != nil {
//					n.incrementErrorCount()
//					n.logger.Println("E! failed to emit batch points:", err)
//				}
//			}
//			n.timer.Resume()
//		} else {
//			err := context.AggregateBatch(&b)
//			if err == nil {
//				if err := n.emit(context); err != nil {
//					n.incrementErrorCount()
//					n.logger.Println("E! failed to emit batch:", err)
//				}
//			} else {
//				n.incrementErrorCount()
//				n.logger.Println("E! failed to aggregate batch:", err)
//			}
//		}
//		n.timer.Stop()
//	}
//	return nil
//}

func (n *InfluxQLNode) getCreateFn(kind reflect.Kind) (createReduceContextFunc, error) {
	changed := n.currentKind != kind
	if !changed && n.createFn != nil {
		return n.createFn, nil
	}
	n.currentKind = kind
	createFn, err := determineReduceContextCreateFn(n.n.Method, kind, n.n.ReduceCreater)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid influxql func %s with field %s", n.n.Method, n.n.Field)
	}
	n.createFn = createFn
	return n.createFn, nil
}

func (n *InfluxQLNode) emit(context reduceContext) (edge.Message, error) {
	switch n.Provides() {
	case pipeline.StreamEdge:
		return context.EmitPoint()
	case pipeline.BatchEdge:
		return context.EmitBatch(), nil
	}
	return nil, nil
}
