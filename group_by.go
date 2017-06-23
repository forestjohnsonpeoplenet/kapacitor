package kapacitor

import (
	"log"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

type GroupByNode struct {
	node
	g             *pipeline.GroupByNode
	dimensions    []string
	allDimensions bool

	dims models.Dimensions

	mu       sync.RWMutex
	lastTime time.Time
	groups   map[models.GroupID]*edge.BufferedBatch
}

// Create a new GroupByNode which splits the stream dynamically based on the specified dimensions.
func newGroupByNode(et *ExecutingTask, n *pipeline.GroupByNode, l *log.Logger) (*GroupByNode, error) {
	gn := &GroupByNode{
		node:   node{Node: n, et: et, logger: l},
		g:      n,
		groups: make(map[models.GroupID]*edge.BufferedBatch),
	}
	gn.node.runF = gn.runGroupBy

	gn.allDimensions, gn.dimensions = determineDimensions(n.Dimensions)
	gn.dims = models.Dimensions{
		ByName:   n.ByMeasurementFlag,
		TagNames: gn.dimensions,
	}
	return gn, nil
}

func (g *GroupByNode) runGroupBy([]byte) error {
	valueF := func() int64 {
		g.mu.RLock()
		l := len(g.groups)
		g.mu.RUnlock()
		return int64(l)
	}
	g.statMap.Set(statCardinalityGauge, expvar.NewIntFuncGauge(valueF))

	consumer := edge.NewConsumer(
		g.ins[0],
		edge.NewBufferingReceiver(g),
	)
	return consumer.Run()
}

func (g *GroupByNode) Point(p edge.PointMessage) error {
	g.timer.Start()
	p = edge.PointMessage(setGroupOnPoint(models.Point(p), g.allDimensions, g.dims, g.g.ExcludedDimensions))
	g.timer.Stop()
	for _, out := range g.outs {
		if err := out.Collect(p); err != nil {
			return err
		}
	}
	return nil
}

func (g *GroupByNode) Batch(batch edge.BufferedBatch) error {
	g.timer.Start()
	defer g.timer.Stop()

	g.emit(batch.End.TMax)

	for _, bp := range batch.Points {
		if g.allDimensions {
			g.dims.TagNames = filterExcludedDimensions(bp.Tags, g.dims, g.g.ExcludedDimensions)
		} else {
			g.dims.TagNames = g.dimensions
		}
		groupID := models.ToGroupID(batch.Begin.Name, bp.Tags, g.dims)
		group, ok := g.groups[groupID]
		if !ok {
			// Create new begin message
			newBegin := batch.Begin
			newBegin.Group = groupID
			// Create new tags
			tags := make(map[string]string, len(g.dims.TagNames))
			for _, dim := range g.dims.TagNames {
				tags[dim] = bp.Tags[dim]
			}
			newBegin.Tags = tags

			// Create buffer for group batch
			group = &edge.BufferedBatch{
				Begin:  newBegin,
				Points: make([]edge.BatchPointMessage, 0, batch.Begin.SizeHint),
				End:    batch.End,
			}
			g.mu.Lock()
			g.groups[groupID] = group
			g.mu.Unlock()
		}
		group.Points = append(group.Points, bp)
	}
	return nil
}

func (g *GroupByNode) Barrier(b edge.BarrierMessage) error {
	g.timer.Start()
	err := g.emit(b.Time)
	g.timer.Stop()
	return err
}

// emit sends all groups before time t to children nodes.
// The node timer must be started when calling this method.
func (g *GroupByNode) emit(t time.Time) error {
	if !t.Equal(g.lastTime) {
		g.lastTime = t
		// Emit all groups
		for id, group := range g.groups {
			// Send group batch to all children
			g.timer.Pause()
			for _, child := range g.outs {
				if err := edge.CollectBufferedBatch(child, *group); err != nil {
					return err
				}
			}
			g.timer.Resume()
			g.mu.Lock()
			// Remove from group
			delete(g.groups, id)
			g.mu.Unlock()
		}
	}
	return nil
}

func determineDimensions(dimensions []interface{}) (allDimensions bool, realDimensions []string) {
	for _, dim := range dimensions {
		switch d := dim.(type) {
		case string:
			realDimensions = append(realDimensions, d)
		case *ast.StarNode:
			allDimensions = true
		}
	}
	sort.Strings(realDimensions)
	return
}

func filterExcludedDimensions(tags models.Tags, dimensions models.Dimensions, excluded []string) []string {
	dimensions.TagNames = models.SortedKeys(tags)
	filtered := dimensions.TagNames[0:0]
	for _, t := range dimensions.TagNames {
		found := false
		for _, x := range excluded {
			if x == t {
				found = true
				break
			}
		}
		if !found {
			filtered = append(filtered, t)
		}
	}
	return filtered
}

func setGroupOnPoint(p models.Point, allDimensions bool, dimensions models.Dimensions, excluded []string) models.Point {
	if allDimensions {
		dimensions.TagNames = filterExcludedDimensions(p.Tags, dimensions, excluded)
	}
	p.Group = models.ToGroupID(p.Name, p.Tags, dimensions)
	p.Dimensions = dimensions
	return p
}
