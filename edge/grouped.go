package edge

import (
	"errors"

	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
)

// GroupedConsumer reads messages off an edge and passes them by group to receivers created from a grouped receiver.
type GroupedConsumer interface {
	Consumer
	// CardinalityVar is an exported var that indicates the current number of groups being managed.
	CardinalityVar() expvar.IntVar
}

type groupedConsumer struct {
	consumer    Consumer
	gr          GroupedReceiver
	groups      map[models.GroupID]Receiver
	current     Receiver
	cardinality *expvar.Int
}

// NewGroupedConsumer creates a new grouped consumer for edge e and grouped receiver r.
func NewGroupedConsumer(e Edge, r GroupedReceiver) GroupedConsumer {
	gc := &groupedConsumer{
		gr:          r,
		groups:      make(map[models.GroupID]Receiver),
		cardinality: new(expvar.Int),
	}
	gc.consumer = NewConsumerWithReceiver(e, gc)
	return gc
}

func (c *groupedConsumer) Consume() error {
	return c.consumer.Consume()
}
func (c *groupedConsumer) CardinalityVar() expvar.IntVar {
	return c.cardinality
}

func (c *groupedConsumer) getOrCreateGroup(group models.GroupID) Receiver {
	r, ok := c.groups[group]
	if !ok {
		c.cardinality.Add(1)
		r = c.gr.NewGroup(group)
		c.groups[group] = r
	}
	return r
}

func (c *groupedConsumer) BeginBatch(begin BeginBatchMessage) error {
	r := c.getOrCreateGroup(begin.Group)
	c.current = r
	return r.BeginBatch(begin)
}

func (c *groupedConsumer) BatchPoint(p BatchPointMessage) error {
	if c.current == nil {
		return errors.New("received batch point without batch")
	}
	return c.current.BatchPoint(p)
}

func (c *groupedConsumer) EndBatch(end EndBatchMessage) error {
	err := c.current.EndBatch(end)
	c.current = nil
	return err
}

func (c *groupedConsumer) Point(p PointMessage) error {
	r := c.getOrCreateGroup(p.Group)
	return r.Point(p)
}

func (c *groupedConsumer) Barrier(b BarrierMessage) error {
	// Barriers messages apply to all gorups
	for _, r := range c.groups {
		if err := r.Barrier(b); err != nil {
			return err
		}
	}
	return nil
}
