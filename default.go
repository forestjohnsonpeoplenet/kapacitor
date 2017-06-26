package kapacitor

import (
	"log"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

const (
	statsFieldsDefaulted = "fields_defaulted"
	statsTagsDefaulted   = "tags_defaulted"
)

type DefaultNode struct {
	node
	d *pipeline.DefaultNode

	fieldsDefaulted *expvar.Int
	tagsDefaulted   *expvar.Int
}

// Create a new  DefaultNode which applies a transformation func to each point in a stream and returns a single point.
func newDefaultNode(et *ExecutingTask, n *pipeline.DefaultNode, l *log.Logger) (*DefaultNode, error) {
	dn := &DefaultNode{
		node:            node{Node: n, et: et, logger: l},
		d:               n,
		fieldsDefaulted: new(expvar.Int),
		tagsDefaulted:   new(expvar.Int),
	}
	dn.node.runF = dn.runDefault
	return dn, nil
}

func (e *DefaultNode) runDefault(snapshot []byte) error {
	e.statMap.Set(statsFieldsDefaulted, e.fieldsDefaulted)
	e.statMap.Set(statsTagsDefaulted, e.tagsDefaulted)

	consumer := edge.NewConsumerWithReceiver(
		e.ins[0],
		edge.NewReceiverFromForwardReceiverWithStats(
			e.outs,
			edge.NewTimedForwardReceiver(e.timer, e),
		),
	)
	return consumer.Consume()
}

func (e *DefaultNode) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	_, begin.Tags = e.setDefaults(nil, begin.Tags)
	begin.UpdateGroup()
	return begin, nil
}

func (e *DefaultNode) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	bp.Fields, bp.Tags = e.setDefaults(bp.Fields, bp.Tags)
	return bp, nil
}

func (e *DefaultNode) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return end, nil
}

func (e *DefaultNode) Point(p edge.PointMessage) (edge.Message, error) {
	p.Fields, p.Tags = e.setDefaults(p.Fields, p.Tags)
	p.UpdateGroup()
	return p, nil
}

func (n *DefaultNode) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}

func (d *DefaultNode) setDefaults(fields models.Fields, tags models.Tags) (models.Fields, models.Tags) {
	newFields := fields
	fieldsCopied := false
	for field, value := range d.d.Fields {
		if v := fields[field]; v == nil {
			if !fieldsCopied {
				newFields = newFields.Copy()
				fieldsCopied = true
			}
			d.fieldsDefaulted.Add(1)
			newFields[field] = value
		}
	}
	newTags := tags
	tagsCopied := false
	for tag, value := range d.d.Tags {
		if v := tags[tag]; v == "" {
			if !tagsCopied {
				newTags = newTags.Copy()
				tagsCopied = true
			}
			d.tagsDefaulted.Add(1)
			newTags[tag] = value
		}
	}
	return newFields, newTags
}
