package kapacitor

import (
	"log"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

const (
	statsFieldsDeleted = "fields_deleted"
	statsTagsDeleted   = "tags_deleted"
)

type DeleteNode struct {
	node
	d *pipeline.DeleteNode

	fieldsDeleted *expvar.Int
	tagsDeleted   *expvar.Int

	tags map[string]bool
}

// Create a new  DeleteNode which applies a transformation func to each point in a stream and returns a single point.
func newDeleteNode(et *ExecutingTask, n *pipeline.DeleteNode, l *log.Logger) (*DeleteNode, error) {
	tags := make(map[string]bool)
	for _, tag := range n.Tags {
		tags[tag] = true
	}

	dn := &DeleteNode{
		node:          node{Node: n, et: et, logger: l},
		d:             n,
		fieldsDeleted: new(expvar.Int),
		tagsDeleted:   new(expvar.Int),
		tags:          tags,
	}
	dn.node.runF = dn.runDelete
	return dn, nil
}

func (n *DeleteNode) runDelete(snapshot []byte) error {
	n.statMap.Set(statsFieldsDeleted, n.fieldsDeleted)
	n.statMap.Set(statsTagsDeleted, n.tagsDeleted)
	consumer := edge.NewConsumerWithReceiver(
		n.ins[0],
		edge.NewReceiverFromForwardReceiverWithStats(
			n.outs,
			edge.NewTimedForwardReceiver(n.timer, n),
		),
	)
	return consumer.Consume()
}

func (n *DeleteNode) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	_, begin.Tags = n.doDeletes(nil, begin.Tags)
	begin.UpdateGroup()
	return begin, nil
}

func (n *DeleteNode) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	bp.Fields, bp.Tags = n.doDeletes(bp.Fields, bp.Tags)
	return bp, nil
}

func (n *DeleteNode) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return end, nil
}

func (n *DeleteNode) Point(p edge.PointMessage) (edge.Message, error) {
	p.Fields, p.Tags = n.doDeletes(p.Fields, p.Tags)
	// Check if we deleted a group by dimension
	updateDims := false
	for _, dim := range p.Dimensions.TagNames {
		if !n.tags[dim] {
			updateDims = true
			break
		}
	}
	if updateDims {
		newDims := make([]string, 0, len(p.Dimensions.TagNames))
		for _, dim := range p.Dimensions.TagNames {
			if !n.tags[dim] {
				newDims = append(newDims, dim)
			}
		}
		p.Dimensions.TagNames = newDims
		p.Group = models.ToGroupID(p.Name, p.Tags, p.Dimensions)
	}
	return p, nil
}

func (n *DeleteNode) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}

func (n *DeleteNode) doDeletes(fields models.Fields, tags models.Tags) (models.Fields, models.Tags) {
	newFields := fields
	fieldsCopied := false
	for _, field := range n.d.Fields {
		if _, ok := fields[field]; ok {
			if !fieldsCopied {
				newFields = newFields.Copy()
				fieldsCopied = true
			}
			n.fieldsDeleted.Add(1)
			delete(newFields, field)
		}
	}
	newTags := tags
	tagsCopied := false
	for _, tag := range n.d.Tags {
		if _, ok := tags[tag]; ok {
			if !tagsCopied {
				newTags = newTags.Copy()
				tagsCopied = true
			}
			n.tagsDeleted.Add(1)
			delete(newTags, tag)
		}
	}
	return newFields, newTags
}
