package kapacitor

import (
	"log"

	"github.com/influxdata/kapacitor/pipeline"
)

type NoOpNode struct {
	node
}

// Create a new  NoOpNode which does nothing with the data and just passes it through.
func newNoOpNode(et *ExecutingTask, n *pipeline.NoOpNode, l *log.Logger) (*NoOpNode, error) {
	nn := &NoOpNode{
		node: node{Node: n, et: et, logger: l},
	}
	nn.node.runF = nn.runNoOp
	return nn, nil
}

func (s *NoOpNode) runNoOp([]byte) error {
	ins := NewLegacyEdges(s.ins)
	outs := NewLegacyEdges(s.outs)

	switch s.Wants() {
	case pipeline.StreamEdge:
		for p, ok := ins[0].NextPoint(); ok; p, ok = ins[0].NextPoint() {
			for _, child := range outs {
				err := child.CollectPoint(p)
				if err != nil {
					return err
				}
			}
		}
	case pipeline.BatchEdge:
		for b, ok := ins[0].NextBatch(); ok; b, ok = ins[0].NextBatch() {
			for _, child := range outs {
				err := child.CollectBatch(b)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
