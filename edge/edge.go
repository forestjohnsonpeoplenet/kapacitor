package edge

import (
	"fmt"

	"github.com/influxdata/influxdb/models"
)

type MessageType int

const (
	Point MessageType = iota
)

type Message interface {
	Type() MessageType
}

type PointMessage models.Point

func (pm PointMessage) Type() MessageType {
	return Point
}

type Edge interface {
	Collect(Message)
	Next() (Message, bool)
	Close()
	Abort()
}

type EdgeConsumer struct {
	edge Edge
	r    EdgeReceiver
}

func NewEdgeConsumer(edge Edge, r EdgeReceiver) *EdgeConsumer {
	return &EdgeConsumer{
		edge: edge,
		r:    r,
	}
}

type ErrImpossibleType struct {
	Expected MessageType
	Actual   interface{}
}

func (e ErrImpossibleType) Error() string {
	return fmt.Sprintf("impossible type: expected %v actual: %v", e.Expected, e.Actual)
}

func (ec *EdgeConsumer) Run() error {
	for msg, ok := ec.edge.Next(); ok; msg, ok = ec.edge.Next() {
		switch typ := msg.Type(); typ {
		case BeginBatch:
		case EndBatch:
		case Point:
			p, ok := msg.(PointMessage)
			if !ok {
				return ErrImpossibleType{Expected: typ, Actual: p}
			}
			if err := ec.r.Point(p); err != nil {
				return err
			}
		}
	}
}

type EdgeReceiver interface {
	BeginBatch()
	Point(p models.Point)
	EndBatch()
}

type GroupedEdge struct {
	edge Edge
}

type GroupHandler interface {
}

func NewGroupedEdge(edge Edge) *GroupedEdge {
	return &GroupedEdge{
		edge: edge,
	}
}
