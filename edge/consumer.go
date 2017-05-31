package edge

import "github.com/influxdata/influxdb/models"

type Consumer struct {
	edge Edge
	r    Receiver
}

func NewConsumer(edge Edge, r Receiver) *Consumer {
	return &Consumer{
		edge: edge,
		r:    r,
	}
}

func (ec *Consumer) Run() error {
	for msg, ok := ec.edge.Next(); ok; msg, ok = ec.edge.Next() {
		switch typ := msg.Type(); typ {
		case BeginBatch:
			begin, ok := msg.(BeginBatchMessage)
			if !ok {
				return ErrImpossibleType{Expected: typ, Actual: msg}
			}
			if err := ec.r.BeginBatch(begin); err != nil {
				return err
			}
		case Point:
			p, ok := msg.(PointMessage)
			if !ok {
				return ErrImpossibleType{Expected: typ, Actual: msg}
			}
			if err := ec.r.Point(p); err != nil {
				return err
			}
		case EndBatch:
			end, ok := msg.(EndBatchMessage)
			if !ok {
				return ErrImpossibleType{Expected: typ, Actual: msg}
			}
			if err := ec.r.EndBatch(end); err != nil {
				return err
			}
		}
	}
}

type Receiver interface {
	BeginBatch(begin BeginBatchMessage) error
	Point(p models.Point) error
	EndBatch(end EndBatchMessage) error
}
