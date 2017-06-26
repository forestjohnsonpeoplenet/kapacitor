package edge

import "fmt"

type Consumer struct {
	edge Edge
	r    Receiver
	b    BufferedReceiver
}

func NewConsumerWithReceiver(edge Edge, r Receiver) *Consumer {
	return &Consumer{
		edge: edge,
		r:    r,
	}
}
func NewConsumerWithBufferedReceiver(edge Edge, buffered BufferedReceiver) *Consumer {
	return &Consumer{
		edge: edge,
		r:    NewBufferingReceiver(buffered),
		b:    buffered,
	}
}

func (ec *Consumer) Run() error {
	for msg, ok := ec.edge.Emit(); ok; msg, ok = ec.edge.Emit() {
		switch m := msg.Value().(type) {
		case BeginBatchMessage:
			if err := ec.r.BeginBatch(m); err != nil {
				return err
			}
		case BatchPointMessage:
			if err := ec.r.BatchPoint(m); err != nil {
				return err
			}
		case EndBatchMessage:
			if err := ec.r.EndBatch(m); err != nil {
				return err
			}
		case BufferedBatchMessage:
			// If we have a buffered receiver pass the batch straight through.
			if ec.b != nil {
				if err := ec.b.Batch(m); err != nil {
					return err
				}
			} else {
				// Pass the batch non buffered.
				if err := ec.r.BeginBatch(m.Begin); err != nil {
					return err
				}
				for _, bp := range m.Points {
					if err := ec.r.BatchPoint(bp); err != nil {
						return err
					}
				}
				if err := ec.r.EndBatch(m.End); err != nil {
					return err
				}
			}
		case PointMessage:
			if err := ec.r.Point(m); err != nil {
				return err
			}
		case BarrierMessage:
			if err := ec.r.Barrier(m); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown message type %T", msg)
		}
	}
	return nil
}
