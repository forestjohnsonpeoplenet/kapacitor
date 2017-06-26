package edge

import "fmt"

// Consumer reads messages off an edge and passes them to a receiver.
type Consumer interface {
	// Consume reads messages off an edge until the edge is closed or aborted.
	// An error is returned if either the edge or receiver errors.
	Consume() error
}

type consumer struct {
	edge Edge
	r    Receiver
	b    BufferedReceiver
}

// NewConsumerWithReceiver creates a new consumer for the edge e and receiver r.
func NewConsumerWithReceiver(e Edge, r Receiver) Consumer {
	return &consumer{
		edge: e,
		r:    r,
	}
}

// NewConsumerWithReceiver creates a new consumer for the edge e and buffered receiver b.
func NewConsumerWithBufferedReceiver(e Edge, b BufferedReceiver) Consumer {
	return &consumer{
		edge: e,
		r:    NewReceiverFromBufferedReceiver(b),
		b:    b,
	}
}

func (ec *consumer) Consume() error {
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
			return fmt.Errorf("unexpected message of type %T", msg)
		}
	}
	return nil
}
