package edge

import "fmt"

// Consumer reads messages off an edge and passes them to a receiver.
type Consumer interface {
	// Consume reads messages off an edge until the edge is closed or aborted.
	// An error is returned if either the edge or receiver errors.
	Consume() error
}

// Receiver handles messages as they arrive via a consumer.
type Receiver interface {
	BeginBatch(begin BeginBatchMessage) error
	BatchPoint(bp BatchPointMessage) error
	EndBatch(end EndBatchMessage) error
	Point(p PointMessage) error
	Barrier(b BarrierMessage) error
}

type consumer struct {
	edge Edge
	r    Receiver
}

// NewConsumerWithReceiver creates a new consumer for the edge e and receiver r.
func NewConsumerWithReceiver(e Edge, r Receiver) Consumer {
	return &consumer{
		edge: e,
		r:    r,
	}
}

func (ec *consumer) Consume() error {
	for msg, ok := ec.edge.Emit(); ok; msg, ok = ec.edge.Emit() {
		switch m := msg.(type) {
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
			err := receiveBufferedBatch(ec.r, m)
			if err != nil {
				return err
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

func receiveBufferedBatch(r Receiver, batch BufferedBatchMessage) error {
	b, ok := r.(BufferedReceiver)
	// If we have a buffered receiver pass the batch straight through.
	if ok {
		return b.BufferedBatch(batch)
	}

	// Pass the batch non buffered.
	if err := r.BeginBatch(batch.Begin()); err != nil {
		return err
	}
	for _, bp := range batch.Points() {
		if err := r.BatchPoint(bp); err != nil {
			return err
		}
	}
	return r.EndBatch(batch.End())
}
