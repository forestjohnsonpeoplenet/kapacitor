package edge

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
		switch typ := msg.Type(); typ {
		case BeginBatch:
			begin, ok := msg.(BeginBatchMessage)
			if !ok {
				return ErrImpossibleType{Expected: typ, Actual: msg}
			}
			if err := ec.r.BeginBatch(begin); err != nil {
				return err
			}
		case BatchPoint:
			bp, ok := msg.(BatchPointMessage)
			if !ok {
				return ErrImpossibleType{Expected: typ, Actual: msg}
			}
			if err := ec.r.BatchPoint(bp); err != nil {
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
		case BufferedBatch:
			batch, ok := msg.(BufferedBatchMessage)
			if !ok {
				return ErrImpossibleType{Expected: typ, Actual: msg}
			}
			// If we have a buffered receiver pass the batch straight through.
			if ec.b != nil {
				if err := ec.b.Batch(batch); err != nil {
					return err
				}
			} else {
				// Pass the batch non buffered.
				if err := ec.r.BeginBatch(batch.Begin); err != nil {
					return err
				}
				for _, bp := range batch.Points {
					if err := ec.r.BatchPoint(bp); err != nil {
						return err
					}
				}
				if err := ec.r.EndBatch(batch.End); err != nil {
					return err
				}
			}
		case Point:
			p, ok := msg.(PointMessage)
			if !ok {
				return ErrImpossibleType{Expected: typ, Actual: msg}
			}
			if err := ec.r.Point(p); err != nil {
				return err
			}
		case Barrier:
			b, ok := msg.(BarrierMessage)
			if !ok {
				return ErrImpossibleType{Expected: typ, Actual: msg}
			}
			if err := ec.r.Barrier(b); err != nil {
				return err
			}
		}
	}
	return nil
}
