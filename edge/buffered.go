package edge

type BufferedBatch struct {
	Begin  BeginBatchMessage
	Points []BatchPointMessage
	End    EndBatchMessage
}

type BufferedReceiver interface {
	Batch(batch BufferedBatch) error
	Point(p PointMessage) error
	Barrier(b BarrierMessage) error
}

func NewBufferingReceiver(r BufferedReceiver) Receiver {
	return &bufferingReceiver{
		r: r,
	}
}

type bufferingReceiver struct {
	r      BufferedReceiver
	buffer BufferedBatch
}

func (r *bufferingReceiver) BeginBatch(begin BeginBatchMessage) error {
	r.buffer.Begin = begin
	r.buffer.Points = make([]BatchPointMessage, 0, begin.SizeHint)
	return nil
}

func (r *bufferingReceiver) BatchPoint(bp BatchPointMessage) error {
	r.buffer.Points = append(r.buffer.Points, bp)
	return nil
}

func (r *bufferingReceiver) EndBatch(end EndBatchMessage) error {
	r.buffer.End = end
	return r.r.Batch(r.buffer)
}

func (r *bufferingReceiver) Point(p PointMessage) error {
	return r.r.Point(p)
}

func (r *bufferingReceiver) Barrier(b BarrierMessage) error {
	return r.r.Barrier(b)
}

func CollectBufferedBatch(e Edge, batch BufferedBatch) error {
	// Update SizeHint now that we know the final size.
	batch.Begin.SizeHint = len(batch.Points)
	if err := e.Collect(batch.Begin); err != nil {
		return err
	}
	for _, bp := range batch.Points {
		if err := e.Collect(bp); err != nil {
			return err
		}
	}
	if err := e.Collect(batch.End); err != nil {
		return err
	}
	return nil
}
