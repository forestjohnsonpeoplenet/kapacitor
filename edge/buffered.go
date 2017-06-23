package edge

type BufferedReceiver interface {
	// Batch processes an entire buffered batch.
	// Do not modify the batch or the slice of Points as it could be shared.
	Batch(batch BufferedBatchMessage) error
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
	buffer BufferedBatchMessage
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
