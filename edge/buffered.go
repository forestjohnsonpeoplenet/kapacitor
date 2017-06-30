package edge

type BufferedReceiver interface {
	// BufferedBatch processes an entire buffered batch.
	// Do not modify the batch or the slice of Points as it is shared.
	BufferedBatch(batch BufferedBatchMessage) error
	Point(p PointMessage) error
	Barrier(b BarrierMessage) error
}

// NewReceiverFromBufferedReceiver creates a new receiver from r.
func NewReceiverFromBufferedReceiver(r BufferedReceiver) Receiver {
	return &bufferingReceiver{
		r: r,
	}
}

// bufferingReceiver implements the Receiver interface and buffers messages to invoke a BufferedReceiver.
type bufferingReceiver struct {
	r      BufferedReceiver
	begin  BeginBatchMessage
	points []BatchPointMessage
}

func (r *bufferingReceiver) BeginBatch(begin BeginBatchMessage) error {
	r.begin = begin.ShallowCopy()
	r.points = make([]BatchPointMessage, 0, begin.SizeHint())
	return nil
}

func (r *bufferingReceiver) BatchPoint(bp BatchPointMessage) error {
	r.points = append(r.points, bp)
	return nil
}

func (r *bufferingReceiver) EndBatch(end EndBatchMessage) error {
	r.begin.SetSizeHint(len(r.points))
	buffer := NewBufferedBatchMessage(r.begin, r.points, end)
	return r.r.BufferedBatch(buffer)
}

func (r *bufferingReceiver) Point(p PointMessage) error {
	return r.r.Point(p)
}

func (r *bufferingReceiver) Barrier(b BarrierMessage) error {
	return r.r.Barrier(b)
}
