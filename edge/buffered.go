package edge

type BufferedReceiver interface {
	Receiver
	// BufferedBatch processes an entire buffered batch.
	// Do not modify the batch or the slice of Points as it is shared.
	BufferedBatch(batch BufferedBatchMessage) error
}

// NewBuffer creates a buffer for buffering individual batch mesasge into a BufferedBatchMessagethat will buffer batch messages into a BufferedBatchMessage.
func NewBuffer() *Buffer {
	return &Buffer{}
}

// Buffer implements the non buffered batch receiver methods and buffers them.
type Buffer struct {
	begin  BeginBatchMessage
	points []BatchPointMessage
}

func (r *Buffer) BeginBatch(begin BeginBatchMessage) error {
	r.begin = begin.ShallowCopy()
	r.points = make([]BatchPointMessage, 0, begin.SizeHint())
	return nil
}

func (r *Buffer) BatchPoint(bp BatchPointMessage) error {
	r.points = append(r.points, bp)
	return nil
}

func (r *Buffer) BufferedBatchMessage(end EndBatchMessage) BufferedBatchMessage {
	r.begin.SetSizeHint(len(r.points))
	return NewBufferedBatchMessage(r.begin, r.points, end)
}
