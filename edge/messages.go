package edge

import (
	"time"

	"github.com/influxdata/kapacitor/models"
)

type MessageType int

const (
	BeginBatch MessageType = iota
	EndBatch
	Point
	Barrier
)

type Message interface {
	Type() MessageType
}

type PointMessage models.Point

func (pm PointMessage) Type() MessageType {
	return Point
}

type BeginBatchMessage struct {
	Name   string
	Group  models.GroupID
	Tags   models.Tags
	ByName bool
	// If non-zero expect a batch with SizeHint points,
	// otherwise an unknown number of points are coming.
	SizeHint int
}

func (bb BeginBatchMessage) Type() MessageType {
	return BeginBatch
}

// EndBatchMessage indicates that all points for a batch have arrived.
type EndBatchMessage struct {
	TMax time.Time
}

func (eb EndBatchMessage) Type() MessageType {
	return EndBatch
}

// BarrierMessage indicates that no data older than the barrier time will arrive.
type BarrierMessage struct {
	Time time.Time
}

func (b BarrierMessage) Type() MessageType {
	return Barrier
}
