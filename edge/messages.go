package edge

import (
	"fmt"
	"sort"
	"time"

	"github.com/influxdata/kapacitor/models"
)

// Message represents data to be passed along an edge.
// To determine the concrete type of a message, and therefore access the data, use the Value method to get a non-pointer value and type assert the value to the contrete type.
type Message interface {
	// Type returns the type of the message.
	Type() MessageType
	// Value returns the message value as a non-pointer type.
	Value() interface{}
}

type MessageType int

const (
	BeginBatch MessageType = iota
	BatchPoint
	EndBatch
	BufferedBatch
	Point
	Barrier
)

func (m MessageType) String() string {
	switch m {
	case BeginBatch:
		return "begin_batch"
	case BatchPoint:
		return "batch_point"
	case EndBatch:
		return "end_batch"
	case BufferedBatch:
		return "buffered_batch"
	case Point:
		return "point"
	case Barrier:
		return "barrier"
	default:
		return fmt.Sprintf("unknown message type %d", int(m))
	}
}

// PointMessage is a single point.
// TODO(nathanielc): Define the structure of the type here instead of in the models package.
type PointMessage models.Point

func (PointMessage) Type() MessageType {
	return Point
}
func (p PointMessage) Value() interface{} {
	return p
}

func (pm PointMessage) GroupInfo() GroupInfo {
	return GroupInfo{
		Group: pm.Group,
		Tags:  pm.Tags,
		Dims:  pm.Dimensions,
	}
}

func (pm *PointMessage) UpdateGroup() {
	sort.Strings(pm.Dimensions.TagNames)
	pm.Group = models.ToGroupID(pm.Name, pm.Tags, pm.Dimensions)
}

// BeginBatchMessage marks the beginning of a batch of points.
// Once a BeginBatchMessage is received all subsequent message will be BatchPointMessages until an EndBatchMessage is received.
type BeginBatchMessage struct {
	Name       string
	Group      models.GroupID
	Tags       models.Tags
	Dimensions models.Dimensions
	// If non-zero expect a batch with SizeHint points,
	// otherwise an unknown number of points are coming.
	SizeHint int
}

func (BeginBatchMessage) Type() MessageType {
	return BeginBatch
}
func (bb BeginBatchMessage) Value() interface{} {
	return bb
}

func (bb BeginBatchMessage) GroupInfo() GroupInfo {
	return GroupInfo{
		Group: bb.Group,
		Tags:  bb.Tags,
		Dims:  bb.Dimensions,
	}
}

func (bb *BeginBatchMessage) UpdateGroup() {
	bb.Group = models.ToGroupID(bb.Name, bb.Tags, bb.Dimensions)
}

// BatchPointMessage is a single point in a batch of data.
type BatchPointMessage models.BatchPoint

func (BatchPointMessage) Type() MessageType {
	return BatchPoint
}
func (bp BatchPointMessage) Value() interface{} {
	return bp
}

// EndBatchMessage indicates that all points for a batch have arrived.
type EndBatchMessage struct {
	TMax time.Time
}

func (EndBatchMessage) Type() MessageType {
	return EndBatch
}
func (eb EndBatchMessage) Value() interface{} {
	return eb
}

// BufferedBatchMessage is a message containing all data for a single batch.
type BufferedBatchMessage struct {
	Begin  BeginBatchMessage
	Points []BatchPointMessage
	End    EndBatchMessage
}

func (BufferedBatchMessage) Type() MessageType {
	return BufferedBatch
}
func (bb BufferedBatchMessage) Value() interface{} {
	return bb
}

// BarrierMessage indicates that no data older than the barrier time will arrive.
type BarrierMessage struct {
	Time time.Time
}

func (BarrierMessage) Type() MessageType {
	return Barrier
}
func (b BarrierMessage) Value() interface{} {
	return b
}
