package edge

import (
	"github.com/influxdata/kapacitor/models"
)

// Receiver handles messages as they arrive via a consumer.
type Receiver interface {
	BeginBatch(begin BeginBatchMessage) error
	BatchPoint(bp BatchPointMessage) error
	EndBatch(end EndBatchMessage) error
	Point(p PointMessage) error
	Barrier(b BarrierMessage) error
}

// GroupedReceiver creates and deletes receivers as groups are created and deleted.
type GroupedReceiver interface {
	// NewGroup signals that a new group has been discovered in the data.
	// Information on the group and the message that first triggered its creation are provided.
	NewGroup(group GroupInfo, first Message) (Receiver, error)
	DeleteGroup(group models.GroupID)
}
