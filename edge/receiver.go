package edge

import "github.com/influxdata/kapacitor/models"

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
	NewGroup(group models.GroupID) Receiver
	DeleteGroup(group models.GroupID)
}
