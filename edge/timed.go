package edge

import "github.com/influxdata/kapacitor/timer"

type timedForwardReceiver struct {
	timer timer.Timer
	r     ForwardReceiver
}

// NewTimedForwardReceiver creates a forward receiver which times the time spent in r.
func NewTimedForwardReceiver(t timer.Timer, r ForwardReceiver) ForwardReceiver {
	return &timedForwardReceiver{
		timer: t,
		r:     r,
	}
}

func (tr *timedForwardReceiver) BeginBatch(begin BeginBatchMessage) (m Message, err error) {
	tr.timer.Start()
	m, err = tr.r.BeginBatch(begin)
	tr.timer.Stop()
	return
}

func (tr *timedForwardReceiver) BatchPoint(bp BatchPointMessage) (m Message, err error) {
	tr.timer.Start()
	m, err = tr.r.BatchPoint(bp)
	tr.timer.Stop()
	return
}

func (tr *timedForwardReceiver) EndBatch(end EndBatchMessage) (m Message, err error) {
	tr.timer.Start()
	m, err = tr.r.EndBatch(end)
	tr.timer.Stop()
	return
}

func (tr *timedForwardReceiver) Point(p PointMessage) (m Message, err error) {
	tr.timer.Start()
	m, err = tr.r.Point(p)
	tr.timer.Stop()
	return
}

func (tr *timedForwardReceiver) Barrier(b BarrierMessage) (m Message, err error) {
	tr.timer.Start()
	m, err = tr.r.Barrier(b)
	tr.timer.Stop()
	return
}
