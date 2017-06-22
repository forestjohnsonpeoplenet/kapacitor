package edge

import (
	"github.com/influxdata/kapacitor/timer"
)

type ForwardReceiver interface {
	BeginBatch(begin BeginBatchMessage) (Message, error)
	BatchPoint(bp BatchPointMessage) (Message, error)
	EndBatch(end EndBatchMessage) (Message, error)
	Point(p PointMessage) (Message, error)
	Barrier(b BarrierMessage) (Message, error)
}

type ForwardingReceiver struct {
	outs []Edge
	r    ForwardReceiver
}

func NewForwardingReceiverFromStats(outs []StatsEdge, r ForwardReceiver) *ForwardingReceiver {
	os := make([]Edge, len(outs))
	for i := range outs {
		os[i] = outs[i]
	}
	return NewForwardingReceiver(os, r)
}
func NewForwardingReceiver(outs []Edge, r ForwardReceiver) *ForwardingReceiver {
	return &ForwardingReceiver{
		outs: outs,
		r:    r,
	}
}

func (fr *ForwardingReceiver) BeginBatch(begin BeginBatchMessage) error {
	return fr.forward(fr.r.BeginBatch(begin))
}
func (fr *ForwardingReceiver) BatchPoint(bp BatchPointMessage) error {
	return fr.forward(fr.r.BatchPoint(bp))
}
func (fr *ForwardingReceiver) EndBatch(end EndBatchMessage) error {
	return fr.forward(fr.r.EndBatch(end))
}
func (fr *ForwardingReceiver) Point(p PointMessage) error {
	return fr.forward(fr.r.Point(p))
}
func (fr *ForwardingReceiver) Barrier(b BarrierMessage) error {
	return fr.forward(fr.r.Barrier(b))
}

func (fr *ForwardingReceiver) forward(msg Message, err error) error {
	if err != nil {
		return err
	}
	if msg != nil {
		for _, out := range fr.outs {
			if err := out.Collect(msg); err != nil {
				return err
			}
		}
	}
	return nil
}

type TimedForwardingReceiver struct {
	timer timer.Timer
	r     ForwardReceiver
}

func NewTimedForwardingReceiver(t timer.Timer, r ForwardReceiver) *TimedForwardingReceiver {
	return &TimedForwardingReceiver{
		timer: t,
		r:     r,
	}
}

func (tr *TimedForwardingReceiver) BeginBatch(begin BeginBatchMessage) (m Message, err error) {
	tr.timer.Start()
	m, err = tr.r.BeginBatch(begin)
	tr.timer.Stop()
	return
}

func (tr *TimedForwardingReceiver) BatchPoint(bp BatchPointMessage) (m Message, err error) {
	tr.timer.Start()
	m, err = tr.r.BatchPoint(bp)
	tr.timer.Stop()
	return
}

func (tr *TimedForwardingReceiver) EndBatch(end EndBatchMessage) (m Message, err error) {
	tr.timer.Start()
	m, err = tr.r.EndBatch(end)
	tr.timer.Stop()
	return
}

func (tr *TimedForwardingReceiver) Point(p PointMessage) (m Message, err error) {
	tr.timer.Start()
	m, err = tr.r.Point(p)
	tr.timer.Stop()
	return
}

func (tr *TimedForwardingReceiver) Barrier(b BarrierMessage) (m Message, err error) {
	tr.timer.Start()
	m, err = tr.r.Barrier(b)
	tr.timer.Stop()
	return
}
