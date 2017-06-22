package edge

import (
	"log"

	"github.com/influxdata/kapacitor/pipeline"
)

type logEdge struct {
	e      Edge
	logger *log.Logger
}

func NewLogEdge(l *log.Logger, e Edge) Edge {
	return &logEdge{
		e:      e,
		logger: l,
	}
}

func (e *logEdge) Collect(m Message) error {
	e.logger.Println("D! collect:", m.Type())
	return e.e.Collect(m)
}

func (e *logEdge) Next() (m Message, ok bool) {
	m, ok = e.e.Next()
	if ok {
		e.logger.Println("D! next:", m.Type())
	}
	return
}

func (e *logEdge) Close() error {
	return e.e.Close()
}

func (e *logEdge) Abort() {
	e.e.Abort()
}

func (e *logEdge) Type() pipeline.EdgeType {
	return e.e.Type()
}
