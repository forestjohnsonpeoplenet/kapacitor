package kapacitor

import (
	"encoding/json"
	"log"
	"net/http"
	"path"
	"sync"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/services/httpd"
)

type HTTPOutNode struct {
	node
	c *pipeline.HTTPOutNode

	endpoint string

	mu      sync.RWMutex
	routes  []httpd.Route
	result  *models.Result
	indexes []*httpGroup
}

// Create a new  HTTPOutNode which caches the most recent item and exposes it over the HTTP API.
func newHTTPOutNode(et *ExecutingTask, n *pipeline.HTTPOutNode, l *log.Logger) (*HTTPOutNode, error) {
	hn := &HTTPOutNode{
		node:   node{Node: n, et: et, logger: l},
		c:      n,
		result: new(models.Result),
	}
	et.registerOutput(hn.c.Endpoint, hn)
	hn.node.runF = hn.runOut
	hn.node.stopF = hn.stopOut
	return hn, nil
}

func (h *HTTPOutNode) Endpoint() string {
	return h.endpoint
}

func (h *HTTPOutNode) runOut([]byte) error {
	hndl := func(w http.ResponseWriter, req *http.Request) {
		h.mu.RLock()
		defer h.mu.RUnlock()

		if b, err := json.Marshal(h.result); err != nil {
			httpd.HttpError(
				w,
				err.Error(),
				true,
				http.StatusInternalServerError,
			)
		} else {
			_, _ = w.Write(b)
		}
	}

	p := path.Join("/tasks/", h.et.Task.ID, h.c.Endpoint)

	r := []httpd.Route{{
		Method:      "GET",
		Pattern:     p,
		HandlerFunc: hndl,
	}}

	h.endpoint = h.et.tm.HTTPDService.URL() + p
	h.mu.Lock()
	h.routes = r
	h.mu.Unlock()

	err := h.et.tm.HTTPDService.AddRoutes(r)
	if err != nil {
		return err
	}

	consumer := edge.NewGroupedConsumer(
		h.ins[0],
		h,
	)
	h.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())

	return consumer.Consume()
}

// Update the result structure with a row.
func (h *HTTPOutNode) updateResultWithRow(idx int, row *models.Row) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if idx >= len(h.result.Series) {
		h.incrementErrorCount()
		h.logger.Printf("E! index out of range for row update %d", idx)
		return
	}
	h.result.Series[idx] = row
}

func (h *HTTPOutNode) stopOut() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.et.tm.HTTPDService.DelRoutes(h.routes)
}

func (h *HTTPOutNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	return edge.NewReceiverFromForwardReceiverWithStats(
		h.outs,
		edge.NewTimedForwardReceiver(h.timer, h.newGroup(group.Group)),
	), nil
}

func (h *HTTPOutNode) newGroup(groupID models.GroupID) *httpGroup {
	h.mu.Lock()
	defer h.mu.Unlock()

	idx := len(h.result.Series)
	h.result.Series = append(h.result.Series, nil)
	g := &httpGroup{
		n:      h,
		idx:    idx,
		buffer: edge.NewBuffer(),
	}
	h.indexes = append(h.indexes, g)
	return g
}

func (h *HTTPOutNode) DeleteGroup(groupID models.GroupID) {
	h.mu.Lock()
	defer h.mu.Unlock()

	filteredSeries := h.result.Series[0:0]
	filtered := h.indexes[0:0]
	found := false
	for i, g := range h.indexes {
		if groupID == g.id {
			found = true
			continue
		}
		if found {
			g.idx--
		}
		filtered = append(filtered, g)
		filteredSeries = append(filteredSeries, h.result.Series[i])
	}
	h.indexes = filtered
	h.result.Series = filteredSeries
}

type httpGroup struct {
	n      *HTTPOutNode
	id     models.GroupID
	idx    int
	buffer *edge.Buffer
}

func (g *httpGroup) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	return nil, g.buffer.BeginBatch(begin)
}

func (g *httpGroup) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	return nil, g.buffer.BatchPoint(bp)
}

func (g *httpGroup) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return g.BufferedBatch(g.buffer.BufferedBatchMessage(end))
}

func (g *httpGroup) BufferedBatch(batch edge.BufferedBatchMessage) (edge.Message, error) {
	row := batch.ToRow()
	g.n.updateResultWithRow(g.idx, row)
	return batch, nil
}

func (g *httpGroup) Point(p edge.PointMessage) (edge.Message, error) {
	row := p.ToRow()
	g.n.updateResultWithRow(g.idx, row)
	return p, nil
}

func (g *httpGroup) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
