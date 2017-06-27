package kapacitor

import (
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/services/logging/loggingtest"
)

var name = "edge_test"
var now = time.Now()
var groupTags = models.Tags{
	"tag1": "value1",
	"tag2": "value2",
}
var groupDims = models.Dimensions{TagNames: []string{"tag1", "tag2"}}
var groupID = models.ToGroupID(name, groupTags, groupDims)

var point = edge.PointMessage{
	Name: name,
	Time: now,
	Tags: models.Tags{
		"tag1": "value1",
		"tag2": "value2",
		"tag3": "value3",
		"tag4": "value4",
	},
	Group:      groupID,
	Dimensions: groupDims,
	Fields: models.Fields{
		"field1": 42,
		"field2": 4.2,
		"field3": 49,
		"field4": 4.9,
	},
}

var batch = edge.BufferedBatchMessage{
	Begin: edge.BeginBatchMessage{
		Name:       name,
		Tags:       groupTags,
		Group:      groupID,
		Dimensions: groupDims,
		SizeHint:   2,
	},
	End: edge.EndBatchMessage{
		TMax: now,
	},
	Points: []edge.BatchPointMessage{
		{
			Time: now.Add(-1 * time.Second),
			Tags: models.Tags{
				"tag1": "value1",
				"tag2": "value2",
				"tag3": "first",
				"tag4": "first",
			},
			Fields: models.Fields{
				"field1": 42,
				"field2": 4.2,
				"field3": 49,
				"field4": 4.9,
			},
		},
		{
			Time: now,
			Tags: models.Tags{
				"tag1": "value1",
				"tag2": "value2",
				"tag3": "second",
				"tag4": "second",
			},
			Fields: models.Fields{
				"field1": 42,
				"field2": 4.2,
				"field3": 49,
				"field4": 4.9,
			},
		},
	},
}

func TestEdge_CollectPoint(t *testing.T) {
	ls := loggingtest.New()
	e := newEdge("TestEdge_CollectPoint", "parent", "child", pipeline.StreamEdge, defaultEdgeBufferSize, ls)

	e.Collect(point)
	msg, ok := e.Emit()
	if !ok {
		t.Fatal("did not get point back out of edge")
	}
	if !reflect.DeepEqual(msg, point) {
		t.Errorf("unexpected point after passing through edge:\ngot:\n%v\nexp:\n%v\n", msg, point)
	}
}

func TestEdge_CollectBatch(t *testing.T) {
	ls := loggingtest.New()
	e := newEdge("TestEdge_CollectBatch", "parent", "child", pipeline.BatchEdge, defaultEdgeBufferSize, ls)
	e.Collect(batch)
	msg, ok := e.Emit()
	if !ok {
		t.Fatal("did not get batch back out of edge")
	}
	if !reflect.DeepEqual(batch, msg) {
		t.Errorf("unexpected batch after passing through edge:\ngot:\n%v\nexp:\n%v\n", msg, batch)
	}
}

var emittedMsg edge.Message
var emittedOK bool

func BenchmarkCollectPoint(b *testing.B) {
	ls := loggingtest.New()
	e := newEdge("BenchmarkCollectPoint", "parent", "child", pipeline.StreamEdge, defaultEdgeBufferSize, ls)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e.Collect(point)
			emittedMsg, emittedOK = e.Emit()
		}
	})
}

func BenchmarkCollectBatch(b *testing.B) {
	ls := loggingtest.New()
	e := newEdge("BenchmarkCollectBatch", "parent", "child", pipeline.StreamEdge, defaultEdgeBufferSize, ls)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e.Collect(batch)
			emittedMsg, emittedOK = e.Emit()
		}
	})
}
