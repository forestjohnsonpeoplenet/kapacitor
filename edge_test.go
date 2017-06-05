package kapacitor

import (
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/services/logging/loggingtest"
)

func TestEdge_CollectPoint(t *testing.T) {
	name := "point"
	ls := loggingtest.New()
	e := NewLegacyEdge(newEdge("BCollectPoint", "parent", "child", pipeline.StreamEdge, defaultEdgeBufferSize, ls))
	p := models.Point{
		Name: name,
		Tags: models.Tags{
			"tag1": "value1",
			"tag2": "value2",
			"tag3": "value3",
			"tag4": "value4",
		},
		Group: models.ToGroupID(name, nil, models.Dimensions{}),
		Fields: models.Fields{
			"field1": 42,
			"field2": 4.2,
			"field3": 49,
			"field4": 4.9,
		},
	}

	e.CollectPoint(p)
	np, ok := e.NextPoint()
	if !ok {
		t.Fatal("did not get point back out of edge")
	}
	if !reflect.DeepEqual(p, np) {
		t.Errorf("unexpected point after passing through edge:\ngot:\n%v\nexp:\n%v\n", np, p)
	}
}

func TestEdge_CollectBatch(t *testing.T) {
	name := "test-collecte-batch"
	ls := loggingtest.New()
	now := time.Now()
	e := NewLegacyEdge(newEdge("BCollectPoint", "parent", "child", pipeline.StreamEdge, defaultEdgeBufferSize, ls))
	b := models.Batch{
		Name:  name,
		Group: models.ToGroupID(name, nil, models.Dimensions{}),
		TMax:  now,
		Tags: models.Tags{
			"tag1": "value1",
			"tag2": "value2",
		},
		Points: []models.BatchPoint{
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

	e.CollectBatch(b)
	nb, ok := e.NextBatch()
	if !ok {
		t.Fatal("did not get batch back out of edge")
	}
	if !reflect.DeepEqual(b, nb) {
		t.Errorf("unexpected batch after passing through edge:\ngot:\n%v\nexp:\n%v\n", nb, b)
	}
}

func BenchmarkCollectPoint(b *testing.B) {
	name := "point"
	b.ReportAllocs()
	ls := loggingtest.New()
	e := NewLegacyEdge(newEdge("BCollectPoint", "parent", "child", pipeline.StreamEdge, defaultEdgeBufferSize, ls))
	p := models.Point{
		Name: name,
		Tags: models.Tags{
			"tag1": "value1",
			"tag2": "value2",
			"tag3": "value3",
			"tag4": "value4",
		},
		Group: models.ToGroupID(name, nil, models.Dimensions{}),
		Fields: models.Fields{
			"field1": 42,
			"field2": 4.2,
			"field3": 49,
			"field4": 4.9,
		},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e.CollectPoint(p)
			e.NextPoint()
		}
	})
}
