package edge

import (
	"fmt"
	"time"

	"github.com/influxdata/kapacitor/models"
)

// Message represents data to be passed along an edge.
// Messages can be shared across many contexts.
//
// All messages implement their own ShallowCopy method.
// All ShallowCopy methods create a copy of the message but does not
// deeply copy any reference types.
//
// Never mutate a reference type returned from a message without first directly copying
// the reference type.
type Message interface {
	// Type returns the type of the message.
	Type() MessageType
	//TODO(nathanielc): Explore adding a shared flag to Messages to check when they have been shared.
	// Then we can enforce shared messages cannot be mutated.
	//markShared()
}

type GroupInfoer interface {
	GroupID() models.GroupID
	GroupInfo() GroupInfo
}

type Namer interface {
	Name() string
	SetName(string)
}

type Dimensioner interface {
	Dimensions() models.Dimensions
	SetDimensions(models.Dimensions)
}

type Timer interface {
	Time() time.Time
	SetTime(time.Time)
}

type Fielder interface {
	Fields() models.Fields
	SetFields(models.Fields)
}

type Tagger interface {
	Tags() models.Tags
	SetTags(models.Tags)
}

type FieldsTagsTimer interface {
	Fielder
	Tagger
	Timer
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
type PointMessage interface {
	Message

	ShallowCopy() PointMessage

	Namer

	Database() string
	SetDatabase(string)
	RetentionPolicy() string
	SetRetentionPolicy(string)

	GroupInfoer

	Dimensioner

	FieldsTagsTimer
}

type pointMessage struct {
	name            string
	database        string
	retentionPolicy string

	groupID    models.GroupID
	dimensions models.Dimensions

	tags models.Tags

	fields models.Fields

	time time.Time
}

func NewPointMessage(
	name,
	database,
	retentionPolicy string,
	dimensions models.Dimensions,
	tags models.Tags,
	fields models.Fields,
	time time.Time) PointMessage {
	pm := &pointMessage{
		name:            name,
		database:        database,
		retentionPolicy: retentionPolicy,
		dimensions:      dimensions,
		groupID:         models.ToGroupID(name, tags, dimensions),
		tags:            tags,
		fields:          fields,
		time:            time,
	}
	return pm
}

func (pm *pointMessage) ShallowCopy() PointMessage {
	c := new(pointMessage)
	*c = *pm
	return c
}

func (*pointMessage) Type() MessageType {
	return Point
}

func (pm *pointMessage) Name() string {
	return pm.name
}
func (pm *pointMessage) SetName(name string) {
	pm.name = name
	pm.groupID = models.ToGroupID(pm.name, pm.tags, pm.dimensions)
}
func (pm *pointMessage) Database() string {
	return pm.database
}
func (pm *pointMessage) SetDatabase(database string) {
	pm.database = database
}
func (pm *pointMessage) RetentionPolicy() string {
	return pm.retentionPolicy
}
func (pm *pointMessage) SetRetentionPolicy(retentionPolicy string) {
	pm.retentionPolicy = retentionPolicy
}
func (pm *pointMessage) GroupID() models.GroupID {
	return pm.groupID
}
func (pm *pointMessage) GroupInfo() GroupInfo {
	tags := make(models.Tags, len(pm.dimensions.TagNames))
	for _, t := range pm.dimensions.TagNames {
		tags[t] = pm.tags[t]
	}
	return GroupInfo{
		Group: pm.groupID,
		Tags:  tags,
		Dims:  pm.dimensions,
	}
}

func (pm *pointMessage) Dimensions() models.Dimensions {
	return pm.dimensions
}
func (pm *pointMessage) SetDimensions(dimensions models.Dimensions) {
	if !pm.dimensions.Equal(dimensions) {
		pm.dimensions = dimensions
		pm.groupID = models.ToGroupID(pm.name, pm.tags, pm.dimensions)
	}
}
func (pm *pointMessage) Tags() models.Tags {
	return pm.tags
}
func (pm *pointMessage) SetTags(tags models.Tags) {
	pm.tags = tags
	pm.groupID = models.ToGroupID(pm.name, pm.tags, pm.dimensions)
}
func (pm *pointMessage) Fields() models.Fields {
	return pm.fields
}
func (pm *pointMessage) SetFields(fields models.Fields) {
	pm.fields = fields
}
func (pm *pointMessage) Time() time.Time {
	return pm.time
}
func (pm *pointMessage) SetTime(time time.Time) {
	pm.time = time
}

// BeginBatchMessage marks the beginning of a batch of points.
// Once a BeginBatchMessage is received all subsequent message will be BatchPointMessages until an EndBatchMessage is received.
type BeginBatchMessage interface {
	Message

	ShallowCopy() BeginBatchMessage

	Namer

	GroupInfoer
	Tagger
	Dimensioner
	SetTagsAndDimensions(models.Tags, models.Dimensions)

	// SizeHint provides a hint about the size of the batch to come.
	// If non-zero expect a batch with SizeHint points,
	// otherwise an unknown number of points are coming.
	SizeHint() int
	SetSizeHint(int)
}

type beginBatchMessage struct {
	name       string
	groupID    models.GroupID
	tags       models.Tags
	dimensions models.Dimensions
	// If non-zero expect a batch with SizeHint points,
	// otherwise an unknown number of points are coming.
	sizeHint int
}

func NewBeginBatchMessage(
	name string,
	tags models.Tags,
	dimensions models.Dimensions,
	sizeHint int,
) BeginBatchMessage {
	bb := &beginBatchMessage{
		name:       name,
		tags:       tags,
		dimensions: dimensions,
		groupID:    models.ToGroupID(name, tags, dimensions),
		sizeHint:   sizeHint,
	}
	return bb
}

func (beginBatchMessage) Type() MessageType {
	return BeginBatch
}

func (bb *beginBatchMessage) ShallowCopy() BeginBatchMessage {
	c := new(beginBatchMessage)
	*c = *bb
	return c
}

func (bb *beginBatchMessage) Name() string {
	return bb.name
}
func (bb *beginBatchMessage) SetName(name string) {
	bb.name = name
	bb.groupID = models.ToGroupID(bb.name, bb.tags, bb.dimensions)
}
func (bb *beginBatchMessage) GroupID() models.GroupID {
	return bb.groupID
}
func (bb *beginBatchMessage) GroupInfo() GroupInfo {
	return GroupInfo{
		Group: bb.groupID,
		Tags:  bb.tags,
		Dims:  bb.dimensions,
	}
}
func (bb *beginBatchMessage) Tags() models.Tags {
	return bb.tags
}

// SetTags updates the tags on the message.
// The dimensions are also updated to reflect the new tags.
func (bb *beginBatchMessage) SetTags(tags models.Tags) {
	bb.tags = tags
	bb.dimensions.TagNames = models.SortedKeys(tags)
}

func (bb *beginBatchMessage) Dimensions() models.Dimensions {
	return bb.dimensions
}

// SetDimensions updates the dimensions on the message.
// The tags are updated to reflect the new dimensions.
// If new dimensions are being added use SetTags instead as the dimensions will be automatically updated.
func (bb *beginBatchMessage) SetDimensions(dimensions models.Dimensions) {
	if !bb.dimensions.Equal(dimensions) {
		bb.SetTagsAndDimensions(bb.tags, dimensions)
	}
}

// SetTagsAndDimensions updates both tags and dimensions at the same time.
// The tags will be updated to make sure they match the new dimensions.
func (bb *beginBatchMessage) SetTagsAndDimensions(tags models.Tags, dimensions models.Dimensions) {
	newTags := make(models.Tags, len(tags))
	for _, dim := range dimensions.TagNames {
		newTags[dim] = tags[dim]
	}
	bb.tags = newTags
	bb.dimensions = dimensions
	bb.groupID = models.ToGroupID(bb.name, bb.tags, bb.dimensions)
}

func (bb *beginBatchMessage) SizeHint() int {
	return bb.sizeHint
}
func (bb *beginBatchMessage) SetSizeHint(sizeHint int) {
	bb.sizeHint = sizeHint
}

// BatchPointMessage is a single point in a batch of data.
type BatchPointMessage interface {
	Message

	ShallowCopy() BatchPointMessage

	FieldsTagsTimer
}

type batchPointMessage struct {
	fields models.Fields
	tags   models.Tags
	time   time.Time
}

func NewBatchPointMessage(
	fields models.Fields,
	tags models.Tags,
	time time.Time,
) BatchPointMessage {
	return &batchPointMessage{
		fields: fields,
		tags:   tags,
		time:   time,
	}
}

func (*batchPointMessage) Type() MessageType {
	return BatchPoint
}
func (bp *batchPointMessage) ShallowCopy() BatchPointMessage {
	c := new(batchPointMessage)
	*c = *bp
	return c
}

func (bp *batchPointMessage) Fields() models.Fields {
	return bp.fields
}
func (bp *batchPointMessage) SetFields(fields models.Fields) {
	bp.fields = fields
}
func (bp *batchPointMessage) Tags() models.Tags {
	return bp.tags
}
func (bp *batchPointMessage) SetTags(tags models.Tags) {
	bp.tags = tags
}
func (bp *batchPointMessage) Time() time.Time {
	return bp.time
}
func (bp *batchPointMessage) SetTime(time time.Time) {
	bp.time = time
}

func BatchPointFromPoint(p PointMessage) BatchPointMessage {
	return NewBatchPointMessage(
		p.Fields(),
		p.Tags(),
		p.Time(),
	)
}

// EndBatchMessage indicates that all points for a batch have arrived.
type EndBatchMessage interface {
	Message

	ShallowCopy() EndBatchMessage

	TMax() time.Time
	SetTMax(time.Time)
}

type endBatchMessage struct {
	tmax time.Time
}

func NewEndBatchMessage(tmax time.Time) EndBatchMessage {
	return &endBatchMessage{
		tmax: tmax,
	}
}

func (*endBatchMessage) Type() MessageType {
	return EndBatch
}
func (eb *endBatchMessage) ShallowCopy() EndBatchMessage {
	c := new(endBatchMessage)
	*c = *eb
	return c
}
func (eb *endBatchMessage) TMax() time.Time {
	return eb.tmax
}
func (eb *endBatchMessage) SetTMax(tmax time.Time) {
	eb.tmax = tmax
}

// BufferedBatchMessage is a message containing all data for a single batch.
type BufferedBatchMessage interface {
	Message

	ShallowCopy() BufferedBatchMessage

	Begin() BeginBatchMessage
	SetBegin(BeginBatchMessage)
	Points() []BatchPointMessage
	SetPoints([]BatchPointMessage)
	End() EndBatchMessage
	SetEnd(EndBatchMessage)
}

type bufferedBatchMessage struct {
	begin  BeginBatchMessage
	points []BatchPointMessage
	end    EndBatchMessage
}

func NewBufferedBatchMessage(
	begin BeginBatchMessage,
	points []BatchPointMessage,
	end EndBatchMessage,
) BufferedBatchMessage {
	return &bufferedBatchMessage{
		begin:  begin,
		points: points,
		end:    end,
	}
}

func (*bufferedBatchMessage) Type() MessageType {
	return BufferedBatch
}
func (bb *bufferedBatchMessage) ShallowCopy() BufferedBatchMessage {
	c := new(bufferedBatchMessage)
	*c = *bb
	return c
}
func (bb *bufferedBatchMessage) Begin() BeginBatchMessage {
	return bb.begin
}
func (bb *bufferedBatchMessage) SetBegin(begin BeginBatchMessage) {
	bb.begin = begin
}
func (bb *bufferedBatchMessage) Points() []BatchPointMessage {
	return bb.points
}
func (bb *bufferedBatchMessage) SetPoints(points []BatchPointMessage) {
	bb.points = points
}
func (bb *bufferedBatchMessage) End() EndBatchMessage {
	return bb.end
}
func (bb *bufferedBatchMessage) SetEnd(end EndBatchMessage) {
	bb.end = end
}

// BarrierMessage indicates that no data older than the barrier time will arrive.
type BarrierMessage interface {
	Message
	ShallowCopy() BarrierMessage
	Timer
}
type barrierMessage struct {
	time time.Time
}

func NewBarrierMessage(time time.Time) BarrierMessage {
	return &barrierMessage{
		time: time,
	}
}

func (b *barrierMessage) ShallowCopy() BarrierMessage {
	c := new(barrierMessage)
	*c = *b
	return c
}

func (*barrierMessage) Type() MessageType {
	return Barrier
}
func (b *barrierMessage) Time() time.Time {
	return b.time
}
func (b *barrierMessage) SetTime(time time.Time) {
	b.time = time
}
