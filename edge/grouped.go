package edge

type GroupedConsumer struct {
	consumer *Consumer
	gr       GroupedReceiver
	groups   map[string]Receiver
	current  Receiver
}

func NewGroupedConsumer(edge Edge, gr GroupedReceiver) *GroupedConsumer {
	gc := &GroupedConsumer{
		gr:     gr,
		groups: make(map[string]Receiver),
	}
	gc.consumer = NewConsumer(edge, gc)
	return gc
}

func (c *GroupedConsumer) Run() error {
	return gc.consumer.Run()
}

func (c *GroupedConsumer) getOrCreateGroup(group models.Group) Receiver {
	r, ok := c.groups[group]
	if !ok {
		r = c.gr.NewGroup(group)
		c.groups[group] = r
	}
	return r
}

func (c *GroupedConsumer) BeginBatch(begin BeginBatchMessage) error {
	r := c.getOrCreateGroup(begin.Group)
	c.current = r
	return r.BeginBatch(begin)
}

func (c *GroupedConsumer) Point(p models.Point) error {
	if c.current != nil {
		return c.current.Point(p)
	}
	r := c.getOrCreateGroup(p.Group)
	return r.Point(point)
}

func (c *GroupedConsumer) EndBatch(end EndBatchMessage) error {
	return c.current.EndBatch(end)
}

type GroupedReceiver interface {
	NewGroup(group models.Group) Receiver
	DeleteGroup(group models.Group)
}
