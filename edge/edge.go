package edge

type Edge interface {
	Collect(Message)
	Next() (Message, bool)
	Close()
	Abort()
}
