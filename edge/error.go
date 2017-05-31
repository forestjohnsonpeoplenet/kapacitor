package edge

import (
	"errors"
	"fmt"
)

var ErrAborted = errors.New("edge aborted")

type ErrImpossibleType struct {
	Expected MessageType
	Actual   interface{}
}

func (e ErrImpossibleType) Error() string {
	return fmt.Sprintf("impossible type: expected %v actual: %v", e.Expected, e.Actual)
}
