package edge

import "fmt"

type ErrImpossibleType struct {
	Expected MessageType
	Actual   interface{}
}

func (e ErrImpossibleType) Error() string {
	return fmt.Sprintf("impossible type: expected %v actual: %v", e.Expected, e.Actual)
}
