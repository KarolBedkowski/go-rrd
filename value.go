package main

import (
	"fmt"
)

// Value stricture holds single value in rrd
type Value struct {
	TS      int64 // not stored
	Valid   bool  // int32
	Value   float32
	Counter int64
	Column  int // not stored
}

func (v *Value) String() string {
	return fmt.Sprintf("Value[td=%d, valid=%v, value=%v, counter=%d]",
		v.TS, v.Valid, v.Value, v.Counter)
}
