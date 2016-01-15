package main

import (
	"fmt"
)

// Value stricture holds single value in rrd
type Value struct {
	TS        int64 // not stored
	Valid     bool  // int32
	Value     float32
	Counter   int64
	Column    int // not stored
	ArchiveID int // not stored -
}

func NewValue(ts int64, value float32) Value {
	return Value{
		TS:      ts,
		Valid:   true,
		Value:   value,
		Counter: 1,
	}
}

func (v *Value) String() string {
	return fmt.Sprintf("Value[ts=%d, ok=%v, v=%v, c=%d]",
		v.TS, v.Valid, v.Value, v.Counter)
}
