package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Value stricture holds single value in rrd
type Value struct {
	TS      int64 // not stored
	Valid   bool  // int32
	Value   float32
	Counter int64
}

var valueSize = 4 + binary.Size(float32(0)) + 8

func (v *Value) String() string {
	return fmt.Sprintf("Value[td=%d, valid=%v, value=%v, counter=%d]",
		v.TS, v.Valid, v.Value, v.Counter)
}

func (v *Value) Write(r io.Writer) (err error) {
	if err = binary.Write(r, binary.LittleEndian, v.Value); err != nil {
		return
	}
	if err = binary.Write(r, binary.LittleEndian, v.Counter); err != nil {
		return
	}
	valid := int32(0)
	if v.Valid {
		valid = 1
	}
	if err = binary.Write(r, binary.LittleEndian, valid); err != nil {
		return
	}
	return
}
