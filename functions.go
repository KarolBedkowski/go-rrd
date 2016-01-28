package main

import "strings"

const (
	// FAverage average values in step
	FAverage Function = iota
	// FMaximum keep greatest value
	FMaximum
	// FMinimum keep minimal value
	FMinimum
	// FSum sum all values in step
	FSum
	// FCount keep count of values in step
	FCount
	// FLast keep last value in step
	FLast
)

// Function is function ID applied on incoming & existing data
type Function int

func (f Function) String() string {
	switch f {
	case FAverage:
		return "average"
	case FSum:
		return "sum"
	case FMinimum:
		return "minimum"
	case FMaximum:
		return "maximum"
	case FCount:
		return "count"
	case FLast:
		return "last"
	}
	return "unknown function"
}

// Apply functions to previous and new value; return processed Value.
func (f Function) Apply(v1, v2 Value) Value {
	v := Value(v2)
	v.Counter = 1
	if !v1.Valid {
		if f == FCount {
			v.Value = float32(v.Counter)
		}
		return v
	}
	if v1.Counter == 0 {
		v1.Counter = 1
	}
	v.Counter = v1.Counter + 1
	switch f {
	case FAverage:
		v.Value = (v1.Value*float32(v1.Counter) + v2.Value) / float32(v1.Counter+1)
	case FSum:
		v.Value = v1.Value + v2.Value
	case FMinimum:
		if v.Value > v1.Value {
			v.Value = v1.Value
		}
	case FMaximum:
		if v.Value < v1.Value {
			v.Value = v1.Value
		}
	case FCount:
		v.Value = float32(v.Counter)
	case FLast:
	}
	return v
}

// ParseFunctionName return function by name
func ParseFunctionName(name string) (Function, bool) {
	var funcID Function
	switch strings.ToLower(name) {
	case "", "average", "avg":
		funcID = FAverage
	case "minimum", "min":
		funcID = FMinimum
	case "maximum", "max":
		funcID = FMaximum
	case "sum":
		funcID = FSum
	case "count":
		funcID = FCount
	case "last":
		funcID = FLast
	default:
		return 0, false
	}
	return funcID, true
}
