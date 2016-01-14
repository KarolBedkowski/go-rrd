package main

import "strings"

const (
	FAverage Function = iota
	FMaximum
	FMinimum
	FSum
	FCount
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
	if !v1.Valid {
		return v2
	}
	v := Value(v2)
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
		v.Value = v1.Value + 1
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
