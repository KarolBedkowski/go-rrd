package main

import "strings"

const (
	F_AVERAGE Function = iota
	F_MAXIMUM
	F_MINIMUM
	F_SUM
	F_COUNT
	F_LAST
)

// Function is function ID applied on incoming & existing data
type Function int

func (f Function) String() string {
	switch f {
	case F_AVERAGE:
		return "average"
	case F_SUM:
		return "sum"
	case F_MINIMUM:
		return "minimum"
	case F_MAXIMUM:
		return "maximum"
	case F_COUNT:
		return "count"
	case F_LAST:
		return "last"
	}
	return "unknown function"
}

// Apply functions to previous and new value; return processed Value.
func (f Function) Apply(v1, v2 Value) Value {
	if !v1.Valid {
		return v2
	}
	v := Value{
		TS:      v2.TS,
		Valid:   v2.Valid,
		Value:   v2.Value,
		Counter: v1.Counter + 1,
	}
	switch f {
	case F_AVERAGE:
		v.Value = (v1.Value*float32(v1.Counter) + v.Value) / float32(v1.Counter+1)
	case F_SUM:
		v.Value = v1.Value + v2.Value
	case F_MINIMUM:
		if v.Value > v1.Value {
			v.Value = v1.Value
		}
	case F_MAXIMUM:
		if v.Value < v1.Value {
			v.Value = v1.Value
		}
	case F_COUNT:
		v.Value = v1.Value + 1
	case F_LAST:
	}
	return v
}

// ParseFunctionName return function by name
func ParseFunctionName(name string) (Function, bool) {
	var funcID Function
	switch strings.ToLower(name) {
	case "", "average", "avg":
		funcID = F_AVERAGE
	case "minimum", "min":
		funcID = F_MINIMUM
	case "maximum", "max":
		funcID = F_MAXIMUM
	case "sum":
		funcID = F_SUM
	case "count":
		funcID = F_COUNT
	case "last":
		funcID = F_LAST
	default:
		return 0, false
	}
	return funcID, true
}
