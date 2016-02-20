package main

// AverageByTime average all values in given interval
func AverageByTime(in Rows, step int64) (out Rows) {
	if len(in) < 2 {
		return in
	}
	var lastTS int64 = -1
	var lastRows []Row
	for _, row := range in {
		rowTS := (row.TS / step)
		if lastTS != rowTS && len(lastRows) > 0 {
			row := averageRows(lastRows)
			out = append(out, row)
			lastRows = nil
		}
		lastTS = rowTS
		lastRows = append(lastRows, row)
	}
	if len(lastRows) > 0 {
		row := averageRows(lastRows)
		out = append(out, row)
	}
	return
}

func averageRows(in Rows) (out Row) {
	// count only valid values
	out.TS = in[0].TS
	cols := len(in[0].Values)
	for c := 0; c < cols; c++ {
		value := Value{
			TS: out.TS,
		}
		for _, row := range in {
			v := row.Values[c]
			if v.Valid {
				value.Value += v.Value
				value.Counter++
				value.Valid = true
			}
		}
		if value.Valid {
			value.Value = value.Value / float32(value.Counter)
		}
		out.Values = append(out.Values, value)
	}
	return
}
