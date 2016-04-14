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

// AverageToNumber average values to get no more than given points
func AverageToNumber(in Rows, maxRows int) (out Rows) {
	if len(in) < 2 || len(in) < maxRows {
		return in
	}

	minTS := in[0].TS
	maxTS := in[len(in)-1].TS
	step := (maxTS - minTS) / int64(maxRows)

	return AverageByTime(in, step)
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

//RemoveInvalidVals set values that not match min-max range as invalid
func RemoveInvalidVals(rows Rows, cols []RRDColumn) (out Rows) {
	for _, row := range rows {
		outRow := Row{TS: row.TS}
		for cid, col := range cols {
			val := row.Values[cid]
			val.Valid = (val.Valid &&
				(!col.HasMinimum || val.Value > col.Minimum) &&
				(!col.HasMaximum || val.Value < col.Maximum))
			outRow.Values = append(outRow.Values, val)
		}
		out = append(out, outRow)
	}
	return
}
