package main

import (
	//	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codegangsta/cli"
)

func initDB(c *cli.Context) {
	if !processGlobalArgs(c) {
		return
	}
	filename, _ := getFilenameParam(c)
	cols := c.String("columns")
	if !c.IsSet("columns") || cols == "" {
		LogError("Missing number of columns (--columns)")
	}

	columns, err := parseColumnsDef(cols)
	if err != nil {
		LogError("Columns definition error: %s", err.Error())
	}

	archivesDef := c.String("archives")
	if !c.IsSet("archives") || archivesDef == "" {
		LogError("Missing archives definition (--archives)")
	}
	archives, err := parseArchiveDef(archivesDef)
	if err != nil {
		LogError("Archives definition error: " + err.Error())
	}

	ExitWhenErrors()

	f, err := NewRRD(filename, columns, archives)
	defer close(f)
	if err != nil {
		LogFatal("Init db error: " + err.Error())
		return
	}

	printRRDInfo(f)
}

func putValues(c *cli.Context) {
	if !processGlobalArgs(c) {
		return
	}
	filename, _ := getFilenameParam(c)

	ts := c.String("ts")
	if !c.IsSet("ts") || ts == "" {
		ts = "now"
	}
	timestamp, ok := dateToTs(ts)
	if !ok {
		LogError("Parse ts error", timestamp)
	}

	if len(c.Args()) == 0 {
		LogError("Missing values to put")
	}

	var values []Value

	for idx, a := range c.Args() {
		v, err := strconv.ParseFloat(a, 32)
		if err != nil {
			LogError("Invalid value '%s' on index %d", a, idx+1)
		}
		values = append(values, Value{
			TS:     timestamp,
			Value:  float32(v),
			Valid:  true,
			Column: idx,
		})
	}

	ExitWhenErrors()

	f, err := OpenRRD(filename, false)
	defer close(f)
	if err != nil {
		LogFatal("Open db error: %s", err.Error())
		return
	}

	if c.IsSet("columns") {
		cols := strings.Split(c.String("columns"), ",")
		colsIDs, err := f.ParseColumnsNames(cols)
		if err != nil {
			LogError("Invalid --columns parameter: %s", err.Error())
		}
		if len(colsIDs) != len(values) {
			LogError("Number of columns (--columns) don't match number of values")
		}
		for idx, c := range colsIDs {
			val := values[idx]
			val.Column = c
			values[idx] = val
		}
	}

	err = f.PutValues(values...)
	if err != nil {
		LogError("Put error: %s", err.Error())
	}
}

func getValue(c *cli.Context) {
	if !processGlobalArgs(c) {
		return
	}
	filename, _ := getFilenameParam(c)
	ts := c.String("ts")
	if !c.IsSet("ts") || ts == "" {
		LogError("Missing timestamp (--ts)")
	}

	ExitWhenErrors()

	f, err := OpenRRD(filename, true)
	defer close(f)
	if err != nil {
		LogFatal("Open db error: %s", err.Error())
	}

	var timestamp int64
	if strings.ToLower(ts) == "last" {
		var err error
		if timestamp, err = f.Last(); err != nil {
			LogError("Getting last TS error: %s", err.Error())
			return
		}

	} else {
		var ok bool
		if timestamp, ok = dateToTs(ts); !ok {
			LogError("Parse ts error")
			return
		}
	}

	var colsIDs []int
	if c.IsSet("columns") {
		var err error
		colsIDs, err = f.ParseColumnsNames(strings.Split(c.String("columns"), ","))
		if err != nil {
			LogError("Invalid --columns parameter: %s", err.Error())
			return
		}
	}

	separator := c.GlobalString("separator")

	if values, err := f.Get(timestamp, colsIDs...); err == nil && len(values) > 0 {
		fmt.Print(values[0].TS, separator)
		for _, val := range values {
			if val.Valid {
				fmt.Print(val.Value, separator)
			} else {
				fmt.Print(separator)
			}
		}
		fmt.Println()
	} else {
		Log("Missing value")
	}
}

func getRangeValues(c *cli.Context) {
	if !processGlobalArgs(c) {
		return
	}
	filename, _ := getFilenameParam(c)
	tsMin := int64(0)
	tsMinStr := c.String("begin")
	if c.IsSet("begin") && tsMinStr != "" {
		var ok bool
		tsMin, ok = dateToTs(tsMinStr)
		if !ok {
			LogError("Parsing begin date error")
		}
	}
	tsMaxStr := c.String("end")
	if !c.IsSet("end") || tsMaxStr == "" {
		tsMaxStr = "now"
	}

	tsMax, ok := dateToTs(tsMaxStr)
	if !ok {
		LogError("Parsing end date error")
	}

	ExitWhenErrors()

	f, err := OpenRRD(filename, true)
	defer close(f)
	if err != nil {
		LogFatal("Open db error: %s", err.Error())
	}

	var colsIDs []int
	if c.IsSet("columns") {
		var err error
		colsIDs, err = f.ParseColumnsNames(strings.Split(c.String("columns"), ","))
		if err != nil {
			LogError("Invalid --columns parameter: %s", err.Error())
			return
		}
	}

	var timeFmt func(int64) string
	if c.GlobalIsSet("format-ts") {
		format := c.GlobalString("custom-ts-format")
		if format == "" {
			format = time.RFC3339
		}
		timeFmt = func(ts int64) string {
			return time.Unix(ts, 0).Format(format)
		}
	} else {
		timeFmt = func(ts int64) string {
			return fmt.Sprintf("%10d", ts)
		}
	}

	separator := c.GlobalString("separator")
	includeInvalid := c.Bool("include-invalid")
	noRealTime := c.GlobalBool("no-rt")
	separate := c.Bool("separate-valid-groups")

	if rows, err := f.GetRange(tsMin, tsMax, colsIDs, includeInvalid, !noRealTime); err == nil {
		if c.IsSet("fix-ranges") {
			// mark values not matching min-max range
			rows = RemoveInvalidVals(rows, f.Columns())
		}
		if c.IsSet("average-result") {
			if step := c.Int("average-result"); step > 1 {
				rows = AverageByTime(rows, int64(step))
			} else {
				LogError("Invalid --average-result: %s; ignoring", step)
			}
		} else if c.IsSet("average-max-count") {
			if cnt := c.Int("average-max-count"); cnt > 1 {
				rows = AverageToNumber(rows, cnt)
			} else {
				LogError("Invalid --average-max-count: %s; ignoring", cnt)
			}
		}
		prevValid := true
		for _, row := range rows {
			valid := false
			outp := timeFmt(row.TS) + separator
			for _, col := range row.Values {
				if col.Valid {
					outp += fmt.Sprintf("%f", col.Value)
					valid = true
				}
				outp += separator
			}
			if separate && !valid {
				if prevValid {
					fmt.Print("\n")
				}
			} else {
				fmt.Println(outp)
			}
			prevValid = valid
		}
	} else {
		LogFatal("Error: %s", err.Error())
	}
}

func showInfo(c *cli.Context) {
	if !processGlobalArgs(c) {
		return
	}
	filename, _ := getFilenameParam(c)

	ExitWhenErrors()

	f, err := OpenRRD(filename, true)
	defer close(f)
	if err != nil {
		LogFatal("Open db error: %s", err.Error())
		return
	}

	printRRDInfo(f)

	if Debug > 1 {
		fmt.Println(f.LowLevelDebugDump())
	}
}

func showLast(c *cli.Context) {
	if !processGlobalArgs(c) {
		return
	}
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}

	ExitWhenErrors()

	f, err := OpenRRD(filename, true)
	defer close(f)
	if err != nil {
		LogFatal("Open db error: %s", err.Error())
		return
	}

	fmt.Println(f.Last())
}

func startServer(c *cli.Context) {
	if !processGlobalArgs(c) {
		return
	}
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}

	ExitWhenErrors()

	server := Server{
		Address:    c.String("address"),
		DbFilename: filename,
	}
	server.Start()
}

func dumpData(c *cli.Context) {
	if !processGlobalArgs(c) {
		return
	}
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}

	output := c.String("output")
	if !c.IsSet("output") || output == "" {
		LogError("Missing output file name (--output)")
	}

	ExitWhenErrors()

	f, err := OpenRRD(filename, true)
	defer close(f)
	if err != nil {
		LogFatal("Open db error: %s", err.Error())
		return
	}

	if err := f.Dump(output); err != nil {
		LogFatal("Error: %s", err.Error())
	} else {
		Log("Done")
	}
}

func loadData(c *cli.Context) {
	if !processGlobalArgs(c) {
		return
	}
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}

	input := c.String("input")
	if !c.IsSet("input") || input == "" {
		LogError("Missing input file name (--input)")
	}

	ExitWhenErrors()

	f, err := LoadDumpRRD(input, filename)
	defer close(f)
	if err != nil {
		LogFatal("Error: %s", err.Error())
	} else {
		Log("Done")
	}
}

func modifyAddColumns(c *cli.Context) {
	if !processGlobalArgs(c) {
		return
	}
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}
	cols := c.String("columns")
	if !c.IsSet("columns") || cols == "" {
		LogError("Missing columns definition (--columns)")
	}

	columns, err := parseColumnsDef(cols)
	if err != nil {
		LogError("Columns definition error: " + err.Error())
	}

	ExitWhenErrors()

	if err := ModifyAddColumns(filename, columns); err != nil {
		LogFatal("Error: %s", err.Error())
	} else {
		Log("Done")
	}
}

func modifyChangeColumn(c *cli.Context) {
	if !processGlobalArgs(c) {
		return
	}
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}

	colS := c.String("column")
	if !c.IsSet("column") || colS == "" {
		LogError("Missing column (--column)")
	}

	ExitWhenErrors()

	f, err := OpenRRD(filename, false)
	defer close(f)
	if err != nil {
		LogFatal("Open db error: %s", err.Error())
		return
	}

	var colIdx int
	colIdx, err = f.ParseColumnName(colS)
	if err != nil {
		LogError("Invalid column (--column): %s", err.Error())
		return
	}

	col := f.GetColumn(colIdx)

	if c.IsSet("name") {
		// change name
		name := strings.TrimSpace(c.String("name"))
		if len(name) > 16 {
			col.Name = name[:16]
		} else if len(name) > 0 {
			col.Name = name
		}
	}

	if c.Bool("no-min") {
		col.HasMinimum = false
	} else if c.IsSet("min") {
		// set minimum value
		col.Minimum = float32(c.Float64("min"))
		col.HasMinimum = true
	}

	if c.Bool("no-max") {
		col.HasMaximum = false
	} else if c.IsSet("max") {
		// set maximum value
		col.Maximum = float32(c.Float64("max"))
		col.HasMaximum = true
	}

	f.SetColumn(colIdx, col)

	tmpName := filename + ".new"
	if err = f.SaveAs(tmpName); err != nil {
		LogFatal("Save file to %s error: %s", filename, err.Error())
		return
	}

	f.Close()
	f = nil

	LogDebug("delete old file")
	if err := os.Remove(filename); err != nil {
		LogFatal("removing old file %s error: %s", filename, err.Error())
		return
	}

	LogDebug("rename temp file")
	if err := os.Rename(tmpName, filename); err != nil {
		LogFatal("renaming file %s to %s error: %s", tmpName, filename, err.Error())
	} else {
		Log("Done")
	}
}

func modifyDelColumns(c *cli.Context) {
	if !processGlobalArgs(c) {
		return
	}
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}
	cols := c.String("columns")
	if !c.IsSet("columns") || cols == "" {
		LogError("Missing columns to delete (--columns)")
	}

	r, err := OpenRRD(filename, true)
	if err != nil {
		close(r)
		return
	}
	columns, err := r.ParseColumnsNames(strings.Split(cols, ","))
	if err != nil {
		LogError("Columns definition error: " + err.Error())
	}
	r.Close()

	ExitWhenErrors()

	if err := ModifyDelColumns(filename, columns); err != nil {
		LogFatal("Error: %s", err.Error())
	} else {
		Log("Done")
	}
}

func modifyAddArchives(c *cli.Context) {
	if !processGlobalArgs(c) {
		return
	}
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}

	archivesDef := c.String("archives")
	if !c.IsSet("archives") || archivesDef == "" {
		LogError("Missing archives definition (--archives)")
	}
	archives, err := parseArchiveDef(archivesDef)
	if err != nil {
		LogError("Archives definition error: " + err.Error())
	}

	ExitWhenErrors()

	if err := ModifyAddArchives(filename, archives); err != nil {
		LogFatal("Error: %s", err.Error())
	} else {
		Log("Done")
	}
}

func modifyDelArchives(c *cli.Context) {
	if !processGlobalArgs(c) {
		return
	}
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}

	archivesDef := c.String("archives")
	if !c.IsSet("archives") || archivesDef == "" {
		LogError("Missing archives list (--archives)")
	}

	ExitWhenErrors()

	f, err := OpenRRD(filename, false)
	if err != nil {
		LogFatal("Open db error: %s", err.Error())
		close(f)
		return
	}

	archives, err := f.ParseArchiveNames(strings.Split(archivesDef, ","))
	if err != nil {
		LogError("Archives definition error: " + err.Error())
		close(f)
		return
	}
	close(f)

	if err := ModifyDelArchives(filename, archives); err != nil {
		LogFatal("Error: %s", err.Error())
	} else {
		Log("Done")
	}
}

func modifyResizeArchive(c *cli.Context) {
	if !processGlobalArgs(c) {
		return
	}
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}

	archivesDef := c.String("archive")
	if !c.IsSet("archive") || archivesDef == "" {
		LogError("Missing archive (--archive)")
	}
	rows := c.Int("rows")
	if rows < 1 {
		LogError("Invalid number of rows (--rows)")
	}

	ExitWhenErrors()

	f, err := OpenRRD(filename, false)
	if err != nil {
		LogFatal("Open db error: %s", err.Error())
		close(f)
		return
	}

	archive, err := f.ParseArchiveName(archivesDef)
	if err != nil {
		LogError("Archives definition error: " + err.Error())
		close(f)
		return
	}
	close(f)

	if err := ModifyResizeArchive(filename, archive, rows); err != nil {
		LogFatal("Error: %s", err.Error())
	} else {
		Log("Done")
	}
}

func updateRRDfile(c *cli.Context) {
	if !processGlobalArgs(c) {
		return
	}
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}

	ExitWhenErrors()

	if err := UpdateRRD(filename); err != nil {
		LogFatal("Error: %s", err.Error())
	} else {
		Log("Done")
	}
}

func genRandomData(c *cli.Context) {
	if !processGlobalArgs(c) {
		return
	}
	filename, _ := getFilenameParam(c)
	tsMin := int64(0)
	tsMinStr := c.String("begin")
	if c.IsSet("begin") && tsMinStr != "" {
		var ok bool
		tsMin, ok = dateToTs(tsMinStr)
		if !ok {
			LogError("Parsing begin date error")
		}
	}
	tsMaxStr := c.String("end")
	if !c.IsSet("end") || tsMaxStr == "" {
		tsMaxStr = "now"
	}

	tsMax, ok := dateToTs(tsMaxStr)
	if !ok {
		LogError("Parsing end date error")
	}

	step := int64(c.Int("step"))
	if !c.IsSet("step") || step < 1 {
		LogError("Invalid or missing step (--step)")
	}

	ExitWhenErrors()

	f, err := OpenRRD(filename, false)
	defer close(f)
	if err != nil {
		LogFatal("Open db error: %s", err.Error())
	}

	cols := f.allColumnsIDs()
	for ts := tsMin; ts <= tsMax; ts = ts + step {
		var values []Value
		for _, c := range cols {
			values = append(values, Value{TS: ts, Value: rand.Float32(), Valid: true, Column: c})
		}
		err = f.PutValues(values...)
		if err != nil {
			LogError("Put error: %s", err.Error())
		}
	}
}

var timeFormats = []string{
	time.RFC822,
	time.RFC822Z,
	time.RFC3339,
	time.RFC3339Nano,
	"2006-01-02T15:04:05",
	"2006-01-02T15:04",
	"2006-01-02T15",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04",
	"2006-01-02 15",
	"2006-01-02",
}

func dateToTs(ts string) (int64, bool) {
	if ts == "N" || ts == "NOW" || ts == "now" {
		return time.Now().Unix(), true
	}
	if res, err := strconv.ParseInt(ts, 10, 64); err == nil {
		return res, true
	}

	if d, err := time.ParseDuration(ts); err == nil {
		return time.Now().Add(d).Unix(), true
	}

	for _, format := range timeFormats {
		if t, err := time.Parse(format, ts); err == nil {
			return t.Unix(), true
		}
	}
	return time.Now().Unix(), false
}

func parseArchiveDef(inp string) (archives []RRDArchive, err error) {
	for idx, v := range strings.Split(inp, ",") {
		adef := strings.Split(v, ":")
		a := RRDArchive{}
		if len(adef) > 3 || len(adef) < 2 {
			return nil, fmt.Errorf("invalid archive definition on index %d: '%s'", idx+1, v)
		}
		if len(adef) == 3 {
			a.Name = adef[2]
			if len(a.Name) > 16 {
				a.Name = a.Name[:16]
			}
		} else {
			a.Name = fmt.Sprintf("a%02d", idx+1)
		}
		var numRows int
		numRows, err = strconv.Atoi(adef[0])
		if err != nil {
			return nil, fmt.Errorf("invalid archive definition on index %d: '%s' - invalid rows number", idx+1, v)
		}
		a.Rows = int32(numRows)
		var step int
		step, err = strconv.Atoi(adef[1])
		if err != nil {
			return nil, fmt.Errorf("invalid archive definition on index %d: '%s' - invalid step", idx+1, v)
		}
		a.Step = int64(step)
		archives = append(archives, a)
	}
	return
}

func parseColumnsDef(inp string) (columns []RRDColumn, err error) {
	for idx, v := range strings.Split(inp, ",") {
		cdef := strings.Split(v, ":")
		if len(cdef) < 1 {
			return nil, fmt.Errorf("invalid column definition on index %d: '%s'", idx+1, v)
		}
		c := RRDColumn{}
		if len(cdef) > 1 {
			c.Name = cdef[1]
			if len(c.Name) > 16 {
				c.Name = c.Name[:16]
			}
		}
		if len(cdef) > 2 { // min value
			minS := strings.TrimSpace(cdef[2])
			if len(minS) > 0 {
				var v float64
				v, err = strconv.ParseFloat(minS, 32)
				if err != nil {
					return nil, fmt.Errorf("invalid min value for column %d: %v; %s", idx+1, minS, err.Error())
				}
				c.Minimum = float32(v)
				c.HasMinimum = true
			}
		}
		if len(cdef) > 2 { // max value
			maxS := strings.TrimSpace(cdef[3])
			if len(maxS) > 0 {
				var v float64
				v, err = strconv.ParseFloat(maxS, 32)
				if err != nil {
					return nil, fmt.Errorf("invalid max value for column %d: %v; %s", idx+1, maxS, err.Error())
				}
				c.Maximum = float32(v)
				c.HasMaximum = true
			}
		}
		if c.Name == "" {
			c.Name = fmt.Sprintf("c%02d", idx+1)
		}
		funcID, ok := ParseFunctionName(cdef[0])
		if !ok {
			return nil, fmt.Errorf("invalid column definition on index %d: '%s' - wrong function", idx+1, v)
		}
		c.Function = funcID
		columns = append(columns, c)
	}
	return
}

func printRRDInfo(f *RRD) {
	if info, err := f.Info(); err == nil {
		fmt.Printf("Filename: %s\n", info.Filename)
		fmt.Printf("Columns: %d\n", info.ColumnsCount)
		for idx, col := range info.Columns {
			fmt.Printf(" %2d. %-16s - %s", idx, col.Name, col.Function.String())
			if col.HasMinimum {
				fmt.Printf(" min: %f", col.Minimum)
			}
			if col.HasMaximum {
				fmt.Printf(" max: %f", col.Maximum)
			}
			fmt.Println("")
		}
		fmt.Printf("Archives: %d\n", info.ArchivesCount)
		for idx, a := range info.Archives {
			fmt.Printf(" %2d. %-16s\n", idx, a.Name)
			fmt.Printf("     Rows: %5d   Step: %d\n", a.Rows, a.Step)
			fmt.Printf("     TS range: %d - %d (%s - %s)\n", a.MinTS, a.MaxTS,
				time.Unix(a.MinTS, 0).String(), time.Unix(a.MaxTS, 0).String())
			fmt.Printf("     Used rows: %d (%0.1f%%)\n", a.UsedRows,
				100.0*float32(a.UsedRows)/float32(a.Rows))
			valuesInRows := float32(0)
			if a.UsedRows > 0 {
				valuesInRows = float32(a.Values) / float32(a.UsedRows*info.ColumnsCount)
			}
			valuesInDb := float32(a.Values) / float32(a.Rows*info.ColumnsCount)
			fmt.Printf("     Inserted values: %d (%0.1f%% in rows; %0.1f%% in database)\n",
				a.Values, 100.0*valuesInRows, 100.0*valuesInDb)
		}
	} else {
		fmt.Println("Error: " + err.Error())
	}
}

func processGlobalArgs(c *cli.Context) (ok bool) {
	if c.GlobalIsSet("debug-level") {
		Debug = c.GlobalInt("debug-level")
	}
	if c.GlobalBool("debug") && Debug <= 0 {
		Debug = 1
	}
	return true
}

func close(r *RRD) {
	if r == nil {
		return
	}

	if err := r.Close(); err != nil {
		LogError("Closing db error: %s", err.Error())
	}
}
