package main

import (
	//	"flag"
	"fmt"
	"math/rand"
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
		LogError("Columns definition error: " + err.Error())
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

	if c.IsSet("columns") {
		colsIDs, err := getContextParamIntList(c, "columns")
		if err != nil {
			LogError("Invalid --columns parameter: ", err.Error())
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

	ExitWhenErrors()

	f, err := OpenRRD(filename, false)
	defer close(f)
	if err != nil {
		LogFatal("Open db error: %s", err.Error())
		return
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
	colsIDs, err := getContextParamIntList(c, "columns")
	if err != nil {
		LogError("Invalid --columns parameter: ", err.Error())
	}
	timestamp, ok := dateToTs(ts)
	if !ok {
		LogError("Parse ts error")
	}

	ExitWhenErrors()

	f, err := OpenRRD(filename, true)
	defer close(f)
	if err != nil {
		LogFatal("Open db error: %s", err.Error())
	}

	separator := c.GlobalString("separator")

	if values, err := f.Get(timestamp, colsIDs...); err == nil {
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

	colsIDs, err := getContextParamIntList(c, "columns")
	if err != nil {
		LogError("Invalid --columns parameter: ", err.Error())
	}

	ExitWhenErrors()

	f, err := OpenRRD(filename, true)
	defer close(f)
	if err != nil {
		LogFatal("Open db error: %s", err.Error())
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
	includeInvalid := c.Bool("include_invalid")

	if rows, err := f.GetRange(tsMin, tsMax, colsIDs, includeInvalid); err == nil {
		for _, row := range rows {
			fmt.Print(timeFmt(row.TS), separator)
			for _, col := range row.Values {
				if col.Valid {
					fmt.Printf("%f", col.Value)
				}
				fmt.Print(separator)
			}
			fmt.Print("\n")
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

	if Debug {
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
		LogFatal("Error: %s", err.Error)
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
		LogFatal("Error: %s", err.Error)
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
		LogFatal("Error: %s", err.Error)
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
		LogFatal("Error: %s", err.Error)
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

func getContextParamIntList(c *cli.Context, param string) (res []int, err error) {
	if !c.IsSet(param) {
		return nil, nil
	}
	value := strings.TrimSpace(c.String(param))
	if value == "" {
		return nil, nil
	}
	return parseStrIntList(value)
}

func parseStrIntList(inp string) (res []int, err error) {
	for _, v := range strings.Split(inp, ",") {
		var vNum int
		vNum, err = strconv.Atoi(v)
		if err != nil {
			err = fmt.Errorf("invalid value '%s'", v)
			return
		}
		res = append(res, vNum)
	}
	return
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
		if len(cdef) > 2 || len(cdef) < 1 {
			return nil, fmt.Errorf("invalid column definition on index %d: '%s'", idx+1, v)
		}
		c := RRDColumn{}
		if len(cdef) == 2 {
			c.Name = cdef[1]
			if len(c.Name) > 16 {
				c.Name = c.Name[:16]
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
		for _, col := range info.Columns {
			fmt.Printf(" - %s - %s\n", col.Name, col.Function.String())
		}
		fmt.Printf("Archives: %d\n", info.ArchivesCount)
		for _, a := range info.Archives {
			fmt.Printf(" - Name: %s\n", a.Name)
			fmt.Printf("   Rows: %d\n", a.Rows)
			fmt.Printf("   Step: %d\n", a.Step)
			fmt.Printf("   TS range: %d - %d (%s-%s)\n", a.MinTS, a.MaxTS,
				time.Unix(a.MinTS, 0).String(), time.Unix(a.MaxTS, 0).String())
			fmt.Printf("   Used rows: %d (%0.1f%%)\n", a.UsedRows,
				100.0*float32(a.UsedRows)/float32(a.Rows))
			valuesInRows := float32(0)
			if a.UsedRows > 0 {
				valuesInRows = float32(a.Values) / float32(a.UsedRows*info.ColumnsCount)
			}
			valuesInDb := float32(a.Values) / float32(a.Rows*info.ColumnsCount)
			fmt.Printf("   Inserted values: %d (%0.1f%% in rows; %0.1f%% in database)\n",
				a.Values, 100.0*valuesInRows, 100.0*valuesInDb)
		}
	} else {
		fmt.Println("Error: " + err.Error())
	}
}

func processGlobalArgs(c *cli.Context) (ok bool) {
	Debug = c.GlobalBool("debug")
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
