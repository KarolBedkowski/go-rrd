package main

import (
	//	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codegangsta/cli"
)

func initDB(c *cli.Context) {
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}
	cols := c.String("columns")
	if !c.IsSet("columns") || cols == "" {
		fmt.Println("Missing number of columns (--columns)")
		return
	}

	columns, err := parseColumnsDef(cols)
	if err != nil {
		fmt.Println("Columns definition error: " + err.Error())
		return
	}

	archivesDef := c.String("archives")
	if !c.IsSet("archives") || archivesDef == "" {
		fmt.Println("Missing archives definition (--archives)")
		return
	}
	archives, err := parseArchiveDef(archivesDef)
	if err != nil {
		fmt.Println("Archives definition error: " + err.Error())
		return
	}

	f, err := NewRRD(filename, columns, archives)
	if err != nil {
		fmt.Println("Init db error: " + err.Error())
		return
	}

	printRRDInfo(f)

	err = f.Close()
	if err != nil {
		fmt.Println("Closing db error: " + err.Error())
		return
	}
}

func putValues(c *cli.Context) {
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}
	ts := c.String("ts")
	if !c.IsSet("ts") || ts == "" {
		ts = "now"
	}
	timestamp, ok := dateToTs(ts)
	if !ok {
		fmt.Println("Parse ts error", timestamp)
		return
	}

	if len(c.Args()) == 0 {
		fmt.Println("Missing values to put")
		return
	}

	var values []Value

	for idx, a := range c.Args() {
		v, err := strconv.ParseFloat(a, 32)
		if err != nil {
			fmt.Printf("Invalid value '%s' on index %d", a, idx+1)
			return
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
			fmt.Println("Invalid --columns parameter: ", err.Error())
			return
		}
		if len(colsIDs) != len(values) {
			fmt.Println("Number of columns (--columns) don't match number of values")
			return
		}
		for idx, c := range colsIDs {
			val := values[idx]
			val.Column = c
			values[idx] = val
		}
	}

	f, err := OpenRRD(filename, false)
	if err != nil {
		fmt.Println("Open db error: " + err.Error())
		return
	}

	err = f.PutValues(values...)
	if err != nil {
		fmt.Println("Put error: " + err.Error())
	}
	err = f.Close()
	if err != nil {
		fmt.Println("Close db error: " + err.Error())
		return
	}
}

func getValue(c *cli.Context) {
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}
	ts := c.String("ts")
	if !c.IsSet("ts") || ts == "" {
		fmt.Println("Missing timestamp (--ts)")
		return
	}
	colsIDs, err := getContextParamIntList(c, "columns")
	if err != nil {
		fmt.Println("Invalid --columns parameter: ", err.Error())
		return
	}
	timestamp, ok := dateToTs(ts)
	if !ok {
		fmt.Println("Parse ts error")
		return
	}
	f, err := OpenRRD(filename, true)
	if err != nil {
		fmt.Println("Open db error: " + err.Error())
		return
	}

	if values, err := f.Get(timestamp, colsIDs...); err == nil {
		for _, val := range values {
			if val.Valid {
				fmt.Print(val.Value, "; ")
			} else {
				fmt.Print("; ")
			}
		}
		fmt.Println()
	} else {
		fmt.Println("Missing value")
	}
	err = f.Close()
	if err != nil {
		fmt.Println("Closing db error: " + err.Error())
		return
	}
}

func getRangeValues(c *cli.Context) {
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}
	tsMin := int64(0)
	tsMinStr := c.String("begin")
	if c.IsSet("begin") && tsMinStr != "" {
		tsMin, ok = dateToTs(tsMinStr)
		if !ok {
			fmt.Println("Parsing begin date error")
			return
		}
	}
	tsMaxStr := c.String("end")
	if !c.IsSet("end") || tsMaxStr == "" {
		tsMaxStr = "now"
	}
	var tsMax int64
	tsMax, ok = dateToTs(tsMaxStr)
	if !ok {
		fmt.Println("Parsing end date error")
		return
	}

	colsIDs, err := getContextParamIntList(c, "columns")
	if err != nil {
		fmt.Println("Invalid --columns parameter: ", err.Error())
		return
	}

	f, err := OpenRRD(filename, true)
	if err != nil {
		fmt.Println("Open db error: " + err.Error())
		return
	}

	var timeFmt func(int64) string
	if c.GlobalIsSet("format-ts") {
		format := c.String("custom-ts-format")
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

	if rows, err := f.GetRange(tsMin, tsMax, colsIDs); err == nil {
		for _, row := range rows {
			fmt.Print(timeFmt(row.TS), "\t")
			for _, col := range row.Values {
				if col.Valid {
					fmt.Printf("%f", col.Value)
				}
				fmt.Print("\t")
			}
			fmt.Print("\n")
		}
	} else {
		fmt.Println("Error: " + err.Error())
	}
	err = f.Close()
	if err != nil {
		fmt.Println("Closing db error: " + err.Error())
		return
	}
}

func showInfo(c *cli.Context) {
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}

	f, err := OpenRRD(filename, true)
	if err != nil {
		fmt.Println("Open db error: " + err.Error())
		return
	}

	printRRDInfo(f)

	err = f.Close()
	if err != nil {
		fmt.Println("Closing db error: " + err.Error())
		return
	}
}

func showLast(c *cli.Context) {
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}

	f, err := OpenRRD(filename, true)
	if err != nil {
		fmt.Println("Open db error: " + err.Error())
		return
	}

	fmt.Println(f.Last())

	if err = f.Close(); err != nil {
		fmt.Println("Closing db error: " + err.Error())
		return
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
			fmt.Printf("   TS range: %d - %d\n", a.MinTS, a.MaxTS)
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
