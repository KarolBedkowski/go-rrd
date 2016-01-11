package main

import (
	//	"flag"
	"fmt"
	"github.com/codegangsta/cli"
	"strconv"
	"time"
)

func initDB(c *cli.Context) {
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}
	rows := c.Int("rows")
	if !c.IsSet("rows") || rows < 1 {
		fmt.Println("Missing number of rows (-rows)")
		return
	}
	cols := c.Int("cols")
	if !c.IsSet("cols") || cols < 1 {
		fmt.Println("Missing number of cols (-cols)")
		return
	}
	step := c.Int("step")
	if !c.IsSet("step") || step < 1 {
		fmt.Println("Missing step (-step)")
		return
	}

	function := c.String("function")
	funcID, ok := ParseFunctionName(function)
	if !ok {
		fmt.Println("Unknown function")
		return
	}

	var archives []RRDArchive
	archives = append(archives, RRDArchive{
		Name: "arch1",
		Step: 15,     // 15 sek
		Rows: 4 * 60, // 1h
	})
	archives = append(archives, RRDArchive{
		Name: "arch2",
		Step: 60,
		Rows: 60 * 24, // 24h
	})
	archives = append(archives, RRDArchive{
		Name: "arch3",
		Step: 300,         // 5m
		Rows: 12 * 24 * 7, // 7d
	})
	archives = append(archives, RRDArchive{
		Name: "arch4",
		Step: 3600,    // 1h
		Rows: 24 * 31, // 1m
	})

	colsDef := make([]RRDColumn, 0, cols)
	for i := 0; i < cols; i++ {
		colsDef = append(colsDef, RRDColumn{
			Name:     fmt.Sprintf("col%02d", i),
			Function: funcID,
		})
	}

	f, err := NewRRDFile(filename, colsDef, archives)
	if err != nil {
		fmt.Println("Init db error: " + err.Error())
		return
	}
	fmt.Println(f.String())

	err = f.Close()
	if err != nil {
		fmt.Println("Init db error: " + err.Error())
		return
	}
}

func putValue(c *cli.Context) {
	filename, ok := getFilenameParam(c)
	if !ok {
		return
	}
	ts := c.String("ts")
	if !c.IsSet("ts") || ts == "" {
		fmt.Println("Missing time stamp (--ts)")
		return
	}
	col := c.Int("col")
	if !c.IsSet("col") || col < 0 {
		fmt.Println("Missing column (--col)")
		return
	}
	if !c.IsSet("value") {
		fmt.Println("Missing step (--value)")
		return
	}
	value := c.Float64("value")

	timestamp, ok := dateToTs(ts)
	if !ok {
		fmt.Println("Parse ts error")
		return
	}

	f, err := OpenRRD(filename, false)
	if err != nil {
		fmt.Println("Open db error: " + err.Error())
		return
	}

	fmt.Println(f.String())

	err = f.Put(timestamp.Unix(), col, float32(value))
	if err != nil {
		fmt.Println("Put error: " + err.Error())
	}
	err = f.Close()
	if err != nil {
		fmt.Println("Close db error: " + err.Error())
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
		fmt.Println("Missing time stamp (--ts)")
		return
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

	var values []float64

	for idx, a := range c.Args() {
		v, err := strconv.ParseFloat(a, 64)
		if err != nil {
			fmt.Printf("Invalid value '%s' on index %d", a, idx+1)
			return
		}
		values = append(values, v)
	}

	f, err := OpenRRD(filename, false)
	if err != nil {
		fmt.Println("Open db error: " + err.Error())
		return
	}

	//	err = f.PutRow(timestamp.Unix(), values)
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
		fmt.Println("Missing number of rows (-rows)")
		return
	}
	col := c.Int("col")
	if !c.IsSet("col") || col < 0 {
		fmt.Println("Missing column (-col)")
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
	if value, err := f.Get(timestamp.Unix(), col); err == nil {
		fmt.Println(value.String())
	} else {
		fmt.Println("Missing value")
	}
	err = f.Close()
	if err != nil {
		fmt.Println("Init db error: " + err.Error())
		return
	}
}

func getRangeValues(c *cli.Context) {
	/*
		filename, ok := getFilenameParam(c)
		if !ok {
			return
		}
		tsMin := int64(0)
		tsMinStr := c.String("ts-min")
		if c.IsSet("ts-min") && tsMinStr != "" {
			if min, ok := dateToTs(tsMinStr); ok {
				tsMin = min.Unix()
			}
		}
		tsMax := int64(-1)
		tsMaxStr := c.String("ts-max")
		if c.IsSet("ts-max") && tsMaxStr != "" {
			if max, ok := dateToTs(tsMaxStr); ok {
				tsMax = max.Unix()
			}
		}

		f, err := OpenRRD(filename, true)
		if err != nil {
			fmt.Println("Open db error: " + err.Error())
			return
		}
		if rows, err := f.GetRange(tsMin, tsMax); err == nil {
			for _, row := range rows {
				fmt.Printf("%10d\t", row.TS)
				for _, col := range row.Cols {
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
			fmt.Println("Init db error: " + err.Error())
			return
		}
	*/
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

	if info, err := f.Info(); err == nil {
		fmt.Printf("Filename: %s\n", info.Filename)
		fmt.Printf("Rows: %d\n", info.Rows)
		fmt.Printf("Cols: %d\n", info.Cols)
		fmt.Printf("Step: %d\n", info.Step)
		fmt.Printf("Used rows: %d (%0.1f%%)\n", info.UsedRows,
			100.0*float32(info.UsedRows)/float32(info.Rows))
		fmt.Printf("TS range: %d - %d\n", info.MinTS, info.MaxTS)
		fmt.Printf("Function: %s\n", info.Function.String())

		valuesInRows := float32(0)
		if info.UsedRows > 0 {
			valuesInRows = float32(info.Values) / float32(info.UsedRows*info.Cols)
		}
		valuesInDb := float32(info.Values) / float32(info.Rows*info.Cols)
		fmt.Printf("Inserted values: %d (%0.1f%% in rows; %0.1f%% in database)\n",
			info.Values, 100.0*valuesInRows, 100.0*valuesInDb)
	}

	err = f.Close()
	if err != nil {
		fmt.Println("Init db error: " + err.Error())
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

	//	fmt.Println(f.Last())

	if err = f.Close(); err != nil {
		fmt.Println("Init db error: " + err.Error())
		return
	}

}

func dateToTs(ts string) (time.Time, bool) {
	if ts == "N" || ts == "NOW" || ts == "now" {
		return time.Now(), true
	}
	if res, err := strconv.ParseInt(ts, 10, 64); err == nil {
		return time.Unix(res, 0), true
	}

	if t, err := time.Parse(time.RFC822, ts); err == nil {
		return t, true
	}
	if t, err := time.Parse(time.RFC822Z, ts); err == nil {
		return t, true
	}
	if t, err := time.Parse(time.RFC3339, ts); err == nil {
		return t, true
	}
	if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
		return t, true
	}
	return time.Now(), false
}
