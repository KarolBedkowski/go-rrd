package main

import (
	"flag"
	"fmt"
	"strconv"
	"time"
)

func main() {
	filename := flag.String("file", "database.rdb", "Database file name")
	rows := flag.Uint("rows", 0, "Number of rows")
	cols := flag.Uint("cols", 0, "Number of columns")
	step := flag.Uint64("step", 0, "Interval between rows")
	init := flag.Bool("init", false, "Init database file")
	put := flag.Bool("put", false, "Put data into db (with -col, -ts, -value)")
	get := flag.Bool("get", false, "Get data from db (with -col, -ts)")
	col := flag.Uint("col", 0, "Column")
	value := flag.Float64("value", 0.0, "Value to put into db")
	ts := flag.String("ts", "", "Time stamp")
	getRange := flag.Bool("range", false, "Get data from range (-min/-max)")
	rMin := flag.String("min", "", "Range start")
	rMax := flag.String("max", "", "Range end")
	flag.Parse()

	if *filename == "" {
		fmt.Println("Missing database file name")
		return
	}

	if *init {
		if *rows < 1 {
			fmt.Println("Missing number of rows (-rows)")
			return
		}
		if *cols < 1 {
			fmt.Println("Missing number of cols (-cols)")
			return
		}
		if *step < 1 {
			fmt.Println("Missing step (-step)")
			return
		}
		initDB(*filename, *cols, *rows, *step)
		return
	}
	if *put {
		if *ts == "" {
			fmt.Println("Missing -ts")
			return
		}
		timestamp, ok := dateToTs(*ts)
		if !ok {
			fmt.Println("Parse ts error")
			return
		}
		putValue(*filename, int32(*col), timestamp.Unix(), *value)
		return
	}
	if *get {
		if *ts == "" {
			fmt.Println("Missing -ts")
			return
		}
		timestamp, ok := dateToTs(*ts)
		if !ok {
			fmt.Println("Parse ts error")
			return
		}
		getValue(*filename, int32(*col), timestamp.Unix())
		return
	}
	if *getRange {
		minTS := int64(1)
		maxTS := int64(-1)
		if *rMin != "" {
			if min, ok := dateToTs(*rMin); ok {
				minTS = min.Unix()
			}
		}
		if *rMax != "" {
			if max, ok := dateToTs(*rMax); ok {
				maxTS = max.Unix()
			}
		}
		getRangeValues(*filename, minTS, maxTS)
		return
	}
}

func initDB(filename string, cols, rows uint, step uint64) {
	f, err := NewRRDFile(filename, int32(cols), int32(rows), int64(step))
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

func putValue(filename string, col int32, ts int64, value float64) {
	f, err := OpenRRD(filename, false)
	if err != nil {
		fmt.Println("Open db error: " + err.Error())
		return
	}

	fmt.Println(f.String())

	err = f.Put(ts, col, value)
	if err != nil {
		fmt.Println("Put error: " + err.Error())
	}
	err = f.Close()
	if err != nil {
		fmt.Println("Close db error: " + err.Error())
		return
	}
}

func getValue(filename string, col int32, ts int64) {
	f, err := OpenRRD(filename, false)
	if err != nil {
		fmt.Println("Open db error: " + err.Error())
		return
	}
	if value, err := f.Get(ts, col); err == nil {
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

func getRangeValues(filename string, tsMin, tsMax int64) {
	f, err := OpenRRD(filename, false)
	if err != nil {
		fmt.Println("Open db error: " + err.Error())
		return
	}
	if rows, err := f.GetRange(tsMin, tsMax); err == nil {
		for _, row := range rows {
			fmt.Printf("%10d\t", row.TS)
			for _, col := range row.Cols {
				if col.Valid {
					fmt.Printf("%v", col.Value)
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
