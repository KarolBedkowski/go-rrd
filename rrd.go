package main

import (
	"fmt"
	"io"
	"sort"
)

type (
	// RRD database
	RRD struct {
		storage  Storage
		filename string
		readonly bool

		columns  []RRDColumn
		archives []RRDArchive
	}

	// RRDColumn define one column
	RRDColumn struct {
		Name     string   // byte[16]
		Function Function // int32
	}

	// RRDArchive defines one archive
	RRDArchive struct {
		Name string // byte[16]
		Step int64
		Rows int32
	}

	// Row keep values for all columns
	Row struct {
		TS     int64
		Values []Value
	}

	// Rows is list of rows
	Rows []Row
)

type (
	// Storage save/load values from physical storage
	Storage interface {
		Create(filename string, columns []RRDColumn, archives []RRDArchive) error
		Open(filename string, readonly bool) ([]RRDColumn, []RRDArchive, error)
		Close() error
		Put(archive int, v ...Value) error
		Get(archive int, ts int64, columns []int) ([]Value, error)

		//Iterate over valid rows in optional begin-end range using RowsIterator
		//Loads only given columns.
		Iterate(archive int, begin, end int64, columns []int) (RowsIterator, error)
	}

	// RowsIterator allow iterating over database
	RowsIterator interface {
		Next() error
		TS() int64
		Value(column int) (*Value, error)
		Values() ([]Value, error)
	}
)

type (
	// RRDFileInfo holds informations about rrd file
	RRDFileInfo struct {
		Filename      string
		ColumnsCount  int
		ArchivesCount int

		Columns  []RRDColumn
		Archives []RRDArchiveInfo
	}

	// RRDArchiveInfo keeps information about archive
	RRDArchiveInfo struct {
		Name     string
		Rows     int
		Step     int64
		UsedRows int
		MinTS    int64
		MaxTS    int64
		Values   int64
	}
)

// OpenRRD open existing rrd database
func OpenRRD(filename string, readonly bool) (*RRD, error) {
	rrd := &RRD{
		filename: filename,
		storage:  &BinaryFileStorage{},
		readonly: readonly,
	}
	var err error
	rrd.columns, rrd.archives, err = rrd.storage.Open(filename, readonly)
	if err != nil {
		return nil, err
	}

	return rrd, nil
}

// NewRRD create new rrd database
func NewRRD(filename string, columns []RRDColumn, archives []RRDArchive) (*RRD, error) {
	rrd := &RRD{
		filename: filename,
		storage:  &BinaryFileStorage{},
		readonly: false,
	}
	err := rrd.storage.Create(filename, columns, archives)
	if err != nil {
		return nil, err
	}
	return rrd, nil
}

// Close rrd database
func (r *RRD) Close() error {
	return r.storage.Close()
}

func (r *RRD) String() string {
	return fmt.Sprintf("RRDFile[filename=%s, readolny=%v, columns=%#v, archives=%#v]",
		r.filename, r.readonly, r.columns, r.archives)
}

// Put value into database
func (r *RRD) Put(ts int64, col int, value float32) error {
	v := Value{
		TS:     ts,
		Valid:  true,
		Value:  value,
		Column: col,
	}
	return r.PutValues(v)
}

// PutValues - write
func (r *RRD) PutValues(values ...Value) error {
	if r.readonly {
		return fmt.Errorf("RRD file open as read-only")
	}

	cols := make([]int, 0, len(values))
	colsSum := 0
	for _, v := range values {
		cols = append(cols, v.Column)
		colsSum += v.Column
	}
	if colsSum == 0 { // no columns defined
		cols = r.allColumnsIDs()
	}

	// all values should have this same TS
	ts := values[0].TS

	for aID, a := range r.archives {
		fmt.Printf("Updating archive %s\n", a.Name)

		// get previous values
		preValues, err := r.storage.Get(aID, ts, cols)
		if err != nil {
			return err
		}
		// update
		updatedVal := make([]Value, 0, len(values))
		for i, v := range values {
			function := r.columns[v.Column].Function
			uv := v
			if preValues != nil {
				pv := preValues[i]
				uv = function.Apply(pv, v)
			}
			updatedVal = append(updatedVal, uv)
		}
		// write updated values
		if err = r.storage.Put(aID, updatedVal...); err != nil {
			return err
		}
	}
	return nil
}

// Get get values for timestamp.
func (r *RRD) Get(ts int64, columns []int) ([]Value, error) {
	if len(columns) == 0 {
		columns = r.allColumnsIDs()
	}

	for aID := range r.archives {
		values, err := r.storage.Get(aID, ts, columns)
		if values != nil || err != nil {
			return values, err
		}
	}
	return nil, nil
}

// Last return last timestamp from db
func (r *RRD) Last() (int64, error) {
	var last int64
	i, err := r.storage.Iterate(0, 0, -1, nil)
	if err != nil {
		return -1, err
	}

	for {
		err := i.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return -1, err
		}
		ts := i.TS()
		if ts > last {
			last = ts
		} else {
			break
		}
	}

	return last, nil
}

// GetRange finds all records in given range
func (r *RRD) GetRange(minTS, maxTS int64, columns []int) (Rows, error) {
	if len(columns) == 0 {
		columns = r.allColumnsIDs()
	}

	last, err := r.Last()
	if err != nil {
		return nil, err
	}
	var archiveID int
	for aID, a := range r.archives {
		archiveID = aID
		// archive range
		aMinTS := last - int64(a.Rows)*a.Step
		if minTS >= aMinTS {
			break
		}
	}
	i, err := r.storage.Iterate(archiveID, minTS, maxTS, columns)
	if err != nil {
		return nil, err
	}

	var rows Rows
	for {
		err := i.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		values, err := i.Values()
		if err != nil {
			return nil, err
		}
		rows = append(rows, Row{i.TS(), values})
	}

	sort.Sort(rows)
	return rows, err
}

func (r *RRD) allColumnsIDs() (cols []int) {
	for i := 0; i < len(r.columns); i++ {
		cols = append(cols, i)
	}
	return
}

// Info return RRDFileInfo structure for current file
func (r *RRD) Info() (*RRDFileInfo, error) {
	res := &RRDFileInfo{
		Filename:      r.filename,
		ColumnsCount:  len(r.columns),
		ArchivesCount: len(r.archives),
		Columns:       r.columns,
	}

	for aID, a := range r.archives {
		ainfo, err := r.infoArchive(aID, a)
		if err != nil {
			return nil, err
		}
		res.Archives = append(res.Archives, ainfo)
	}
	return res, nil
}

func (r *RRD) infoArchive(aID int, a RRDArchive) (RRDArchiveInfo, error) {
	arch := RRDArchiveInfo{
		Name: a.Name,
		Rows: int(a.Rows),
		Step: a.Step,
	}
	// Count rows & Values
	iter, err := r.storage.Iterate(aID, 0, -1, r.allColumnsIDs())
	if err != nil {
		return arch, err
	}
	for {
		if err := iter.Next(); err != nil {
			if err == io.EOF {
				break
			}
			return arch, err
		}
		arch.UsedRows++
		ts := iter.TS()
		if ts < arch.MinTS || arch.MinTS == 0 {
			arch.MinTS = ts
		}
		if ts > arch.MaxTS {
			arch.MaxTS = ts
		}
		values, err := iter.Values()
		if err != nil {
			return arch, err
		}
		for _, value := range values {
			if value.Valid {
				arch.Values++
			}
		}

	}
	return arch, nil
}

func (r Rows) Len() int           { return len(r) }
func (r Rows) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r Rows) Less(i, j int) bool { return r[i].TS < r[j].TS }