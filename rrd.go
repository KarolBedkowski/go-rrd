package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type (
	// RRD database
	RRD struct {
		mu sync.RWMutex

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
		Put(archive int, ts int64, v ...Value) error
		Get(archive int, ts int64, columns []int) ([]Value, error)

		//Iterate over valid rows in optional begin-end range using RowsIterator
		//Loads only given columns.
		Iterate(archive int, begin, end int64, columns []int) (RowsIterator, error)
		Flush()
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
		Name         string
		Rows         int
		Step         int64
		UsedRows     int
		MinTS        int64
		MaxTS        int64
		Values       int64
		DataRangeMin int64
	}
)

// OpenRRD open existing rrd database
func OpenRRD(filename string, readonly bool) (*RRD, error) {
	LogDebug("OpenRRD filename=%s, readonly=%v", filename, readonly)
	rrd := &RRD{
		filename: filename,
		storage:  &BinaryFileStorage{},
		readonly: readonly,
	}
	var err error
	rrd.columns, rrd.archives, err = rrd.storage.Open(filename, readonly)
	return rrd, err
}

// NewRRD create new rrd database
func NewRRD(filename string, columns []RRDColumn, archives []RRDArchive) (*RRD, error) {
	LogDebug("NewRRD filename=%s, columns=%v, archives=%v",
		filename, columns, archives)
	rrd := &RRD{
		filename: filename,
		storage:  &BinaryFileStorage{},
		readonly: false,
		columns:  columns,
		archives: archives,
	}
	err := rrd.storage.Create(filename, columns, archives)
	return rrd, err
}

// Close rrd database
func (r *RRD) Close() error {
	LogDebug("RRD.Close")
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.storage.Close()
}

func (r *RRD) Flush() {
	LogDebug("RRD.Close")
	r.mu.Lock()
	defer r.mu.Unlock()

	r.storage.Flush()
}

func (r *RRD) String() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return fmt.Sprintf("RRDFile[filename=%s, readolny=%v, columns=%#v, archives=%#v]",
		r.filename, r.readonly, r.columns, r.archives)
}

func (r *RRD) ColumnName(col int) string {
	return r.columns[col].Name
}

// Put value into database
func (r *RRD) Put(ts int64, col int, value float32) error {
	LogDebug("RRD.Put ts=%v, col=%d, value=%v", ts, col, value)
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
	LogDebug("RRD.PutValues values=%v", values)

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.readonly {
		return fmt.Errorf("RRD file open as read-only")
	}

	cols := make([]int, 0, len(values))
	for _, v := range values {
		cols = append(cols, v.Column)
	}
	if len(cols) == 0 { // no columns defined
		LogDebug("RRD.PutValues load all columns")
		cols = r.allColumnsIDs()
	}

	for aID, a := range r.archives {
		LogDebug("RRD.PutValues updating archive %d", a)

		// all values should have this same TS
		ts := a.calcTS(values[0].TS)

		// get previous values
		LogDebug("RRD.PutValues get prevoius values")
		preValues, err := r.storage.Get(aID, ts, cols)
		if err != nil {
			return err
		}
		// update
		var updatedVal []Value
		if len(preValues) > 0 {
			LogDebug("RRD.PutValues found prevoius values: %v", preValues)
			for i, v := range values {
				pv := preValues[i]
				col := cols[i]
				if pv.Column != col {
					panic(fmt.Errorf("invalid column %d on %d in %v", pv.Column, i, cols))
				}
				if v.Column != col {
					panic(fmt.Errorf("invalid column in val %d != %v", v.Column, col))
				}
				function := r.columns[col].Function
				uv := function.Apply(pv, v)
				updatedVal = append(updatedVal, uv)
			}
		} else {
			for col, v := range values {
				v.Counter = 1
				if r.columns[col].Function == FCount {
					v.Value = 1
				}
				updatedVal = append(updatedVal, v)
			}
		}

		// write updated values
		LogDebug("RRD.PutValues writing values: %v", updatedVal)
		if err = r.storage.Put(aID, ts, updatedVal...); err != nil {
			return err
		}
	}
	return nil
}

// Get get values for timestamp.
func (r *RRD) Get(ts int64, columns ...int) ([]Value, error) {
	LogDebug("RRD.Get ts=%s, columns=%v", ts, columns)
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(columns) == 0 {
		LogDebug("RRD.Get load all columns")
		columns = r.allColumnsIDs()
	}

	for aID := range r.archives {
		LogDebug("RRD.Get checking archive %d", aID)
		values, err := r.getFromArchive(aID, ts, columns)
		if values != nil || err != nil {
			LogDebug("RRD.Get found value in archive %d: %v", aID, values)
			return values, err
		}
	}

	LogDebug("RRD.Get value not found")
	return nil, nil
}

func (r *RRD) getFromArchive(aID int, ts int64, columns []int) ([]Value, error) {
	a := r.archives[aID]
	ts = a.calcTS(ts)
	return r.storage.Get(aID, ts, columns)
}

// Last return last timestamp from db
func (r *RRD) Last() (int64, error) {
	LogDebug("RRD.Last")

	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.last()
}

// Last return last timestamp from db
func (r *RRD) last() (int64, error) {
	var last int64 = -1
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
func (r *RRD) GetRange(minTS, maxTS int64, columns []int, includeInvalid bool, realTime bool) (Rows, error) {
	LogDebug("RRD.GetRange minTS=%d, maxTS=%d, columns=%v", minTS, maxTS, columns)

	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(columns) == 0 {
		LogDebug("RRD.GetRange load all columns")
		columns = r.allColumnsIDs()
	}

	var last int64
	if realTime {
		last = time.Now().Unix()
	} else {
		var err error
		last, err = r.last()
		if err != nil {
			return nil, err
		}
	}

	archiveID, aMinTS, aMaxTS := r.findArchiveForRange(minTS, maxTS, last)
	LogDebug("RRD.GetRange archive: using archive=%d, aMinTS=%d, aMaxTS=%d", archiveID, aMinTS, aMaxTS)

	i, err := r.storage.Iterate(archiveID, aMinTS, aMaxTS, columns)
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

	LogDebug("RRD.GetRange found %d records, sorting...", len(rows))
	sort.Sort(rows)
	if includeInvalid {
		rows = fillData(aMinTS, aMaxTS, r.archives[archiveID].Step, rows, columns)
	}
	return rows, err
}

func (r *RRD) findArchiveForRange(minTS, maxTS, last int64) (archiveID int, aMinTS, aMaxTS int64) {
	LogDebug("RRD.findArchiveForRange minTS=%d, maxTS=%d", minTS, maxTS)

	for aID, a := range r.archives {
		archiveID = aID
		// archive range
		aOldestTS := a.calcTS(last - int64(a.Rows)*a.Step)
		aMaxTS = maxTS // check
		//fmt.Printf("arch=%d, aOldestTS=%d, minTS=%d, last=%d\n", archiveID, aOldestTS, minTS, last)
		if minTS >= aOldestTS {
			aMinTS = a.calcTS(minTS)
			LogDebug("RRD.findArchiveForRange found %d, aMinTS=%d", aID, aMinTS)
			break
		}
	}
	LogDebug("RRD.findArchiveForRange not found exact archive, using last %d, aMinTS=%d", archiveID, aMinTS)
	return
}

func (r *RRD) allColumnsIDs() (cols []int) {
	for i := 0; i < len(r.columns); i++ {
		cols = append(cols, i)
	}
	return
}

// Info return RRDFileInfo structure for current file
func (r *RRD) Info() (*RRDFileInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

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
		Name:  a.Name,
		Rows:  int(a.Rows),
		Step:  a.Step,
		MinTS: -1,
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
		if ts < arch.MinTS || arch.MinTS == -1 {
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
		arch.DataRangeMin = a.calcTS(arch.MaxTS - int64(a.Rows)*a.Step)
	}
	return arch, nil
}

// LowLevelDebugDump return informations useful to debug
func (r *RRD) LowLevelDebugDump() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var res []string
	res = append(res, fmt.Sprintf("Filename: %s", r.filename))
	res = append(res, fmt.Sprintf("ColumnsCount: %d", len(r.columns)))
	res = append(res, fmt.Sprintf("Columns: %#v", r.columns))
	res = append(res, fmt.Sprintf("ArchivesCount: %d", len(r.archives)))
	for aID, a := range r.archives {
		res = append(res, fmt.Sprintf("Archives: %d -  %#v", aID, a))
		iter, _ := r.storage.Iterate(aID, 0, -1, r.allColumnsIDs())
		var maxTS int64
		for {
			err := iter.Next()
			if err == io.EOF {
				break
			}
			row := fmt.Sprintf("  - %d ", iter.TS())

			values, _ := iter.Values()
			for _, value := range values {
				if value.Valid {
					row += value.String() + ", "
				} else {
					row += ", "
				}
			}
			if iter.TS() > maxTS {
				maxTS = iter.TS()
			}
			res = append(res, row)
		}
		res = append(res, fmt.Sprintf("  TS range: %d - %d", a.calcTS(maxTS-int64(a.Rows)*a.Step), maxTS))

	}
	return strings.Join(res, "\n")
}

type (
	RRDDump struct {
		Columns  []RRDColumn
		Archives []RRDArchive
		Data     []RRDArchiveData
	}
	RRDArchiveData struct {
		ArchiveID int
		Rows      Rows
	}
)

func (r *RRD) Dump(filename string) error {
	LogDebug("RRD.Dump filename=%s", filename)
	r.mu.RLock()
	defer r.mu.RUnlock()

	data := RRDDump{
		Columns:  r.columns,
		Archives: r.archives,
	}
	for aID, _ := range r.archives {
		iter, _ := r.storage.Iterate(aID, 0, -1, r.allColumnsIDs())
		ad := RRDArchiveData{ArchiveID: aID}
		for {
			err := iter.Next()
			if err == io.EOF {
				break
			}
			row := Row{TS: iter.TS()}

			values, _ := iter.Values()
			for _, value := range values {
				if value.Valid {
					row.Values = append(row.Values, value)
				}
			}
			ad.Rows = append(ad.Rows, row)
		}
		data.Data = append(data.Data, ad)
	}
	enc, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(filename, enc, 0660)
}

func LoadDumpRRD(input, rrdFilename string) (*RRD, error) {
	LogDebug("LoadDumpRRD input=%s filename=%s", input, rrdFilename)

	var dump RRDDump
	data, err := ioutil.ReadFile(input)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &dump); err != nil {
		return nil, err
	}

	r, err := NewRRD(rrdFilename, dump.Columns, dump.Archives)
	if err != nil {
		return nil, err
	}

	for _, ad := range dump.Data {
		for _, row := range ad.Rows {
			var values []Value
			for _, v := range row.Values {
				v.Valid = true
				values = append(values, v)
			}
			if err = r.storage.Put(ad.ArchiveID, row.TS, values...); err != nil {
				return nil, err
			}
		}
	}
	return r, err
}

func ModifyAddColumns(filename string, columns []RRDColumn) error {
	r, err := OpenRRD(filename, true)
	if err != nil {
		return err
	}
	defer func() {
		if r != nil {
			r.Close()
		}
	}()

	dstCols := r.columns
	for cIdx, c := range columns {
		for _, c2 := range dstCols {
			if c2.Name == c.Name {
				name := fmt.Sprintf("col%d", len(dstCols))
				Log("Using %s name for new column %d (%s)", name, cIdx, c.Name)
				c.Name = name
				break
			}
		}
		dstCols = append(dstCols, c)
	}

	nRRD, err := NewRRD(filename+".new", dstCols, r.archives)
	if err != nil {
		return err
	}
	defer func() {
		if nRRD != nil {
			nRRD.Close()
		}
	}()

	if err := copyData(r, nRRD, nil, nil); err != nil {
		return err
	}

	nRRD.Close()
	nRRD = nil
	r.Close()
	r = nil

	// delete old file
	if err := os.Remove(filename); err != nil {
		return err
	}

	// rename temp file
	return os.Rename(filename+".new", filename)
}

func ModifyDelColumns(filename string, columns []int) error {
	r, err := OpenRRD(filename, true)
	if err != nil {
		return err
	}
	defer func() {
		if r != nil {
			r.Close()
		}
	}()

	var dstCols []RRDColumn
	for cIdx, c := range r.columns {
		if !InList(cIdx, columns) {
			dstCols = append(dstCols, c)
		}
	}

	nRRD, err := NewRRD(filename+".new", dstCols, r.archives)
	if err != nil {
		return err
	}
	defer func() {
		if nRRD != nil {
			nRRD.Close()
		}
	}()

	if err := copyData(r, nRRD, columns, nil); err != nil {
		return err
	}

	nRRD.Close()
	nRRD = nil
	r.Close()
	r = nil

	// delete old file
	if err := os.Remove(filename); err != nil {
		return err
	}

	// rename temp file
	return os.Rename(filename+".new", filename)
}

func ModifyAddArchives(filename string, archs []RRDArchive) error {
	r, err := OpenRRD(filename, true)
	if err != nil {
		return err
	}
	defer func() {
		if r != nil {
			r.Close()
		}
	}()

	// TODO check names uniques

	nRRD, err := NewRRD(filename+".new", r.columns, append(r.archives, archs...))
	if err != nil {
		return err
	}
	defer func() {
		if nRRD != nil {
			nRRD.Close()
		}
	}()

	if err := copyData(r, nRRD, nil, nil); err != nil {
		return err
	}

	nRRD.Close()
	nRRD = nil
	r.Close()
	r = nil

	LogDebug("delete old file")
	if err := os.Remove(filename); err != nil {
		return err
	}

	LogDebug("rename temp file")
	return os.Rename(filename+".new", filename)
}

func ModifyDelArchives(filename string, archs []int) error {
	r, err := OpenRRD(filename, true)
	if err != nil {
		return err
	}
	defer func() {
		if r != nil {
			r.Close()
		}
	}()

	var dstArchs []RRDArchive
	for aIdx, a := range r.archives {
		if !InList(aIdx, archs) {
			dstArchs = append(dstArchs, a)
		}
	}

	nRRD, err := NewRRD(filename+".new", r.columns, dstArchs)
	if err != nil {
		return err
	}
	defer func() {
		if nRRD != nil {
			nRRD.Close()
		}
	}()

	if err := copyData(r, nRRD, nil, archs); err != nil {
		return err
	}

	nRRD.Close()
	nRRD = nil
	r.Close()
	r = nil

	LogDebug("delete old file")
	if err := os.Remove(filename); err != nil {
		return err
	}

	LogDebug("rename temp file")
	return os.Rename(filename+".new", filename)
}

func (a *RRDArchive) calcTS(ts int64) (ats int64) {
	if ts < 1 {
		return ts
	}
	return int64(ts/a.Step) * a.Step
}

func (r Rows) Len() int           { return len(r) }
func (r Rows) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r Rows) Less(i, j int) bool { return r[i].TS < r[j].TS }

func fillData(minTS, maxTS, step int64, values Rows, columns []int) Rows {
	LogDebug("fillData minTS=%d, maxTS=%d, step=%d, valuescnd=%d", minTS, maxTS,
		step, len(values))

	var result Rows
	emptyRowValues := make([]Value, 0, len(columns))
	for _, c := range columns {
		emptyRowValues = append(emptyRowValues, Value{
			Valid:  false,
			Column: c,
		})
	}

	if len(values) == 0 {
		for ts := minTS; ts <= maxTS; ts = ts + step {
			result = append(result, Row{TS: ts, Values: emptyRowValues})
		}
		return result
	}

	ts := minTS
	vidx := 0
	for ts <= maxTS {
		if vidx < len(values) && values[vidx].TS == ts {
			result = append(result, values[vidx])
			vidx++
		} else {
			result = append(result, Row{TS: ts, Values: emptyRowValues})
		}
		ts += step
	}
	return result
}

func copyData(src, dst *RRD, skipColumns []int, skipArchives []int) error {
	LogDebug("copy data")
	var cols []int
	var colsMap map[int]int
	if len(skipColumns) > 0 {
		colsMap = make(map[int]int)
		for c := 0; c < len(src.columns); c++ {
			if !InList(c, skipColumns) {
				colsMap[c] = len(cols)
				cols = append(cols, c)
			}
		}
	} else {
		cols = src.allColumnsIDs()
	}

	dstAID := 0
	for aID, _ := range src.archives {
		if InList(aID, skipArchives) {
			continue
		}

		iter, _ := src.storage.Iterate(aID, 0, -1, cols)
		for {
			err := iter.Next()
			if err == io.EOF {
				break
			}
			values, _ := iter.Values()
			if colsMap != nil {
				var vOut []Value
				for _, v := range values {
					v.Column = colsMap[v.Column]
					vOut = append(vOut, v)
				}
				values = vOut
			}
			if err = dst.storage.Put(dstAID, iter.TS(), values...); err != nil {
				return err
			}
		}
		dstAID++
	}
	return nil
}
