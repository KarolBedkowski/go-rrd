package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

/*
File format:
header {
	rows: int32
	cols: int32
	step: int64
	function: int8
}
rows[
	timestamp: int64, -> 0=invalid
	cols[
		valid: int8 -> 1=valid data
		value: float64
	][number of cols]
][number of rows]

*/

const (
	fileVersion = 1
)

// Value stricture holds single value in rrd
type Value struct {
	TS      int64
	Valid   bool
	Value   float64
	Counter int64
}

var valueSize = binary.Size(float64(0)) + binary.Size(int64(0)) + binary.Size(int8(0))

func (v *Value) String() string {
	return fmt.Sprintf("Value[td=%d, valid=%v, value=%v, counter=%d]",
		v.TS, v.Valid, v.Value, v.Counter)
}

func (v *Value) Write(r io.Writer) (err error) {
	if err = binary.Write(r, binary.LittleEndian, v.Value); err != nil {
		return
	}
	if err = binary.Write(r, binary.LittleEndian, v.Counter); err != nil {
		return
	}
	valid := int8(0)
	if v.Valid {
		valid = 1
	}
	if err = binary.Write(r, binary.LittleEndian, valid); err != nil {
		return
	}
	return
}

func loadValue(r io.Reader) (v Value, err error) {
	v = Value{}
	if err = binary.Read(r, binary.LittleEndian, &v.Value); err != nil {
		return
	}
	if err = binary.Read(r, binary.LittleEndian, &v.Counter); err != nil {
		return
	}
	var valid int8
	if err = binary.Read(r, binary.LittleEndian, &valid); err != nil {
		return
	}
	v.Valid = valid == 1
	return
}

type rddHeader struct {
	Version  int64
	Rows     int32
	Cols     int32
	Step     int64
	Function Function
}

var rddHeaderSize = binary.Size(int64(0)) + binary.Size(int32(0)) + binary.Size(int32(0)) +
	binary.Size(int64(0)) + binary.Size(int8(0))

func (h *rddHeader) String() string {
	return fmt.Sprintf("rddHeader[Rows: %d, Cols: %d, Step: %d]", h.Rows, h.Cols, h.Step)
}

func (h *rddHeader) read(r io.Reader) (err error) {
	if err = binary.Read(r, binary.LittleEndian, &h.Version); err != nil {
		return
	}
	if err = binary.Read(r, binary.LittleEndian, &h.Rows); err != nil {
		return
	}
	if err = binary.Read(r, binary.LittleEndian, &h.Cols); err != nil {
		return
	}
	if err = binary.Read(r, binary.LittleEndian, &h.Step); err != nil {
		return
	}
	var f int8
	if err = binary.Read(r, binary.LittleEndian, &f); err != nil {
		return
	}
	h.Function = Function(f)
	return nil
}

func (h *rddHeader) write(w io.Writer) (err error) {
	if err = binary.Write(w, binary.LittleEndian, h.Version); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, h.Rows); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, h.Cols); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, h.Step); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, int8(h.Function)); err != nil {
		return
	}
	return nil
}

// RRDFile is round-robin database file
type RRDFile struct {
	filename string
	f        *os.File
	header   rddHeader
	rowSize  int
	readonly bool
}

// OpenRRD open existing rrd file
func OpenRRD(filename string, readonly bool) (*RRDFile, error) {
	flag := os.O_RDWR
	if readonly {
		flag = os.O_RDONLY
	}
	f, err := os.OpenFile(filename, flag, 0660)
	if err != nil {
		return nil, err
	}

	r := &RRDFile{
		f:        f,
		filename: filename,
		readonly: readonly,
	}

	f.Seek(0, 0)
	err = r.header.read(f)
	r.rowSize = calcRowSize(r.header.Cols)

	return r, err
}

// NewRRDFile create new, empty RRD file
func NewRRDFile(filename string, cols, rows int32, step int64, function Function) (*RRDFile, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	r := &RRDFile{
		f:        f,
		filename: filename,
		rowSize:  calcRowSize(cols),
	}

	r.header.Version = fileVersion
	r.header.Cols = cols
	r.header.Rows = rows
	r.header.Step = step
	r.header.Function = Function(function)

	f.Seek(0, 0)
	if err := r.header.write(f); err != nil {
		fmt.Println("NewRRDFile write error: " + err.Error())
	}

	dstSize := rows * int32(calcRowSize(cols))
	buff := make([]byte, dstSize, dstSize)
	f.Write(buff)

	fmt.Println(r.header.String())

	return r, nil
}

func (r *RRDFile) String() string {
	return fmt.Sprintf("RRDFile[filename=%s, f=%v, header=%#v, headerSize=%d, rowSize=%d]",
		r.filename, r.f, r.header, rddHeaderSize, r.rowSize)
}

// Close RRD file
func (r *RRDFile) Close() error {
	return r.f.Close()
}

// Put value into database
func (r *RRDFile) Put(ts int64, col int32, value float64) error {
	v := Value{
		TS:    ts,
		Valid: true,
		Value: value,
	}
	return r.PutValue(v, col)
}

// PutValue insert value into database
func (r *RRDFile) PutValue(v Value, col int32) error {
	if r.readonly {
		return errors.New("RRD file open as read-only")
	}
	ts := r.calcTS(v.TS)
	tsOffset, valueOffset := r.calcOffset(ts, col)
	// write real ts
	if _, err := r.f.Seek(tsOffset, 0); err != nil {
		return err
	}

	fmt.Printf("ts=%v, tsOffset=%v, valueOffset=%v\n", ts, tsOffset, valueOffset)
	// invalide record when ts changed
	if err := r.checkAndCleanRow(ts, tsOffset); err != nil {
		return err
	}

	if _, err := r.f.Seek(valueOffset, 0); err != nil {
		return err
	}

	prev, _ := loadValue(r.f)

	fmt.Printf("Prev: %s\n", prev.String())

	v = r.header.Function.Apply(prev, v)

	fmt.Printf("New: %s\n", v.String())

	if _, err := r.f.Seek(valueOffset, 0); err != nil {
		return err
	}
	return v.Write(r.f)
}

// PutRow insert many values into single row
func (r *RRDFile) PutRow(ts int64, values []float64) error {
	if r.readonly {
		return errors.New("RRD file open as read-only")
	}

	if ts <= 0 {
		return errors.New("Missing TS in row")
	}

	if int32(len(values)) > r.header.Cols {
		return errors.New("To many columns")
	}

	ts = r.calcTS(ts)
	tsOffset, valueOffset := r.calcOffset(ts, 0)
	// write real ts
	if _, err := r.f.Seek(tsOffset, 0); err != nil {
		return err
	}

	fmt.Printf("ts=%v, tsOffset=%v, valueOffset=%v\n", ts, tsOffset, valueOffset)
	// invalide record when ts changed
	if err := r.checkAndCleanRow(ts, tsOffset); err != nil {
		return err
	}

	// load old values
	if _, err := r.f.Seek(valueOffset, 0); err != nil {
		return err
	}
	var prevs []Value
	for i := 0; i < len(values); i++ {
		v, _ := loadValue(r.f)
		prevs = append(prevs, v)
	}

	if _, err := r.f.Seek(valueOffset, 0); err != nil {
		return err
	}
	for idx, val := range values {
		v := Value{
			Valid: true,
			Value: val,
		}
		v = r.header.Function.Apply(prevs[idx], v)
		if err := v.Write(r.f); err != nil {
			return err
		}
	}

	return nil
}

// Get value from database
func (r *RRDFile) Get(ts int64, col int32) (*Value, error) {
	tsOffset, valueOffset := r.calcOffset(ts, col)
	// Read real ts
	if _, err := r.f.Seek(tsOffset, 0); err != nil {
		return nil, err
	}

	var rowTS int64
	if err := binary.Read(r.f, binary.LittleEndian, &rowTS); err != nil {
		return nil, err
	}

	// Read value
	if _, err := r.f.Seek(valueOffset, 0); err != nil {
		return nil, err
	}
	v, err := loadValue(r.f)
	v.TS = rowTS
	return &v, err
}

func (r *RRDFile) checkAndCleanRow(ts int64, tsOffset int64) error {
	var storeTS int64
	if err := binary.Read(r.f, binary.LittleEndian, &storeTS); err != nil {
		return err
	}
	if storeTS == ts {
		return nil
	}
	//fmt.Printf("storeTS != ts - reseting storeTS=%v\n", storeTS)
	v := Value{}
	for c := int32(0); c < r.header.Cols; c++ {
		v.Write(r.f)
	}
	if _, err := r.f.Seek(tsOffset, 0); err != nil {
		return err
	}
	if err := binary.Write(r.f, binary.LittleEndian, ts); err != nil {
		return err
	}
	return nil
}

// Row keep values for all columns
type Row struct {
	TS   int64
	Cols []Value
}

// Rows is list of rows
type Rows []Row

func (r Rows) Len() int           { return len(r) }
func (r Rows) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r Rows) Less(i, j int) bool { return r[i].TS < r[j].TS }

// GetRange finds all records in given range
func (r *RRDFile) GetRange(minTS, maxTS int64) (Rows, error) {
	res := make(Rows, 0, r.header.Rows)
	fmt.Printf("GetRange(%d, %d) %d\n", minTS, maxTS, len(res))

	minTS = r.calcTS(minTS)

	if _, err := r.f.Seek(int64(rddHeaderSize), 0); err != nil {
		return nil, err
	}
	offset := int64(rddHeaderSize)
	for y := int32(0); y < r.header.Rows; y++ {
		if _, err := r.f.Seek(offset, 0); err != nil {
			return nil, err
		}
		var ts int64
		if err := binary.Read(r.f, binary.LittleEndian, &ts); err != nil {
			return nil, err
		}
		//fmt.Printf("ts=%d, minTS=%d,maxTS=%d\n", ts, minTS, maxTS)
		if ts > 0 && ts >= minTS && (ts <= maxTS || maxTS < 0) {
			row := Row{TS: ts, Cols: make([]Value, 0, r.header.Cols)}
			for c := int32(0); c < r.header.Cols; c++ {
				v, _ := loadValue(r.f)
				row.Cols = append(row.Cols, v)
			}
			res = append(res, row)
		}
		offset += int64(r.rowSize)
	}

	sort.Sort(res)
	return res, nil
}

func (r *RRDFile) Last() int64 {
	if _, err := r.f.Seek(int64(rddHeaderSize), 0); err != nil {
		return 0
	}
	var last int64
	offset := int64(rddHeaderSize)
	for y := int32(0); y < r.header.Rows; y++ {
		if _, err := r.f.Seek(offset, 0); err != nil {
			return last
		}
		var ts int64
		if err := binary.Read(r.f, binary.LittleEndian, &ts); err != nil {
			return last
		}
		//fmt.Printf("ts=%d, minTS=%d,maxTS=%d\n", ts, minTS, maxTS)
		if ts > 0 {
			if ts > last {
				last = ts
			} else {
				break
			}
		}
		offset += int64(r.rowSize)
	}
	return last
}

func (r *RRDFile) calcOffset(ts int64, col int32) (tsOffset, valueOffset int64) {
	tsOffset = int64(rddHeaderSize) + int64(r.rowSize)*((ts/r.header.Step)%int64(r.header.Rows))
	valueOffset = tsOffset + int64(binary.Size(ts)) // ts
	valueOffset += int64(col) * int64(binary.Size(float64(0)))
	return
}

func (r *RRDFile) calcTS(ts int64) int64 {
	return int64(ts/r.header.Step) * r.header.Step
}

func calcRowSize(cols int32) int {
	return binary.Size(int64(0)) + int(cols)*valueSize
}

// RRDFileInfo holds informations about rrd file
type RRDFileInfo struct {
	Filename string
	Rows     int32
	Cols     int32
	Step     int64
	UsedRows int32
	MinTS    int64
	MaxTS    int64
	Values   int64
	Function Function
}

func (r *RRDFile) Info() (*RRDFileInfo, error) {
	res := &RRDFileInfo{
		Filename: r.filename,
		Rows:     r.header.Rows,
		Cols:     r.header.Cols,
		Step:     r.header.Step,
		Function: r.header.Function,
	}

	// Count rows & Values
	offset := int64(rddHeaderSize)
	if _, err := r.f.Seek(offset, 0); err != nil {
		return nil, err
	}
	for y := int32(0); y < r.header.Rows; y++ {
		offset += int64(r.rowSize)
		var ts int64
		if err := binary.Read(r.f, binary.LittleEndian, &ts); err != nil {
			return nil, err
		}
		if ts > 0 {
			res.UsedRows++
			if ts < res.MinTS || res.MinTS == 0 {
				res.MinTS = ts
			}
			if ts > res.MaxTS {
				res.MaxTS = ts
			}
			for x := int32(0); x < r.header.Cols; x++ {
				value, _ := loadValue(r.f)
				if value.Valid {
					res.Values++
				}
			}
		} else {
			if _, err := r.f.Seek(offset, 0); err != nil {
				return nil, err
			}
		}
	}
	return res, nil
}
