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
Row format:
[
	timestamp: int64,
	cols[
		valid: int8
		value: float64
	][*]
]

*/

type Value struct {
	TS    int64
	Valid bool
	Value float64
}

func (v *Value) String() string {
	return fmt.Sprintf("Value[td=%d, valid=%v, value=%v]",
		v.TS, v.Valid, v.Value)
}

func (v *Value) size() int {
	return binary.Size(v.Value) + binary.Size(int8(0))
}

func (v *Value) Read(r io.Reader) (err error) {
	var valid int8
	if err = binary.Read(r, binary.LittleEndian, &valid); err != nil {
		return
	}
	v.Valid = valid == 1
	if err = binary.Read(r, binary.LittleEndian, &v.Value); err != nil {
		return
	}
	return
}

func (v *Value) Write(r io.Writer) (err error) {
	valid := int8(0)
	if v.Valid {
		valid = 1
	}
	if err = binary.Write(r, binary.LittleEndian, valid); err != nil {
		return
	}
	if err = binary.Write(r, binary.LittleEndian, v.Value); err != nil {
		return
	}
	return
}

type rddHeader struct {
	Rows int32
	Cols int32
	Step int64
}

func (h *rddHeader) String() string {
	return fmt.Sprintf("rddHeader[Rows: %d, Cols: %d, Step: %d]", h.Rows, h.Cols, h.Step)
}

func (h *rddHeader) size() int {
	return binary.Size(h.Rows) + binary.Size(h.Cols) + binary.Size(h.Step)
}

func (h *rddHeader) read(r io.Reader) (err error) {
	err = binary.Read(r, binary.LittleEndian, &h.Rows)
	if err != nil {
		return
	}
	err = binary.Read(r, binary.LittleEndian, &h.Cols)
	if err != nil {
		return
	}
	err = binary.Read(r, binary.LittleEndian, &h.Step)
	if err != nil {
		return
	}
	return nil
}

func (h *rddHeader) write(w io.Writer) (err error) {
	err = binary.Write(w, binary.LittleEndian, h.Rows)
	if err != nil {
		return
	}
	err = binary.Write(w, binary.LittleEndian, h.Cols)
	if err != nil {
		return
	}
	err = binary.Write(w, binary.LittleEndian, h.Step)
	if err != nil {
		return
	}
	return nil
}

type RRDFile struct {
	filename   string
	f          *os.File
	header     rddHeader
	headerSize int
	rowSize    int
	readonly   bool
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
	r.rowSize = rowSize(r.header.Cols)
	r.headerSize = r.header.size()

	return r, err
}

// NewRRDFile create new, empty RRD file
func NewRRDFile(filename string, cols, rows int32, step int64) (*RRDFile, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	r := &RRDFile{
		f:        f,
		filename: filename,
		rowSize:  rowSize(cols),
	}

	r.headerSize = r.header.size()
	r.header.Cols = cols
	r.header.Rows = rows
	r.header.Step = step

	f.Seek(0, 0)
	if err := r.header.write(f); err != nil {
		fmt.Println("NewRRDFile write error: " + err.Error())
	}

	dstSize := rows * int32(rowSize(cols))
	buff := make([]byte, dstSize, dstSize)
	f.Write(buff)

	fmt.Println(r.header.String())

	return r, nil
}

func (r *RRDFile) String() string {
	return fmt.Sprintf("RRDFile[filename=%s, f=%v, header=%#v, headerSize=%d, rowSize=%d]",
		r.filename, r.f, r.header, r.headerSize, r.rowSize)
}

// Close RRD file
func (r *RRDFile) Close() error {
	return r.f.Close()
}

// Put value into database
func (r *RRDFile) Put(ts int64, col int32, value float64) error {
	v := &Value{
		TS:    ts,
		Valid: true,
		Value: value,
	}
	return r.PutValue(v, col)
}

func (r *RRDFile) PutValue(v *Value, col int32) error {
	if r.readonly {
		return errors.New("RRD file open as read-only")
	}
	ts := r.calcTS(v.TS)
	tsOffset, valueOffset := r.calcOffset(ts, col)
	// write real ts
	if _, err := r.f.Seek(tsOffset, 0); err != nil {
		return err
	}
	// invalide record when ts changed
	var storeTS int64
	if err := binary.Read(r.f, binary.LittleEndian, &storeTS); err != nil {
		return err
	}
	if storeTS != ts {
		v := Value{}
		for c := int32(0); c < r.header.Cols; c++ {
			v.Write(r.f)
		}
	}

	if _, err := r.f.Seek(tsOffset, 0); err != nil {
		return err
	}
	if err := binary.Write(r.f, binary.LittleEndian, ts); err != nil {
		return err
	}
	if _, err := r.f.Seek(valueOffset, 0); err != nil {
		return err
	}
	return v.Write(r.f)
}

// Get value from database
func (r *RRDFile) Get(ts int64, col int32) (*Value, error) {
	tsOffset, valueOffset := r.calcOffset(ts, col)
	v := &Value{}

	// Read real ts
	if _, err := r.f.Seek(tsOffset, 0); err != nil {
		return nil, err
	}

	if err := binary.Read(r.f, binary.LittleEndian, &v.TS); err != nil {
		return nil, err
	}

	// Read value
	if _, err := r.f.Seek(valueOffset, 0); err != nil {
		return nil, err
	}
	err := v.Read(r.f)
	return v, err
}

type Row struct {
	TS   int64
	Cols []Value
}

type Rows []Row

func (r Rows) Len() int           { return len(r) }
func (r Rows) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r Rows) Less(i, j int) bool { return r[i].TS < r[j].TS }

// GetRange finds all records in given range
func (r *RRDFile) GetRange(minTS, maxTS int64) (Rows, error) {
	res := make(Rows, 0, r.header.Rows)

	minTS = r.calcTS(minTS)

	if _, err := r.f.Seek(int64(r.headerSize), 0); err != nil {
		return nil, err
	}
	offset := int64(r.headerSize)
	for y := int32(0); y < r.header.Rows; y++ {
		if _, err := r.f.Seek(offset, 0); err != nil {
			return nil, err
		}
		var ts int64
		if err := binary.Read(r.f, binary.LittleEndian, &ts); err != nil {
			return nil, err
		}
		if ts >= minTS && (ts <= maxTS || maxTS < 0) {
			row := Row{TS: ts, Cols: make([]Value, 0, r.header.Cols)}
			for c := int32(0); c < r.header.Cols; c++ {
				v := Value{}
				v.Read(r.f)
				row.Cols = append(row.Cols, v)
			}
			res = append(res, row)
		}
		offset += int64(r.rowSize)
	}

	sort.Sort(res)
	return res, nil
}

func (r *RRDFile) calcOffset(ts int64, col int32) (tsOffset, valueOffset int64) {
	tsOffset = int64(r.headerSize) + int64(r.rowSize)*((ts/r.header.Step)%int64(r.header.Rows))
	valueOffset = tsOffset + int64(binary.Size(ts)) // ts
	valueOffset += int64(col) * int64(binary.Size(float64(0)))
	return
}

func (r *RRDFile) calcTS(ts int64) int64 {
	return int64(ts/r.header.Step) * r.header.Step
}

func rowSize(cols int32) int {
	v := Value{}
	return binary.Size(int64(0)) + int(cols)*v.size()
}
