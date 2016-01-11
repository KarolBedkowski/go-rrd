package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	//	"sort"
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
		value: float32
	][number of cols]
][number of rows]

*/

const (
	fileVersion = int32(1)
	fileMagic   = int64(1038472294759683202)
)

// Value stricture holds single value in rrd
type Value struct {
	TS      int64 // not stored
	Valid   bool  // int32
	Value   float32
	Counter int64
}

var valueSize = 4 + 8 + 8

func (v *Value) String() string {
	return fmt.Sprintf("Value[td=%d, valid=%v, value=%v, counter=%d]",
		v.TS, v.Valid, v.Value, v.Counter)
}

func (v *Value) Write(r io.Writer) (err error) {
	if err = binary.Write(r, binary.BigEndian, v.Value); err != nil {
		return
	}
	if err = binary.Write(r, binary.BigEndian, v.Counter); err != nil {
		return
	}
	valid := int32(0)
	if v.Valid {
		valid = 1
	}
	if err = binary.Write(r, binary.BigEndian, valid); err != nil {
		return
	}
	return
}

func loadValue(r io.Reader) (v Value, err error) {
	v = Value{}
	if err = binary.Read(r, binary.BigEndian, &v.Value); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &v.Counter); err != nil {
		return
	}
	var valid int32
	if err = binary.Read(r, binary.BigEndian, &valid); err != nil {
		return
	}
	v.Valid = valid == 1
	return
}

type (
	// rrdHeader is file header
	rrdHeader struct {
		Version       int32
		ColumnsCount  int16
		ArchivesCount int16
		Magic         int64
	}

	// RRDColumn define one column
	RRDColumn struct {
		Name     string // byte[16]
		Function Function
	}

	// RRDArchive defines one archive
	RRDArchive struct {
		Name string // byte[16]
		Step int64
		Rows int32

		archiveOffset int64
		archiveSize   int64
	}
)

var (
	rrdHeaderSize  = 4 + 2 + 2 + 8
	rrdColumnSize  = 16 + 4
	rrdArchiveSize = 16 + 8 + 4 + 8 + 8
)

func (h *rrdHeader) String() string {
	return fmt.Sprintf("rrdHeader[%#v]", h)
}

// RRDFile is round-robin database file
type RRDFile struct {
	filename string
	header   rrdHeader
	readonly bool

	columns  []RRDColumn
	archives []RRDArchive

	f *os.File

	rowSize int
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

	if _, err = f.Seek(0, 0); err != nil {
		return nil, err
	}
	r.header, err = loadHeader(r.f)
	if err != nil {
		return nil, err
	}

	r.columns, err = loadColumnsDef(f, int(r.header.ColumnsCount))
	if err != nil {
		return nil, err
	}
	r.archives, err = loadArchiveDef(f, int(r.header.ArchivesCount))
	if err != nil {
		return nil, err
	}
	r.rowSize = valueSize*len(r.columns) + 8 // ts

	return r, err
}

// NewRRDFile create new, empty RRD file
func NewRRDFile(filename string, cols []RRDColumn, archives []RRDArchive) (*RRDFile, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	r := &RRDFile{
		f:        f,
		filename: filename,
		header: rrdHeader{
			Version:       fileVersion,
			ColumnsCount:  int16(len(cols)),
			ArchivesCount: int16(len(archives)),
			Magic:         fileMagic,
		},
		columns:  cols,
		archives: archives,
	}

	allHeadersLen := rrdHeaderSize + rrdColumnSize*len(cols) +
		rrdArchiveSize*len(archives)

	r.rowSize = valueSize*len(r.columns) + 8 // ts
	archivesSize := r.calcArchiveOffsetSize(allHeadersLen)

	if _, err = f.Seek(0, 0); err != nil {
		return nil, err
	}
	if err = writeHeader(f, r.header); err != nil {
		return nil, err
	}
	if err = writeColumnsDef(f, cols); err != nil {
		return nil, err
	}
	if err = writeArchivesDef(f, archives); err != nil {
		return nil, err
	}

	buf := make([]byte, archivesSize, archivesSize)
	if err = binary.Write(f, binary.BigEndian, buf); err != nil {
		return nil, err
	}

	fmt.Printf("%+v", r)
	return r, nil
}

func (r *RRDFile) String() string {
	return fmt.Sprintf("RRDFile[%#v]", r)
}

// Close RRD file
func (r *RRDFile) Close() error {
	return r.f.Close()
}

// Put value into database
func (r *RRDFile) Put(ts int64, col int, value float32) error {
	v := Value{
		TS:    ts,
		Valid: true,
		Value: value,
	}
	return r.PutValue(v, col)
}

func (r *RRDFile) PutValue(v Value, col int) error {
	if r.readonly {
		return fmt.Errorf("RRD file open as read-only")
	}

	function := r.columns[col].Function
	for _, a := range r.archives {
		aTS := a.calcTS(v.TS)
		rowOffset, valOffset := a.calcOffset(aTS, col, r.rowSize)
		if err := r.writeValue(aTS, v, rowOffset, valOffset, function); err != nil {
			return err
		}

	}
	return nil
}

// PutValue insert value into database
func (r *RRDFile) writeValue(ts int64, v Value, rowOffset, valOffset int64, function Function) error {
	// write real ts
	if _, err := r.f.Seek(rowOffset, 0); err != nil {
		return err
	}

	// invalide record when ts changed
	if err := r.checkAndCleanRow(ts, rowOffset); err != nil {
		return err
	}

	if _, err := r.f.Seek(valOffset, 0); err != nil {
		return err
	}

	prev, _ := loadValue(r.f)

	fmt.Printf("Prev: %s\n", prev.String())

	v = function.Apply(prev, v)

	fmt.Printf("New: %s\n", v.String())

	if _, err := r.f.Seek(valOffset, 0); err != nil {
		return err
	}
	return v.Write(r.f)
}

// PutRow insert many values into single row
func (r *RRDFile) PutRow(ts int64, values []float32) error {
	if r.readonly {
		return errors.New("RRD file open as read-only")
	}
	/*
		if ts <= 0 {
			return errors.New("Missing TS in row")
		}

		if len(values) > len(r.columns) {
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
			//v = r.header.Function.Apply(prevs[idx], v)
			if err := v.Write(r.f); err != nil {
				return err
			}
		}
	*/
	return nil
}

// Get value from database
func (r *RRDFile) Get(ts int64, col int) (*Value, error) {
	for _, a := range r.archives {
		aTS := a.calcTS(ts)
		rowOffset, valOffset := a.calcOffset(aTS, col, r.rowSize)

		// Read real ts
		if _, err := r.f.Seek(rowOffset, 0); err != nil {
			return nil, err
		}

		var rowTS int64
		if err := binary.Read(r.f, binary.BigEndian, &rowTS); err != nil {
			return nil, err
		}

		if rowTS != aTS {
			// value not found in this archive, search in next
			continue
		}

		// Read value
		if _, err := r.f.Seek(valOffset, 0); err != nil {
			return nil, err
		}
		v, err := loadValue(r.f)
		v.TS = rowTS
		return &v, err
	}
	return nil, nil
}

func (r *RRDFile) checkAndCleanRow(ts int64, tsOffset int64) error {
	var storeTS int64
	if err := binary.Read(r.f, binary.BigEndian, &storeTS); err != nil {
		return err
	}
	if storeTS == ts {
		return nil
	}
	//fmt.Printf("storeTS != ts - reseting storeTS=%v\n", storeTS)
	v := Value{}
	for c := 0; c < int(r.header.ColumnsCount); c++ {
		v.Write(r.f)
	}
	if _, err := r.f.Seek(tsOffset, 0); err != nil {
		return err
	}
	if err := binary.Write(r.f, binary.BigEndian, ts); err != nil {
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

/*
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
		if err := binary.Read(r.f, binary.BigEndian, &ts); err != nil {
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
		if err := binary.Read(r.f, binary.BigEndian, &ts); err != nil {
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
*/
func (r *RRDFile) calcArchiveOffsetSize(baseOffset int) (allSize int64) {
	offset := int64(baseOffset)
	for _, a := range r.archives {
		a.archiveSize = int64(int(a.Rows) * r.rowSize)
		a.archiveOffset = int64(offset)
		offset += int64(a.archiveSize)
		allSize += a.archiveSize
	}
	return
}

func loadHeader(r io.Reader) (header rrdHeader, err error) {
	header = rrdHeader{}
	if err = binary.Read(r, binary.BigEndian, &header.Version); err != nil {
		return
	}
	if header.Version != fileVersion {
		err = fmt.Errorf("Invalid file (version)")
		return
	}

	if err = binary.Read(r, binary.BigEndian, &header.ColumnsCount); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &header.ArchivesCount); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &header.Magic); err != nil {
		return
	}
	if header.Magic != fileMagic {
		err = fmt.Errorf("Invalid file (magic)")
	}
	return

}

func writeHeader(w io.Writer, header rrdHeader) (err error) {
	if err = binary.Write(w, binary.BigEndian, header.Version); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, header.ColumnsCount); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, header.ArchivesCount); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, header.Magic); err != nil {
		return
	}
	return nil
}

func loadColumnsDef(r io.Reader, colCount int) (cols []RRDColumn, err error) {
	for i := 0; i < colCount; i++ {
		buf := make([]byte, 16, 16)
		if err = binary.Read(r, binary.BigEndian, &buf); err != nil {
			return
		}
		var funcID int32
		if err = binary.Read(r, binary.BigEndian, &funcID); err != nil {
			return
		}
		col := RRDColumn{
			Name:     string(buf),
			Function: Function(funcID),
		}
		cols = append(cols, col)
	}
	return
}

func writeColumnsDef(w io.Writer, cols []RRDColumn) (err error) {
	for _, col := range cols {
		name := make([]byte, 16, 16)
		copy(name, []byte((col.Name + "                ")[:16]))
		if err = binary.Write(w, binary.BigEndian, name); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, int32(col.Function)); err != nil {
			return
		}
	}
	return
}

func loadArchiveDef(r io.Reader, archCount int) (archives []RRDArchive, err error) {
	for i := 0; i < archCount; i++ {
		buf := make([]byte, 16, 16)
		if err = binary.Read(r, binary.BigEndian, &buf); err != nil {
			return
		}
		a := RRDArchive{
			Name: string(buf),
		}
		if err = binary.Read(r, binary.BigEndian, &a.Step); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &a.Rows); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &a.archiveSize); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &a.archiveOffset); err != nil {
			return
		}
		archives = append(archives, a)
	}
	return
}

func writeArchivesDef(w io.Writer, archives []RRDArchive) (err error) {
	for _, a := range archives {
		name := make([]byte, 16, 16)
		copy(name, []byte((a.Name + "                ")[:16]))
		if err = binary.Write(w, binary.BigEndian, name); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, a.Step); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, a.Rows); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, a.archiveSize); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, a.archiveOffset); err != nil {
			return
		}
	}
	return
}

func (a *RRDArchive) calcOffset(ts int64, col int, rowSize int) (rowOffset, valOffset int64) {
	rowNum := (ts / a.Step) % int64(a.Rows)
	rowOffset = int64(a.archiveOffset) + int64(rowSize)*rowNum
	valOffset = rowOffset + 8 // TS
	valOffset += int64(col) * int64(valueSize)
	return
}

func (a *RRDArchive) calcTS(ts int64) int64 {
	return int64(ts/a.Step) * a.Step
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
	/*
		res := &RRDFileInfo{
			Filename: r.filename,
			//		Rows:     r.header.Rows,
			//		Cols:     r.header.Cols,
			//		Step:     r.header.Step,
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
			if err := binary.Read(r.f, binary.BigEndian, &ts); err != nil {
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
	*/
	return nil, nil
}
