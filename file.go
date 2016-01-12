package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

/*
File format:
header
columns definitions[columns count]
archives definitions[archives count]
archives[archives count]
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

var valueSize = 4 + binary.Size(float32(0)) + 8

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
	valid := int32(0)
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
	var valid int32
	if err = binary.Read(r, binary.LittleEndian, &valid); err != nil {
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
		Name     string   // byte[16]
		Function Function // int32
	}

	// RRDArchive defines one archive
	RRDArchive struct {
		Name string // byte[16]
		Step int64
		Rows int32

		archiveOffset int64
		archiveSize   int64
	}

	archIterator struct {
		file       *RRDFile
		archive    *RRDArchive
		currentRow int

		TS int64
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
		columns: cols,
	}

	allHeadersLen := rrdHeaderSize + rrdColumnSize*len(cols) +
		rrdArchiveSize*len(archives)

	r.rowSize = valueSize*len(r.columns) + 8 // ts
	r.archives, _ = calcArchiveOffsetSize(archives, r.rowSize, allHeadersLen)

	if err = writeHeader(f, r.header); err != nil {
		return nil, err
	}
	if err = writeColumnsDef(f, r.columns); err != nil {
		return nil, err
	}
	if err = writeArchivesDef(f, r.archives); err != nil {
		return nil, err
	}

	for _, a := range r.archives {
		for i := 0; i < int(a.Rows); i++ {
			if err := r.writeEmptyRow(-1); err != nil {
				return nil, err
			}
		}
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
		fmt.Printf("PutValue info archive %s: %d, %d\n", a.Name, rowOffset, valOffset)
		if err := r.writeValue(aTS, v, rowOffset, valOffset, function); err != nil {
			return err
		}
	}
	return nil
}

// PutValue insert value into database
func (r *RRDFile) writeValue(ts int64, v Value, rowOffset, valOffset int64, function Function) error {
	// write real ts
	// invalidate record when ts changed
	if err := r.checkAndCleanRow(ts, rowOffset); err != nil {
		return err
	}

	if _, err := r.f.Seek(valOffset, 0); err != nil {
		return err
	}
	prev, _ := loadValue(r.f)
	v = function.Apply(prev, v)
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
	if ts < 0 {
		return errors.New("Missing TS in row")
	}

	if len(values) > len(r.columns) {
		return errors.New("To many columns")
	}

	for _, a := range r.archives {
		aTS := a.calcTS(ts)
		rowOffset, _ := a.calcOffset(aTS, 0, r.rowSize)
		fmt.Printf("PutRow info archive %s: %d\n", a.Name, rowOffset)
		if err := r.writeRow(aTS, values, rowOffset); err != nil {
			return err
		}
	}
	return nil
}

func (r *RRDFile) writeRow(ts int64, values []float32, rowOffset int64) error {
	// invalide record when ts changed
	if err := r.checkAndCleanRow(ts, rowOffset); err != nil {
		return err
	}
	// load old values
	var prevs []Value
	for i := 0; i < len(values); i++ {
		v, _ := loadValue(r.f)
		prevs = append(prevs, v)
	}

	if _, err := r.f.Seek(rowOffset+8, 0); err != nil {
		return err
	}
	for idx, val := range values {
		v := Value{
			Valid: true,
			Value: val,
		}
		v = r.columns[idx].Function.Apply(prevs[idx], v)
		if err := v.Write(r.f); err != nil {
			return err
		}
	}
	return nil
}

// Get value from database
func (r *RRDFile) Get(ts int64, cols []int) ([]Value, error) {
	if len(cols) == 0 {
		for i := 0; i < len(r.columns); i++ {
			cols = append(cols, i)
		}
	}

	for _, a := range r.archives {
		aTS := a.calcTS(ts)
		rowOffset, _ := a.calcOffset(aTS, 0, r.rowSize)

		// Read real ts
		if _, err := r.f.Seek(rowOffset, 0); err != nil {
			return nil, err
		}

		var rowTS int64
		if err := binary.Read(r.f, binary.LittleEndian, &rowTS); err != nil {
			return nil, err
		}

		if rowTS != aTS {
			// value not found in this archive, search in next
			continue
		}
		fmt.Printf("Getting from %s - %d, %v\n", a.Name, rowOffset, cols)
		return r.loadValues(rowOffset, rowTS, cols)
	}
	return nil, nil
}

func (r *RRDFile) loadValues(rowOffset int64, rowTS int64, cols []int) ([]Value, error) {
	var values []Value
	for _, col := range cols {
		if _, err := r.f.Seek(rowOffset+8+int64(col*valueSize), 0); err != nil {
			return nil, err
		}
		v, err := loadValue(r.f)
		if err == nil {
			v.TS = rowTS
		} else {
			return nil, err
		}
		values = append(values, v)
	}
	return values, nil
}

func (r *RRDFile) checkAndCleanRow(ts int64, tsOffset int64) error {
	if _, err := r.f.Seek(tsOffset, 0); err != nil {
		return err
	}
	var storeTS int64
	if err := binary.Read(r.f, binary.LittleEndian, &storeTS); err != nil {
		return err
	}
	if storeTS == ts {
		return nil
	}
	if _, err := r.f.Seek(tsOffset, 0); err != nil {
		return err
	}
	if err := r.writeEmptyRow(ts); err != nil {
		return err
	}
	if _, err := r.f.Seek(tsOffset+8, 0); err != nil {
		return err
	}
	return nil
}

func (r *RRDFile) writeEmptyRow(ts int64) error {
	if err := binary.Write(r.f, binary.LittleEndian, ts); err != nil {
		return err
	}
	v := Value{}
	for c := 0; c < int(r.header.ColumnsCount); c++ {
		if err := v.Write(r.f); err != nil {
			return err
		}
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
func (r *RRDFile) GetRange(minTS, maxTS int64, cols []int) (Rows, error) {
	if len(cols) == 0 {
		for i := 0; i < len(r.columns); i++ {
			cols = append(cols, i)
		}
	}

	last := r.Last()
	var archive *RRDArchive
	for _, a := range r.archives {
		archive = &a
		// archive range
		aMinTS := last - int64(a.Rows)*a.Step
		if minTS >= aMinTS {
			break
		}
	}
	fmt.Printf("Using archive %s\n", archive)
	rows, err := r.getRange(archive, minTS, maxTS, cols)
	if err == nil {
		sort.Sort(rows)
	}
	return rows, err
}

func (r *RRDFile) getRange(a *RRDArchive, min, max int64, cols []int) (Rows, error) {
	offset := a.archiveOffset
	var rows Rows
	for y := int32(0); y < a.Rows; y++ {
		if _, err := r.f.Seek(offset, 0); err != nil {
			return nil, err
		}
		var ts int64
		if err := binary.Read(r.f, binary.LittleEndian, &ts); err != nil {
			return nil, err
		}
		//fmt.Printf("ts=%d, minTS=%d,maxTS=%d\n", ts, minTS, maxTS)
		if ts > -1 && ts >= max && ts <= max {
			values, err := r.loadValues(offset, ts, cols)
			if err != nil {
				return nil, err
			}
			row := Row{
				TS:   ts,
				Cols: values,
			}
			rows = append(rows, row)
		}
		offset += int64(r.rowSize)
	}
	return rows, nil
}

// Last return last timestamp from db
func (r *RRDFile) Last() int64 {
	var last int64
	offset := r.archives[0].archiveOffset
	for y := 0; y < int(r.archives[0].Rows); y++ {
		if _, err := r.f.Seek(offset, 0); err != nil {
			return last
		}
		var ts int64
		if err := binary.Read(r.f, binary.LittleEndian, &ts); err != nil {
			return last
		}
		//fmt.Printf("ts=%d, minTS=%d,maxTS=%d\n", ts, minTS, maxTS)
		if ts >= 0 {
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

func calcArchiveOffsetSize(archives []RRDArchive, rowSize int, baseOffset int) (out []RRDArchive, allSize int64) {
	offset := int64(baseOffset)
	for _, a := range archives {
		a.archiveOffset = int64(offset)
		a.archiveSize = int64(int(a.Rows) * rowSize)
		out = append(out, a)
		offset += int64(a.archiveSize)
		allSize += a.archiveSize
	}
	return
}

func loadHeader(r io.Reader) (header rrdHeader, err error) {
	header = rrdHeader{}
	if err = binary.Read(r, binary.LittleEndian, &header.Version); err != nil {
		return
	}
	if header.Version != fileVersion {
		err = fmt.Errorf("invalid file (version)")
		return
	}

	if err = binary.Read(r, binary.LittleEndian, &header.ColumnsCount); err != nil {
		return
	}
	if err = binary.Read(r, binary.LittleEndian, &header.ArchivesCount); err != nil {
		return
	}
	if err = binary.Read(r, binary.LittleEndian, &header.Magic); err != nil {
		return
	}
	if header.Magic != fileMagic {
		err = fmt.Errorf("invalid file (magic)")
	}
	return

}

func writeHeader(w io.Writer, header rrdHeader) (err error) {
	if err = binary.Write(w, binary.LittleEndian, header.Version); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, header.ColumnsCount); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, header.ArchivesCount); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, header.Magic); err != nil {
		return
	}
	return nil
}

func loadColumnsDef(r io.Reader, colCount int) (cols []RRDColumn, err error) {
	for i := 0; i < colCount; i++ {
		buf := make([]byte, 16, 16)
		if err = binary.Read(r, binary.LittleEndian, &buf); err != nil {
			return
		}
		var funcID int32
		if err = binary.Read(r, binary.LittleEndian, &funcID); err != nil {
			return
		}
		col := RRDColumn{
			Name:     strings.TrimRight(string(buf), "\x00"),
			Function: Function(funcID),
		}
		cols = append(cols, col)
	}
	return
}

func writeColumnsDef(w io.Writer, cols []RRDColumn) (err error) {
	for _, col := range cols {
		name := make([]byte, 16, 16)
		if len(col.Name) > 16 {
			copy(name, []byte(col.Name[:16]))
		} else {
			copy(name, []byte(col.Name))
		}
		if err = binary.Write(w, binary.LittleEndian, name); err != nil {
			return
		}
		if err = binary.Write(w, binary.LittleEndian, int32(col.Function)); err != nil {
			return
		}
	}
	return
}

func loadArchiveDef(r io.Reader, archCount int) (archives []RRDArchive, err error) {
	for i := 0; i < archCount; i++ {
		buf := make([]byte, 16, 16)
		if err = binary.Read(r, binary.LittleEndian, &buf); err != nil {
			return
		}
		a := RRDArchive{
			Name: strings.TrimRight(string(buf), "\x00"),
		}
		if err = binary.Read(r, binary.LittleEndian, &a.Step); err != nil {
			return
		}
		if err = binary.Read(r, binary.LittleEndian, &a.Rows); err != nil {
			return
		}
		if err = binary.Read(r, binary.LittleEndian, &a.archiveSize); err != nil {
			return
		}
		if err = binary.Read(r, binary.LittleEndian, &a.archiveOffset); err != nil {
			return
		}
		archives = append(archives, a)
	}
	return
}

func writeArchivesDef(w io.Writer, archives []RRDArchive) (err error) {
	for _, a := range archives {
		name := make([]byte, 16, 16)
		if len(a.Name) > 16 {
			copy(name, []byte(a.Name[:16]))
		} else {
			copy(name, []byte(a.Name))
		}
		if err = binary.Write(w, binary.LittleEndian, name); err != nil {
			return
		}
		if err = binary.Write(w, binary.LittleEndian, a.Step); err != nil {
			return
		}
		if err = binary.Write(w, binary.LittleEndian, a.Rows); err != nil {
			return
		}
		if err = binary.Write(w, binary.LittleEndian, a.archiveSize); err != nil {
			return
		}
		if err = binary.Write(w, binary.LittleEndian, a.archiveOffset); err != nil {
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

func (r *RRDFile) iterate(archive *RRDArchive) *archIterator {
	return &archIterator{
		file:       r,
		archive:    archive,
		currentRow: -1,
		TS:         -1,
	}
}

func (a *archIterator) Next() (ts int64, ok bool) {
	for {
		if a.currentRow >= int(a.archive.Rows)-1 {
			return -1, false
		}
		a.currentRow++
		rowOffset := int64(a.currentRow*a.file.rowSize) + a.archive.archiveOffset
		if _, err := a.file.f.Seek(rowOffset, 0); err != nil {
			return -1, false
		}

		if err := binary.Read(a.file.f, binary.LittleEndian, &ts); err != nil {
			return -1, false
		}
		if ts >= 0 {
			a.TS = ts
			return ts, true
		}
	}
	a.TS = -1
	return -1, false
}

func (a *archIterator) Value(column int) (*Value, error) {
	if a.TS < 0 {
		return nil, fmt.Errorf("no next() or no data")
	}
	rowOffset := int64(a.currentRow*a.file.rowSize) + a.archive.archiveOffset
	valOffset := rowOffset + 8 + int64(column)*int64(valueSize)
	if _, err := a.file.f.Seek(valOffset, 0); err != nil {
		return nil, err
	}
	v, err := loadValue(a.file.f)
	if err != nil {
		return nil, err
	}
	v.TS = a.TS
	return &v, nil
}

func (a *archIterator) Values() ([]Value, error) {
	if a.TS < 0 {
		return nil, fmt.Errorf("no next() or no data")
	}
	rowOffset := int64(a.currentRow*a.file.rowSize) + a.archive.archiveOffset
	valOffset := rowOffset + 8
	if _, err := a.file.f.Seek(valOffset, 0); err != nil {
		return nil, err
	}
	var values []Value
	for col := 0; col < len(a.file.columns); col++ {
		v, err := loadValue(a.file.f)
		if err != nil {
			return nil, err
		}
		v.TS = a.TS
		values = append(values, v)
	}
	return values, nil

}

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

// Info return RRDFileInfo structure for current file
func (r *RRDFile) Info() (*RRDFileInfo, error) {
	res := &RRDFileInfo{
		Filename:      r.filename,
		ColumnsCount:  len(r.columns),
		ArchivesCount: len(r.archives),
		Columns:       r.columns,
	}

	for _, a := range r.archives {
		arch := RRDArchiveInfo{
			Name: a.Name,
			Rows: int(a.Rows),
			Step: a.Step,
		}
		// Count rows & Values
		if _, err := r.f.Seek(a.archiveOffset, 0); err != nil {
			return nil, err
		}
		iter := r.iterate(&a)
		for {
			ts, ok := iter.Next()
			if !ok {
				break
			}
			arch.UsedRows++
			if ts < arch.MinTS || arch.MinTS == 0 {
				arch.MinTS = ts
			}
			if ts > arch.MaxTS {
				arch.MaxTS = ts
			}
			values, err := iter.Values()
			if err != nil {
				return nil, err
			}
			for _, value := range values {
				if value.Valid {
					arch.Values++
				}
			}

		}
		res.Archives = append(res.Archives, arch)
	}
	return res, nil
}
