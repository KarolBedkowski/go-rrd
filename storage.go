package main

import (
	"encoding/binary"
	"fmt"
	"github.com/camlistore/lock"
	"io"
	"os"
	"strings"
	"sync"
)

/*
File format:
header
columns definitions[columns count]
archives definitions[archives count]
archives[archives count]
*/

type (
	// BinaryFileStorage use binary encoding for storing files
	BinaryFileStorage struct {
		mu sync.RWMutex

		filename string
		header   bfHeader
		readonly bool

		columns  []bfColumn
		archives []bfArchive

		f     *os.File
		fLock io.Closer

		rowSize int
	}

	// file header
	bfHeader struct {
		Version       int32
		ColumnsCount  int16
		ArchivesCount int16
		Magic         int64
	}

	// column definition
	bfColumn struct {
		RRDColumn
	}

	// archive definition
	bfArchive struct {
		RRDArchive

		archiveOffset int64
		archiveSize   int64
		rowSize       int64
	}

	// BinaryFileIterator is iterator for binary encoded file
	BinaryFileIterator struct {
		mu         sync.RWMutex
		file       *BinaryFileStorage
		archive    int
		currentRow int
		ts         int64
		begin      int64
		end        int64
		columns    []int
		rowOffset  int64
	}
)

const (
	fileVersion    = int32(1)
	fileMagic      = int64(1038472294759683202)
	rrdHeaderSize  = 4 + 2 + 2 + 8
	rrdColumnSize  = 16 + 4
	rrdArchiveSize = 16 + 8 + 4 + 8 + 8
	valueSize      = 4 + 4 + 8
)

// Create new file
func (b *BinaryFileStorage) Create(filename string, columns []RRDColumn, archives []RRDArchive) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.f != nil {
		return fmt.Errorf("already open")
	}

	f, err := os.Create(filename)
	if err != nil {
		return err
	}

	if flock, err := lock.Lock(filename + ".lock"); err != nil {
		return err
	} else {
		b.fLock = flock
	}

	b.f = f
	b.filename = filename
	b.header = bfHeader{
		Version:       fileVersion,
		ColumnsCount:  int16(len(columns)),
		ArchivesCount: int16(len(archives)),
		Magic:         fileMagic,
	}

	for _, c := range columns {
		b.columns = append(b.columns, bfColumn{c})
	}

	allHeadersLen := rrdHeaderSize + rrdColumnSize*len(columns) +
		rrdArchiveSize*len(archives)

	b.rowSize = valueSize*len(b.columns) + 8 // ts
	b.archives = calcArchiveOffsetSize(archives, b.rowSize, allHeadersLen)

	if err = writeHeader(f, b.header); err != nil {
		return err
	}
	if err = writeColumnsDef(f, b.columns); err != nil {
		return err
	}
	if err = writeArchivesDef(f, b.archives); err != nil {
		return err
	}

	for _, a := range b.archives {
		for i := 0; i < int(a.Rows); i++ {
			if err := b.writeEmptyRow(-1); err != nil {
				return err
			}
		}
	}

	return f.Sync()
}

// Open existing file
func (b *BinaryFileStorage) Open(filename string, readonly bool) ([]RRDColumn, []RRDArchive, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.f != nil {
		return nil, nil, fmt.Errorf("already open")
	}

	if flock, err := lock.Lock(filename + ".lock"); err != nil {
		return nil, nil, err
	} else {
		b.fLock = flock
	}

	flag := os.O_RDWR
	if readonly {
		flag = os.O_RDONLY
	}
	f, err := os.OpenFile(filename, flag, 0660)
	if err != nil {
		return nil, nil, err
	}

	b.f = f
	b.filename = filename
	b.readonly = readonly

	if _, err = f.Seek(0, 0); err != nil {
		return nil, nil, err
	}
	b.header, err = loadHeader(b.f)
	if err != nil {
		return nil, nil, err
	}

	b.columns, err = loadColumnsDef(f, int(b.header.ColumnsCount))
	if err != nil {
		return nil, nil, err
	}
	b.rowSize = valueSize*len(b.columns) + 8 // ts
	b.archives, err = loadArchiveDef(f, int(b.header.ArchivesCount), b.rowSize)
	if err != nil {
		return nil, nil, err
	}

	return bfColumnToRRDColumn(b.columns), bfArchiveToRRDArchive(b.archives), err
}

// Close file
func (b *BinaryFileStorage) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.f == nil {
		return nil
	}

	err := b.f.Close()
	b.fLock.Close()

	b.f = nil

	return err
}

func (b *BinaryFileStorage) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.f != nil {
		b.f.Sync()
	}
}

// Put values into archive
func (b *BinaryFileStorage) Put(archive int, ts int64, values ...Value) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.f == nil {
		return fmt.Errorf("closed file")
	}

	if b.readonly {
		return fmt.Errorf("RRD file open as read-only")
	}
	a := b.archives[archive]
	rowOffset := a.calcRowOffset(ts)

	// invalidate record when ts changed
	if err := b.checkAndCleanRow(ts, rowOffset); err != nil {
		return err
	}

	for _, v := range values {
		if _, err := b.f.Seek(rowOffset+8+int64(valueSize*v.Column), 0); err != nil {
			return err
		}
		writeValue(b.f, v)
	}
	return nil
}

// Get values (selected columns) from archive
func (b *BinaryFileStorage) Get(archive int, ts int64, columns []int) ([]Value, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.f == nil {
		return nil, fmt.Errorf("closed file")
	}

	a := b.archives[archive]
	rowOffset := a.calcRowOffset(ts)

	// Read real ts
	if _, err := b.f.Seek(rowOffset, 0); err != nil {
		return nil, err
	}

	var rowTS int64
	if err := binary.Read(b.f, binary.LittleEndian, &rowTS); err != nil {
		return nil, err
	}
	if rowTS != ts {
		// value not found in this archive, search in next
		return nil, nil
	}
	//fmt.Printf("Getting from %s - %d, %v\n", a.Name, rowOffset, cols)
	return b.loadValues(rowOffset, rowTS, columns, archive)
}

// Iterate create iterator for archive
func (b *BinaryFileStorage) Iterate(archive int, begin, end int64, columns []int) (RowsIterator, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.f == nil {
		return nil, fmt.Errorf("closed file")
	}
	//fmt.Printf("archive=%d, begin=%d, end=%d, columns=%#v\n", archive, begin, end, columns)
	return &BinaryFileIterator{
		file:       b,
		archive:    archive,
		currentRow: -1,
		ts:         -1,
		begin:      begin,
		end:        end,
		columns:    columns,
	}, nil
}

func (b *BinaryFileStorage) loadValue(ts int64, column, archive int) (v Value, err error) {
	v = Value{
		TS:        ts,
		Column:    column,
		ArchiveID: archive,
	}
	if err = binary.Read(b.f, binary.LittleEndian, &v.Value); err != nil {
		return
	}
	if err = binary.Read(b.f, binary.LittleEndian, &v.Counter); err != nil {
		return
	}
	var valid int32
	if err = binary.Read(b.f, binary.LittleEndian, &valid); err != nil {
		return
	}
	v.Valid = valid == 1
	return
}

func (b *BinaryFileStorage) loadValues(rowOffset int64, rowTS int64, cols []int, archive int) ([]Value, error) {
	var values []Value
	for _, col := range cols {
		if _, err := b.f.Seek(rowOffset+8+int64(col*valueSize), 0); err != nil {
			return nil, err
		}
		v, err := b.loadValue(rowTS, col, archive)
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	return values, nil
}

func (b *BinaryFileStorage) checkAndCleanRow(ts int64, tsOffset int64) error {
	if _, err := b.f.Seek(tsOffset, 0); err != nil {
		return err
	}
	var storeTS int64
	if err := binary.Read(b.f, binary.LittleEndian, &storeTS); err != nil {
		return err
	}
	if storeTS == ts {
		return nil
	}
	if storeTS > ts {
		return fmt.Errorf("updating by older value not allowed")
	}
	if _, err := b.f.Seek(tsOffset, 0); err != nil {
		return err
	}
	if err := b.writeEmptyRow(ts); err != nil {
		return err
	}
	if _, err := b.f.Seek(tsOffset+8, 0); err != nil {
		return err
	}
	return nil
}

func (b *BinaryFileStorage) writeEmptyRow(ts int64) error {
	if err := binary.Write(b.f, binary.LittleEndian, ts); err != nil {
		return err
	}
	v := Value{}
	for c := 0; c < int(b.header.ColumnsCount); c++ {
		if err := writeValue(b.f, v); err != nil {
			return err
		}
	}
	return nil
}

// TS is time stamp
func (i *BinaryFileIterator) TS() int64 {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.ts
}

// Next move to next row, return io.EOF on error
func (i *BinaryFileIterator) Next() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.file.f == nil {
		return fmt.Errorf("closed file")
	}

	a := i.file.archives[i.archive]
	for {
		if i.currentRow >= int(a.Rows)-1 {
			return io.EOF
		}
		i.currentRow++
		rowOffset := int64(i.currentRow*i.file.rowSize) + a.archiveOffset
		if _, err := i.file.f.Seek(rowOffset, 0); err != nil {
			return err
		}
		var ts int64
		if err := binary.Read(i.file.f, binary.LittleEndian, &ts); err != nil {
			return err
		}
		if ts >= i.begin {
			if i.end > -1 && ts > i.end {
				return io.EOF
			}
			i.ts = ts
			i.rowOffset = rowOffset
			return nil
		}
	}
}

// Value return value for one column in current row
func (i *BinaryFileIterator) Value(column int) (*Value, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.file.f == nil {
		return nil, fmt.Errorf("closed file")
	}

	if i.ts < 0 {
		return nil, fmt.Errorf("no next() or no data")
	}
	valOffset := i.rowOffset + 8 + int64(column)*int64(valueSize)
	if _, err := i.file.f.Seek(valOffset, 0); err != nil {
		return nil, err
	}
	v, err := i.file.loadValue(i.ts, column, i.archive)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// Values return all values according to columns defined during creating iterator
func (i *BinaryFileIterator) Values() (values []Value, err error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.file.f == nil {
		return nil, fmt.Errorf("closed file")
	}

	if i.ts < 0 {
		return nil, fmt.Errorf("no next() or no data")
	}
	for _, col := range i.columns {
		valOffset := i.rowOffset + 8 + int64(col)*int64(valueSize)
		if _, err = i.file.f.Seek(valOffset, 0); err != nil {
			return nil, err
		}
		var v Value
		if v, err = i.file.loadValue(i.ts, col, i.archive); err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	return values, nil
}

func (a *bfArchive) calcRowOffset(ts int64) (rowOffset int64) {
	rowNum := (ts / a.Step) % int64(a.Rows)
	rowOffset = int64(a.archiveOffset) + a.rowSize*rowNum
	return
}

func bfColumnToRRDColumn(cols []bfColumn) (res []RRDColumn) {
	for _, c := range cols {
		res = append(res, c.RRDColumn)
	}
	return
}

func bfArchiveToRRDArchive(a []bfArchive) (res []RRDArchive) {
	for _, c := range a {
		res = append(res, c.RRDArchive)
	}
	return
}

func calcArchiveOffsetSize(archives []RRDArchive, rowSize int, baseOffset int) (out []bfArchive) {
	offset := int64(baseOffset)
	for _, a := range archives {
		bfa := bfArchive{
			RRDArchive: a,
			rowSize:    int64(rowSize),
		}
		bfa.archiveOffset = int64(offset)
		bfa.archiveSize = int64(int(a.Rows) * rowSize)
		out = append(out, bfa)
		offset += int64(bfa.archiveSize)
	}
	return
}

func loadHeader(r io.Reader) (header bfHeader, err error) {
	header = bfHeader{}
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

func writeHeader(w io.Writer, header bfHeader) (err error) {
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

func loadColumnsDef(r io.Reader, colCount int) (cols []bfColumn, err error) {
	for i := 0; i < colCount; i++ {
		buf := make([]byte, 16, 16)
		if err = binary.Read(r, binary.LittleEndian, &buf); err != nil {
			return
		}
		var funcID int32
		if err = binary.Read(r, binary.LittleEndian, &funcID); err != nil {
			return
		}
		col := bfColumn{
			RRDColumn: RRDColumn{

				Name:     strings.TrimRight(string(buf), "\x00"),
				Function: Function(funcID),
			},
		}
		cols = append(cols, col)
	}
	return
}

func writeColumnsDef(w io.Writer, cols []bfColumn) (err error) {
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

func loadArchiveDef(r io.Reader, archCount int, rowSize int) (archives []bfArchive, err error) {
	for i := 0; i < archCount; i++ {
		buf := make([]byte, 16, 16)
		if err = binary.Read(r, binary.LittleEndian, &buf); err != nil {
			return
		}
		a := bfArchive{
			RRDArchive: RRDArchive{
				Name: strings.TrimRight(string(buf), "\x00"),
			},
			rowSize: int64(rowSize),
		}
		if err = binary.Read(r, binary.LittleEndian, &a.RRDArchive.Step); err != nil {
			return
		}
		if err = binary.Read(r, binary.LittleEndian, &a.RRDArchive.Rows); err != nil {
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

func writeArchivesDef(w io.Writer, archives []bfArchive) (err error) {
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

func writeValue(w io.Writer, v Value) (err error) {
	if err = binary.Write(w, binary.LittleEndian, v.Value); err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, v.Counter); err != nil {
		return
	}
	if v.Valid {
		err = binary.Write(w, binary.LittleEndian, int32(1))
	} else {
		err = binary.Write(w, binary.LittleEndian, int32(0))
	}
	return
}
