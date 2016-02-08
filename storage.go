package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/camlistore/lock"
)

/*
File format:
header
columns definitions[columns count]
archives definitions[archives count]
archives[archives count]

column[
	name byte[16]
	funcid int32
	flags int32
		- &1 - has minimum
		- &2 - has maximum
	minimum float32
	maximum float32
]
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
		Flags int32
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
	fileVersion     = int32(2)
	fileMagic       = int64(1038472294759683202)
	rrdHeaderSize   = 4 + 2 + 2 + 8
	rrdColumnSize   = 16 + 4
	rrdColumnSizeV2 = 16 + 4 + 4 + 4 + 4
	rrdArchiveSize  = 16 + 8 + 4 + 8 + 8
	valueSize       = 4 + 4 + 8

	hasMinimumFlag = 1
	hasMaximumFlag = 2
)

// Create new file
func (b *BinaryFileStorage) Create(filename string, columns []RRDColumn, archives []RRDArchive) error {
	LogDebug("BFS.Create filename=%s, columns=%v, archives=%v", filename, columns, archives)
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.f != nil {
		return fmt.Errorf("already open")
	}

	//	 TODO: check is exists
	LogDebug("BFS.Create locking")
	{
		flock, err := lock.Lock(filename + ".lock")
		if err != nil {
			return err
		}
		b.fLock = flock
	}

	LogDebug("BFS.Create creating file")
	f, err := os.Create(filename)
	if err != nil {
		return err
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
		b.columns = append(b.columns, bfColumn{c, 0})
	}

	allHeadersLen := rrdHeaderSize + rrdArchiveSize*len(archives)
	if b.header.Version == 1 {
		allHeadersLen += rrdColumnSize * len(columns)
	} else {
		allHeadersLen += rrdColumnSizeV2 * len(columns)
	}

	b.rowSize = valueSize*len(b.columns) + 8 // ts
	b.archives = calcArchiveOffsetSize(archives, b.rowSize, allHeadersLen)

	LogDebug("BFS.Create rowSize=%d, allHeadersLen=%d", b.rowSize, allHeadersLen)

	if err = writeHeader(f, b.header); err != nil {
		return err
	}
	if err = writeColumnsDef(f, b.columns, b.header.Version); err != nil {
		return err
	}
	if err = writeArchivesDef(f, b.archives); err != nil {
		return err
	}

	LogDebug("BFS.Create creating archive space")

	for _, a := range b.archives {
		for i := 0; i < int(a.Rows); i++ {
			if err := b.writeEmptyRow(-1); err != nil {
				return err
			}
		}
	}

	LogDebug("BFS.Create creating done")

	return f.Sync()
}

// Open existing file
func (b *BinaryFileStorage) Open(filename string, readonly bool) ([]RRDColumn, []RRDArchive, error) {
	LogDebug("BFS.Open filename=%s, readonly=%v", filename, readonly)

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.f != nil {
		return nil, nil, fmt.Errorf("already open")
	}

	LogDebug("BFS.Open locking file")
	{
		flock, err := lock.Lock(filename + ".lock")
		if err != nil {
			return nil, nil, err
		}
		b.fLock = flock
	}

	LogDebug("BFS.Open opening file")
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

	LogDebug("BFS.Open loading headers")
	if _, err = f.Seek(0, 0); err != nil {
		return nil, nil, err
	}
	b.header, err = loadHeader(b.f)
	if err != nil {
		return nil, nil, err
	}

	b.columns, err = loadColumnsDef(f, int(b.header.ColumnsCount), b.header.Version)
	if err != nil {
		return nil, nil, err
	}
	b.rowSize = valueSize*len(b.columns) + 8 // ts
	b.archives, err = loadArchiveDef(f, int(b.header.ArchivesCount), b.rowSize)
	if err != nil {
		return nil, nil, err
	}

	LogDebug("BFS.Open opening finished")
	return bfColumnToRRDColumn(b.columns), bfArchiveToRRDArchive(b.archives), err
}

// Close file
func (b *BinaryFileStorage) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	LogDebug("BFS.Close")

	if b.f == nil {
		return nil
	}

	err := b.f.Close()
	b.fLock.Close()

	b.f = nil

	LogDebug("BFS.Close done")
	return err
}

// Flush data to disk
func (b *BinaryFileStorage) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	LogDebug("BFS.Flush")

	if b.f != nil {
		b.f.Sync()
	}
	LogDebug("BFS.Flush finished")
}

// Put values into archive
func (b *BinaryFileStorage) Put(archive int, ts int64, values ...Value) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	LogDebug2("BFS.Put archive=%d, ts=%d, values=%v", archive, ts, values)

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

	LogDebug2("BFS.Put writing values")
	for _, v := range values {
		if _, err := b.f.Seek(rowOffset+8+int64(valueSize*v.Column), 0); err != nil {
			return err
		}
		writeValue(b.f, v)
	}

	LogDebug2("BFS.Put done")
	return nil
}

// Get values (selected columns) from archive
func (b *BinaryFileStorage) Get(archive int, ts int64, columns []int) ([]Value, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	LogDebug("BFS.Get archive=%d, ts=%d, columns=%v", archive, ts, columns)

	if b.f == nil {
		return nil, fmt.Errorf("closed file")
	}

	a := b.archives[archive]
	rowOffset := a.calcRowOffset(ts)

	LogDebug2("BFS.Get rowOffset=%d", rowOffset)

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
	LogDebug2("BFS.Get loading...")
	return b.loadValues(rowOffset, rowTS, columns, archive)
}

// Iterate create iterator for archive
func (b *BinaryFileStorage) Iterate(archive int, begin, end int64, columns []int) (RowsIterator, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	LogDebug("BFS.Iterate archive=%d, begin=%d, end=%d, columns=%v", archive, begin, end, columns)

	if b.f == nil {
		return nil, fmt.Errorf("closed file")
	}

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
	LogDebug2("BFS.loadValue ts=%d, column=%d, archive=%d", ts, column, archive)
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
	LogDebug2("BFS.loadValues rowOffset=%d, rowTD=%d, column=%d, archive=%d", rowOffset, rowTS, cols, archive)
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
	LogDebug2("BFS.checkAndCleanRow ts=%d, tsOffset=%d", ts, tsOffset)

	if _, err := b.f.Seek(tsOffset, 0); err != nil {
		return err
	}
	var storeTS int64
	if err := binary.Read(b.f, binary.LittleEndian, &storeTS); err != nil {
		return err
	}
	if storeTS == ts {
		LogDebug2("BFS.checkAndCleanRow not need to clean")
		return nil
	}
	LogDebug2("BFS.checkAndCleanRow cleaning")
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
	LogDebug2("BFS.Next [iter: row=%d, rowOffset=%d]", i.currentRow, i.rowOffset)

	i.mu.Lock()
	defer i.mu.Unlock()

	if i.file.f == nil {
		return fmt.Errorf("closed file")
	}

	a := i.file.archives[i.archive]
	for {
		if i.currentRow >= int(a.Rows)-1 {
			LogDebug2("BFS.Next eof - last row")
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
				LogDebug2("BFS.Next eof - lower value")
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
	LogDebug2("BFS.Value col=%d [iter: row=%d, rowOffset=%d]", column, i.currentRow, i.rowOffset)

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
	LogDebug2("BFS.Values [iter: row=%d, rowOffset=%d]", i.currentRow, i.rowOffset)
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
	LogDebug("BFS.loadHeader")
	header = bfHeader{}
	if err = binary.Read(r, binary.LittleEndian, &header.Version); err != nil {
		return
	}
	if header.Version > fileVersion {
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
	LogDebug("BFS.loadHeader finished cols=%d, archs=%d",
		header.ColumnsCount, header.ArchivesCount)
	return

}

func writeHeader(w io.Writer, header bfHeader) (err error) {
	LogDebug("BFS.writeHeader")
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
	LogDebug("BFS.writeHeader finished")
	return nil
}

func loadColumnsDef(r io.Reader, colCount int, version int32) (cols []bfColumn, err error) {
	LogDebug("BFS.loadColumnsDef")
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

				Name:       strings.TrimRight(string(buf), "\x00"),
				Function:   Function(funcID),
				HasMinimum: false,
				HasMaximum: false,
			},
		}
		if version > 1 {
			if err = binary.Read(r, binary.LittleEndian, &col.Flags); err != nil {
				return
			}
			col.RRDColumn.HasMinimum = col.Flags&hasMinimumFlag == hasMinimumFlag
			col.RRDColumn.HasMaximum = col.Flags&hasMaximumFlag == hasMaximumFlag
			if err = binary.Read(r, binary.LittleEndian, &col.RRDColumn.Minimum); err != nil {
				return
			}
			if err = binary.Read(r, binary.LittleEndian, &col.RRDColumn.Maximum); err != nil {
				return
			}
		}
		cols = append(cols, col)
	}
	LogDebug("BFS.loadColumnsDef finished cols num=%d", len(cols))
	return
}

func writeColumnsDef(w io.Writer, cols []bfColumn, version int32) (err error) {
	LogDebug("BFS.writeColumnsDef")
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
		if version > 1 {
			col.Flags = 0
			if col.RRDColumn.HasMinimum {
				col.Flags |= hasMinimumFlag
			}
			if col.RRDColumn.HasMaximum {
				col.Flags |= hasMaximumFlag
			}
			if err = binary.Write(w, binary.LittleEndian, col.Flags); err != nil {
				return
			}
			if err = binary.Write(w, binary.LittleEndian, col.RRDColumn.Minimum); err != nil {
				return
			}
			if err = binary.Write(w, binary.LittleEndian, col.RRDColumn.Maximum); err != nil {
				return
			}
		}
	}
	LogDebug("BFS.writeColumnsDef finished")
	return
}

func loadArchiveDef(r io.Reader, archCount int, rowSize int) (archives []bfArchive, err error) {
	LogDebug("BFS.loadArchiveDef archCount=%d, rowSize=%d", archCount, rowSize)
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
	LogDebug("BFS.loadArchiveDef archCount=%d", len(archives))
	return
}

func writeArchivesDef(w io.Writer, archives []bfArchive) (err error) {
	LogDebug("BFS.writeArchivesDef")
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
	LogDebug("BFS.writeArchivesDef finished")
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
