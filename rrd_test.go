package main

import (
	"testing"
)

func TestRRDArchiveCalcTS(t *testing.T) {
	a := RRDArchive{
		Step: 60,
	}

	data := [][]int64{
		{-1, -1},
		{0, 0},
		{1, 0},
		{59, 0},
		{60, 60},
		{61, 60},
		{120, 120},
		{3600, 3600},
	}

	for _, d := range data {
		res := a.calcTS(d[0])
		if res != d[1] {
			t.Errorf("wrong result for %d - expected %d != %d", d[0], d[1], res)
		}
	}
}

func TestFuncs(t *testing.T) {
	data := []struct {
		f        Function
		values   []float32
		expected float32
	}{
		{FAverage, []float32{1, 2, 3}, 2},
		{FAverage, []float32{22}, 22},
		{FAverage, []float32{10, 5, 0, 5, 25, 15}, 10},
		{FCount, []float32{22, 32, 32, 12, 213}, 5},
		{FSum, []float32{10, 5, 0, 5, 25, 15}, 60},
		{FMinimum, []float32{10, 5, 0, 5, 25, 15}, 0},
		{FMaximum, []float32{10, 5, 0, 5, 25, 15}, 25},
		{FLast, []float32{10, 5, 0, 5, 25, 15}, 15},
	}

	for _, d := range data {
		res := Value{Valid: true, Value: d.values[0], Counter: 1}
		for _, v := range d.values[1:] {
			res = d.f.Apply(res, Value{Valid: true, Value: v})
		}
		if res.Value != d.expected {
			t.Errorf("wrong result for %#v: %v", d, res)
		}
	}
}
func TestNewRRD(t *testing.T) {
	r, c, a := createTestDB(t)
	closeTestDb(t, r)

	r2, err := OpenRRD("tmp.rdb", true)
	if err != nil {
		t.Errorf("OpenRRD error: %s", err.Error())
		return
	}
	if r2 == nil {
		t.Errorf("OpenRRD empty result")
		return
	}

	if len(r2.columns) != len(c) {
		ainfo, _ := r2.Info()
		t.Logf("info: %#v\n", ainfo)

		t.Errorf("OpenRRD different columns: %v - %v", r2.columns, c)
	}
	for i, col := range c {
		if r2.columns[i] != col {
			ainfo, _ := r2.Info()
			t.Logf("info: %#v\n", ainfo)
			t.Errorf("OpenRRD different column: %d:  %v - %v", i, r2.columns[i], col)
		}
	}

	if len(r2.archives) != len(a) {
		ainfo, _ := r2.Info()
		t.Logf("info: %#v\n", ainfo)
		t.Errorf("OpenRRD different archives: %v - %v", r2.archives, a)
	}
	for i, arch := range a {
		if r2.archives[i] != arch {
			ainfo, _ := r2.Info()
			t.Logf("info: %#v\n", ainfo)
			t.Errorf("OpenRRD different archive: %d:  %v - %v", i, r2.archives[i], arch)
		}
	}

	if err := r2.Close(); err != nil {
		t.Errorf("OpenRRD close error: %s", err.Error())
		return
	}
}

func TestInfo(t *testing.T) {
	r, c, a := createTestDB(t)
	defer closeTestDb(t, r)

	// sample data
	testV := []int{10, 10, 12, 13, 15, 20, 21, 22, 25, 30, 32,
		102, 200, 300, 400, 1000, 1200, 1300, 3000, 4000, 5000, 6000,
		9100, 21000, 25000, 33000}
	for _, v := range testV {
		for i := 0; i < 6; i++ {
			if err := r.Put(int64(v), i, float32(v)); err != nil {
				t.Errorf("Put error: %s", err.Error())
			}
		}
	}

	info, err := r.Info()
	if err != nil {
		t.Errorf("Info error: %s", err.Error())
		return
	}

	if info.ArchivesCount != len(a) {
		t.Logf("dump: %s", r.LowLevelDebugDump())
		t.Errorf("wrong number of archives: %d, expected %d", info.ArchivesCount, len(a))
	}

	for i, ar := range a {
		ia := info.Archives[i]
		if ia.Name != ar.Name {
			t.Errorf("wrong archive name: %v, expected %v", ia, ar)
		}
		if ia.Rows != int(ar.Rows) {
			t.Errorf("wrong archive rows: %v, expected %v", ia, ar)
		}
		if ia.Step != ar.Step {
			t.Errorf("wrong archive step: %v, expected %v", ia, ar)
		}
		if ia.MaxTS != 33000 {
			t.Log("dump ", r.LowLevelDebugDump())
			t.Logf("info %#v", ia)
			t.Errorf("wrong archive MaxTS: %v, expected 33000", ia.MinTS)
		}

	}
	ia := info.Archives[0]
	if ia.MinTS != 10 {
		t.Log("dump ", r.LowLevelDebugDump())
		t.Logf("info %#v", ia)
		t.Errorf("wrong archive MinTS: %v, expected 10", ia.MinTS)
	}

	ia = info.Archives[1]
	if ia.MinTS != 0 {
		t.Log("dump ", r.LowLevelDebugDump())
		t.Logf("info %#v", ia)
		t.Errorf("wrong archive MinTS: %v, expected 10", ia.MinTS)
	}

	ia = info.Archives[2]
	if ia.MinTS != 0 {
		t.Log("dump ", r.LowLevelDebugDump())
		t.Logf("info %#v", ia)
		t.Errorf("wrong archive MinTS: %v, expected 10", ia.MinTS)
	}

	if info.ColumnsCount != len(c) {
		t.Logf("dump: %s", r.LowLevelDebugDump())
		t.Errorf("wrong number of columns: %d, expected %d", info.ColumnsCount, len(c))
	}
}

func TestPutData(t *testing.T) {
	r, _, _ := createTestDB(t)
	defer closeTestDb(t, r)

	if err := r.Put(10, 0, 100.0); err != nil {
		ainfo, _ := r.Info()
		t.Logf("info: %#v\n", ainfo)
		t.Errorf("Put error: %s", err.Error())
	}

	if last, err := r.Last(); err == nil {
		if last != 10 {
			ainfo, _ := r.Info()
			t.Logf("info: %#v\n", ainfo)
			t.Errorf("Last value wrong val, last=%d, expected 10", last)
		}
	} else {
		t.Errorf("Last get error: %s", err.Error())
	}

	if values, err := r.Get(10, 0); err != nil {
		ainfo, _ := r.Info()
		t.Logf("info: %#v\n", ainfo)
		t.Errorf("Get error: %s", err.Error())
	} else {
		if len(values) != 1 {
			t.Errorf("Get error: wrong number of values %#v", values)
		} else {
			val := values[0]
			if val.Value != 100.0 {
				t.Errorf("Get error - wrong value; %v != 100.0", val.Value)
			}
			if val.TS != 10 {
				t.Logf("dump: %s", r.LowLevelDebugDump())
				t.Errorf("Get error - wrong value.TS ; %v != 10", val.TS)
			}
			if !val.Valid {
				t.Errorf("Get error - value not valid")
			}
		}
	}

	// get not existing
	if values, err := r.Get(10100000, 0); err != nil {
		ainfo, _ := r.Info()
		t.Logf("info: %#v\n", ainfo)
		t.Errorf("Get error: %s", err.Error())
	} else {
		if values != nil {
			t.Errorf("Get error: found data for not-existing keys: %#v", values)
		}
	}

	// update value
	if err := r.Put(10, 0, 200.0); err != nil {
		ainfo, _ := r.Info()
		t.Logf("info: %#v\n", ainfo)
		t.Errorf("Put error: %s", err.Error())
	}

	if last, err := r.Last(); err == nil {
		if last != 10 {
			ainfo, _ := r.Info()
			t.Logf("info: %#v\n", ainfo)
			t.Errorf("Last value wrong val, last=%d, expected 10", last)
		}
	} else {
		t.Errorf("Last get error: %s", err.Error())
	}

	// Get update value
	if values, err := r.Get(10, 0); err != nil {
		ainfo, _ := r.Info()
		t.Logf("info: %#v\n", ainfo)
		t.Errorf("Get error: %s", err.Error())
	} else {
		if len(values) != 1 {
			t.Errorf("Get error: wrong number of values %#v", values)
		} else {
			val := values[0]
			if val.Value != 200.0 {
				t.Errorf("Get error - wrong value; %v != 200.0", val.Value)
			}
			if val.TS != 10 {
				t.Logf("dump: %s", r.LowLevelDebugDump())
				t.Errorf("Get error - wrong value.TS ; %v != 10", val.TS)
			}
			if !val.Valid {
				t.Errorf("Get error - value not valid")
			}
		}
	}

	if err := r.Put(20, 0, 300.0); err != nil {
		ainfo, _ := r.Info()
		t.Logf("info: %#v\n", ainfo)
		t.Errorf("Put error: %s", err.Error())
	}

	if last, err := r.Last(); err == nil {
		if last != 20 {
			ainfo, _ := r.Info()
			t.Logf("info: %#v\n", ainfo)
			t.Errorf("Last value wrong val, last=%d, expected 10", last)
		}
	} else {
		t.Errorf("Last get error: %s", err.Error())
	}

}

func TestPutDataRR1(t *testing.T) {
	r, _, _ := createTestDB(t)
	defer closeTestDb(t, r)
	// update value

	for i := 0; i < 600; i = i + 10 {
		if err := r.Put(int64(i), 0, float32(i)); err != nil {
			t.Errorf("Put error: %s", err.Error())
		}
	}

	if last, err := r.Last(); err == nil {
		if last != 590 {
			ainfo, _ := r.Info()
			t.Logf("info: %#v\n", ainfo)
			t.Logf("dump: %s", r.LowLevelDebugDump())
			t.Errorf("Last value wrong val, last=%d, expected 590", last)
		}
	} else {
		t.Errorf("Last get error: %s", err.Error())
	}

	for i := 0; i < 600; i = i + 10 {
		if values, err := r.Get(int64(i), 0); err != nil {
			ainfo, _ := r.Info()
			t.Logf("info: %#v\n", ainfo)
			t.Errorf("Get error: %s", err.Error())
		} else {
			if len(values) != 1 {
				t.Errorf("Get error: wrong number of values %#v", values)
			} else {
				val := values[0]
				if int(val.Value) != i {
					t.Errorf("Get error - wrong value; %v != %v", val.Value, i)
				}
				if val.TS != int64(i) {
					t.Logf("dump: %s", r.LowLevelDebugDump())
					t.Errorf("Get error - wrong value.TS ; %v != %d", val.TS, i)
				}
				if !val.Valid {
					t.Errorf("Get error - value not valid")
				}
			}
		}
	}

}

func TestPutDataRR2(t *testing.T) {
	r, _, _ := createTestDB(t)
	defer closeTestDb(t, r)
	// update value

	for i := 0; i < 600; i = i + 1 {
		if err := r.Put(int64(i), 0, float32(i)); err != nil {
			t.Errorf("Put error: %s", err.Error())
		}
	}

	if last, err := r.Last(); err == nil {
		if last != 590 {
			ainfo, _ := r.Info()
			t.Logf("info: %#v\n", ainfo)
			t.Logf("dump: %s", r.LowLevelDebugDump())
			t.Errorf("Last value wrong val, last=%d, expected 590", last)
		}
	} else {
		t.Errorf("Last get error: %s", err.Error())
	}

	for i := 0; i < 600; i = i + 10 {
		if values, err := r.Get(int64(i), 0); err != nil {
			ainfo, _ := r.Info()
			t.Logf("info: %#v\n", ainfo)
			t.Errorf("Get error: %s", err.Error())
		} else {
			if len(values) != 1 {
				t.Errorf("Get error: wrong number of values %#v", values)
			} else {
				val := values[0]
				exp := i + 9
				if int(val.Value) != exp {
					t.Errorf("Get error - wrong value; %v != %v", val.Value, exp)
				}
				if val.TS != int64(i) {
					t.Logf("dump: %s", r.LowLevelDebugDump())
					t.Errorf("Get error - wrong value.TS ; %v != %d", val.TS, i)
				}
				if !val.Valid {
					t.Errorf("Get error - value not valid")
				}
			}
		}
	}
}

func TestPutDataRR3(t *testing.T) {
	r, _, _ := createTestDB(t)
	defer closeTestDb(t, r)

	testV := []int{
		10,   // row 1 - replaced by 610
		30,   // row 3
		100,  // row 10 - replaced by 700
		500,  // row 50 - replaced by 1100
		610,  // row 1
		200,  // row 20 - replaced by 800
		300,  // row 30
		700,  // row 10
		800,  // row 20
		1000, // row 40 -
		1100, // row 50
		1200, // row 0
	}

	missing := map[int]bool{
		10:  true,
		100: true,
		500: true,
		200: true,
	}

	for _, v := range testV {
		if err := r.Put(int64(v), 0, float32(v)); err != nil {
			t.Errorf("Put error: %s", err.Error())
		}
	}

	for _, v := range testV {
		if values, err := r.getFromArchive(0, int64(v), []int{0}); err != nil {
			ainfo, _ := r.Info()
			t.Logf("info: %#v\n", ainfo)
			t.Errorf("Get error: %s", err.Error())
		} else {
			if values == nil || len(values) != 1 {
				if _, ok := missing[v]; !ok {
					t.Logf("dump: %s", r.LowLevelDebugDump())
					t.Errorf("Get error: wrong number of values for %v - %#v", v, values)
				}
			} else {
				if _, ok := missing[v]; ok {
					t.Logf("dump: %s", r.LowLevelDebugDump())
					t.Errorf("Get error: values for %v shouldn't exist -  %#v", v, values)
				}
				val := values[0]
				exp := v
				if int(val.Value) != exp {
					t.Errorf("Get error - wrong value; %v != %v", val.Value, exp)
				}
				if val.TS != int64(v) {
					t.Logf("dump: %s", r.LowLevelDebugDump())
					t.Errorf("Get error - wrong value.TS ; %v != %d", val.TS, v)
				}
				if !val.Valid {
					t.Errorf("Get error - value not valid")
				}
			}
		}
	}

}

func TestPutDataRR4(t *testing.T) {
	// Test dupicates
	r, _, _ := createTestDB(t)
	defer closeTestDb(t, r)

	if err := r.Put(int64(10), 0, 10); err != nil {
		t.Errorf("Put error: %s", err.Error())
	}

	if err := r.Put(int64(610), 0, 610); err != nil {
		t.Errorf("Put error: %s", err.Error())
	}

	if err := r.Put(int64(1810), 0, 1810); err != nil {
		t.Errorf("Put error: %s", err.Error())
	}

	// insert lower value - expecting error
	if err := r.Put(int64(1210), 0, 1210); err == nil {
		t.Errorf("Put lower value - missing error")
	}

	// this values shouldn't be found
	if values, err := r.getFromArchive(0, 10, []int{0}); err == nil && len(values) > 0 {
		t.Errorf("Get error: values for 10 found:  %v", values)
		t.Logf("dump: %s", r.LowLevelDebugDump())
	}
	if values, err := r.getFromArchive(0, 610, []int{0}); err == nil && len(values) > 0 {
		t.Errorf("Get error: values for 610 found:  %v", values)
		t.Logf("dump: %s", r.LowLevelDebugDump())
	}
	// this value should exist
	if values, err := r.getFromArchive(0, 1810, []int{0}); err != nil || len(values) != 1 {
		t.Errorf("Get error: values for 1810 not found:  %v, %s", values, err)
		t.Logf("dump: %s", r.LowLevelDebugDump())
	} else {
		v := values[0]
		if int(v.Value) != 1810 || v.TS != 1810 {
			t.Errorf("Get error: wrong value: %v, expecting 1810", values)
			t.Logf("dump: %s", r.LowLevelDebugDump())
		}
	}
}

func TestPutDataFuncs(t *testing.T) {
	// Test agregations
	r, _, _ := createTestDB(t)
	defer closeTestDb(t, r)

	testV := []int{10, 10, 12, 13, 15, 20, 21, 22, 25, 30, 32}

	for _, v := range testV {
		for i := 0; i < 6; i++ {
			if err := r.Put(int64(v), i, float32(v)); err != nil {
				t.Errorf("Put error: %s", err.Error())
			}
		}
	}

	expected := [][]int{
		{10, 15, 12, 60, 10, 15, 5},
		{20, 25, 22, 88, 20, 25, 4},
		{30, 32, 31, 62, 30, 32, 2},
	}

	for _, e := range expected {
		// last
		if values, err := r.getFromArchive(0, int64(e[0]), []int{0, 1, 2, 3, 4, 5}); err != nil {
			t.Errorf("Get error: %s", err.Error())
			t.Logf("dump: %s", r.LowLevelDebugDump())
		} else {
			if len(values) != len(e)-1 {
				t.Errorf("Get value - not found %v - found: %v", e, values)
				t.Logf("dump: %s", r.LowLevelDebugDump())
			} else {
				for col, exp := range e[1:] {
					v := int(values[col].Value)
					if v != exp {
						t.Errorf("Get value - wrong value for col %d -  %v, expected %v", col, v, exp)
						t.Logf("dump: %s", r.LowLevelDebugDump())
					}
				}
			}
		}
	}
}

func TestRange(t *testing.T) {
	r, _, _ := createTestDB(t)
	defer closeTestDb(t, r)

	// sample data
	testV := []int{50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550,
		600, 650, 700, 750, 800, 850, 900, 950, 1000}
	for _, v := range testV {
		if err := r.Put(int64(v), 0, float32(v)); err != nil {
			t.Errorf("Put error: %s", err.Error())
		}
	}

	// get all - should use arch "a1"
	if vls, err := r.GetRange(0, -1, []int{0}); err != nil {
		t.Errorf("GetRange error: %s", err.Error())
	} else {
		if len(vls) != 17 {
			t.Log("dump ", r.LowLevelDebugDump())
			t.Errorf("wrong result len: %v", vls)
		}
		exp := [][]int{
			{0, 50}, {60, 100}, {120, 150}, {180, 200}, {240, 250}, {300, 350},
			{360, 400}, {420, 450}, {480, 500}, {540, 550}, {600, 650}, {660, 700},
			{720, 750}, {780, 800}, {840, 850}, {900, 950}, {960, 1000},
		}
		for i, e := range exp {
			ts := vls[i].TS
			if int(ts) != e[0] {
				t.Errorf("wrong ts on pos %d: %v - expected %v", i, ts, e[0])
			}
			v := vls[i].Values[0]
			if int(v.Value) != e[1] {
				t.Errorf("wrong result on pos %d: %v - expected %v", i, v, e[1])
			}
			if int(v.ArchiveID) != 1 {
				t.Errorf("wrong result on pos %d: %v wrong archive - expected %v", i, v, e[1])
			}
		}
	}

	// get last - should use arch "a0"
	if vls, err := r.GetRange(400, 1000, []int{0}); err != nil {
		t.Errorf("GetRange error: %s", err.Error())
	} else {
		exp := [][]int{
			{450, 450}, {500, 500}, {550, 550}, {600, 600}, {650, 650},
			{700, 700}, {750, 750}, {800, 800}, {850, 850}, {900, 900},
			{950, 950}, {1000, 1000},
		}

		if len(vls) != len(exp) {
			t.Log("dump ", r.LowLevelDebugDump())
			t.Errorf("wrong result len: %v", vls)
		}

		for i, e := range exp {
			ts := vls[i].TS
			if int(ts) != e[0] {
				t.Errorf("wrong ts on pos %d: %v - expected %v", i, ts, e[0])
			}
			v := vls[i].Values[0]
			if int(v.Value) != e[1] {
				t.Errorf("wrong result on pos %d: %v - expected %v", i, v, e[1])
			}
			if int(v.ArchiveID) != 0 {
				t.Errorf("wrong result on pos %d: %v wrong archive - expected 0", i, v)
			}
		}
	}
}

func createTestDB(t *testing.T) (*RRD, []RRDColumn, []RRDArchive) {
	c := []RRDColumn{
		RRDColumn{"col1", FLast},
		RRDColumn{"col2", FAverage},
		RRDColumn{"col3", FSum},
		RRDColumn{"col4", FMinimum},
		RRDColumn{"col5", FMaximum},
		RRDColumn{"col6", FCount},
	}
	a := []RRDArchive{
		RRDArchive{"a0", 10, 60},
		RRDArchive{"a1", 60, 300},
		RRDArchive{"a2", 300, 1800},
	}
	r, err := NewRRD("tmp.rdb", c, a)
	if err != nil {
		t.Errorf("NewRRD error: %s", err.Error())
		return nil, nil, nil
	}
	if r == nil {
		t.Errorf("NewRRD empty result")
		return nil, nil, nil
	}
	return r, c, a
}

func closeTestDb(t *testing.T, r *RRD) {
	err := r.Close()
	if err != nil {
		t.Errorf("NewRRD close error: %s", err.Error())
	}
}
