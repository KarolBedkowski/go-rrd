package main

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	//Debug = true
	os.Exit(m.Run())
}

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
	defer r2.Close()
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
	testV := []int{1, 10, 10, 12, 13, 15, 20, 21, 22, 25, 30, 32,
		102, 200, 300, 400, 1000, 1200, 1300, 3000, 4000, 5000, 6000,
		9100, 21000, 25000, 33000}
	if errors := putTestDataInts(r, testV, 0, 1, 2, 3, 4, 5); len(errors) > 0 {
		t.Errorf("Put data error: %v", errors)
		return
	}

	info, err := r.Info()
	if err != nil {
		t.Errorf("Info error: %s", err.Error())
		return
	}

	if info.ArchivesCount != len(a) {
		t.Errorf("wrong number of archives: %d, expected %d", info.ArchivesCount, len(a))
		t.Logf("dump: %s", r.LowLevelDebugDump())
		return
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
			t.Errorf("wrong archive MaxTS: %v, expected 33000", ia.MinTS)
			t.Log("dump ", r.LowLevelDebugDump())
			t.Logf("info %#v", ia)
		}

	}
	ia := info.Archives[0]
	if ia.MinTS != 13 {
		t.Log("dump ", r.LowLevelDebugDump())
		t.Errorf("wrong archive MinTS: %v, expected 13", ia.MinTS)
	}

	ia = info.Archives[1]
	if ia.MinTS != 10 {
		t.Errorf("wrong archive MinTS: %v, expected 10", ia.MinTS)
		t.Log("dump ", r.LowLevelDebugDump())
	}

	ia = info.Archives[2]
	if ia.MinTS != 400 {
		t.Errorf("wrong archive MinTS: %v, expected 400", ia.MinTS)
		t.Log("dump ", r.LowLevelDebugDump())
		t.Logf("info %#v", ia)
	}

	if info.ColumnsCount != len(c) {
		t.Errorf("wrong number of columns: %d, expected %d", info.ColumnsCount, len(c))
		t.Logf("dump: %s", r.LowLevelDebugDump())
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
			for _, err := range checkValue(values[0], float32(100.0), int64(10), true, 0, 0) {
				t.Error(err)
				t.Logf("dump: %s", r.LowLevelDebugDump())
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

	for i := 0; i < 600; i++ {
		if err := r.Put(int64(i), 0, float32(i)); err != nil {
			t.Errorf("Put error: %s", err.Error())
		}
	}

	if last, err := r.Last(); err == nil {
		if last != 599 {
			t.Errorf("Last value wrong val, last=%d, expected 590", last)
			t.Logf("dump: %s", r.LowLevelDebugDump())
		}
	} else {
		t.Errorf("Last get error: %s", err.Error())
	}

	// check archive 0 - 10 rows step 1
	// this shouldn't be found in archive 0
	for i := 0; i < 581; i++ {
		if values, err := r.getFromArchive(0, int64(i), []int{0}); len(values) > 0 {
			t.Errorf("found value in archive 0 that shouldn't exists: %d, %#v, %s", i, values, err)
			t.Logf("dump: %s", r.LowLevelDebugDump())
		}
	}
	for i := 590; i < 600; i++ {
		if values, err := r.getFromArchive(0, int64(i), []int{0}); err != nil {
			t.Errorf("Get error: %s", err.Error())
			t.Logf("dump: %s", r.LowLevelDebugDump())
		} else {
			if len(values) != 1 {
				t.Errorf("Get error: wrong number of values %#v", values)
			} else {
				val := values[0]

				for _, err := range checkValue(val, float32(i), int64(i), true, 0, 0) {
					t.Error(err)
					t.Logf("dump: %s", r.LowLevelDebugDump())
				}
			}
		}
	}

	// check archive 1 - 10 rows, step 10
	// this shouldn't be found in archive 0
	for i := 0; i < 500; i++ {
		if values, err := r.getFromArchive(1, int64(i), []int{0}); len(values) > 0 {
			t.Errorf("found value in archive 1 that shouldn't exists: %d, %#v, %s", i, values, err)
			t.Logf("dump: %s", r.LowLevelDebugDump())
		}
	}
	for i := 500; i < 600; i = i + 10 {
		if values, err := r.getFromArchive(1, int64(i), []int{0}); err != nil {
			t.Errorf("Get error: %s", err.Error())
			t.Logf("dump: %s", r.LowLevelDebugDump())
		} else {
			if len(values) != 1 {
				t.Errorf("Get error: wrong number of values for %d: %#v", i, values)
				t.Logf("dump: %s", r.LowLevelDebugDump())
			} else {
				val := values[0]
				exp := i + 9
				for _, err := range checkValue(val, float32(exp), int64(i), true, -1, -1) {
					t.Error(err)
					t.Logf("dump: %s", r.LowLevelDebugDump())
				}
			}
		}
	}

	// check archive 2 - 10 rows, step 100
	// this shouldn't be found in archive 0
	for i := 0; i < 600; i = i + 100 {
		if values, err := r.getFromArchive(2, int64(i), []int{0}); err != nil {
			t.Errorf("Get error: %s", err.Error())
			t.Logf("dump: %s", r.LowLevelDebugDump())
		} else {
			if len(values) != 1 {
				t.Errorf("Get error: wrong number of values for %d: %#v", i, values)
				t.Logf("dump: %s", r.LowLevelDebugDump())
			} else {
				val := values[0]
				exp := i + 99
				for _, err := range checkValue(val, float32(exp), int64(i), true, -1, -1) {
					t.Error(err)
					t.Logf("dump: %s", r.LowLevelDebugDump())
				}
			}
		}
	}

}

func TestPutDataRR2(t *testing.T) {
	r, _, _ := createTestDB(t)
	defer closeTestDb(t, r)
	// update value

	for i := 0; i < 15; i++ {
		if err := r.Put(int64(i), 0, float32(i)); err != nil {
			t.Errorf("Put error: %s", err.Error())
		}
	}

	if last, err := r.Last(); err == nil {
		if last != 14 {
			t.Errorf("Last value wrong val, last=%d, expected 14", last)
			t.Logf("dump: %s", r.LowLevelDebugDump())
		}
	} else {
		t.Errorf("Last get error: %s", err.Error())
	}

	// check archive 0 - 10 rows step 1
	// this shouldn't be found in archive 0
	for i := 0; i < 5; i++ {
		if values, err := r.getFromArchive(0, int64(i), []int{0}); len(values) > 0 {
			t.Errorf("found value in archive 0 that shouldn't exists: %d, %#v, %s", i, values, err)
			t.Logf("dump: %s", r.LowLevelDebugDump())
		}
	}
	for i := 5; i < 15; i++ {
		if values, err := r.getFromArchive(0, int64(i), []int{0}); err != nil {
			t.Errorf("Get error: %s", err.Error())
			t.Logf("dump: %s", r.LowLevelDebugDump())
		} else {
			if len(values) != 1 {
				t.Errorf("Get error: wrong number of values %#v", values)
			} else {
				val := values[0]

				for _, err := range checkValue(val, float32(i), int64(i), true, 0, 0) {
					t.Error(err)
					t.Logf("dump: %s", r.LowLevelDebugDump())
				}
			}
		}
	}

	if vls, err := r.GetRange(5, -1, []int{0}, false, false); err != nil {
		t.Errorf("GetRange error: %s", err.Error())
	} else {
		if len(vls) != 10 {
			t.Errorf("wrong result len: %v", vls)
			t.Log("dump ", r.LowLevelDebugDump())
		}
		for i := 0; i < 10; i++ {
			v := vls[i].Values[0]
			for _, err := range checkValue(v, float32(i+5), int64(i+5), true, 0, 0) {
				t.Error(err)
				t.Logf("dump: %s", r.LowLevelDebugDump())
				t.Logf("res: %+v", vls)
				return
			}
		}
	}
}

func TestPutDataRR3(t *testing.T) {
	r, _, _ := createTestDB(t)
	defer closeTestDb(t, r)

	testV := []int{
		1,  // row 1 - replaced by 11
		3,  // row 3 - replaced by 13
		5,  // row 5
		10, // row 0 - replaced by 20
		11, // row 1
		12, // row 2
		13, // row 3
		14, // row 4 - replaced by 54
		20, // row 0
		38, // row 8
		54, // row 4
		8,  // row 9 but skipped
		44, // row 4 but skipped
	}

	missing := map[int]bool{
		1:  true,
		3:  true,
		10: true,
		14: true,
		8:  true,
		44: true,
	}

	for _, v := range testV {
		r.Put(int64(v), 0, float32(v))
	}

	for _, v := range testV {
		if values, err := r.getFromArchive(0, int64(v), []int{0}); err != nil {
			t.Errorf("Get error: %s", err.Error())
			t.Logf("dump: %s", r.LowLevelDebugDump())
		} else {
			if values == nil || len(values) != 1 {
				if _, ok := missing[v]; !ok {
					t.Errorf("Get error: wrong number of values for %v - %#v", v, values)
					t.Logf("dump: %s", r.LowLevelDebugDump())
				}
			} else {
				if _, ok := missing[v]; ok {
					t.Errorf("Get error: values for %v shouldn't exist -  %#v", v, values)
					t.Logf("dump: %s", r.LowLevelDebugDump())
					continue
				}
				for _, err := range checkValue(values[0], float32(v), int64(v), true, 0, 0) {
					t.Error(err)
					t.Logf("dump: %s", r.LowLevelDebugDump())
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

func TestPutDataRR5(t *testing.T) {
	r, _, _ := createTestDB(t)
	defer closeTestDb(t, r)

	// few values

	inp := []int{1, 2, 5, 7, 10, 12, 14}

	for _, i := range inp {
		if err := r.Put(int64(i), 0, float32(i)); err != nil {
			t.Errorf("Put error: %s", err.Error())
		}
	}

	exp := []int{5, 7, 10, 12, 14}
	for _, i := range exp {
		if values, err := r.getFromArchive(0, int64(i), []int{0}); err != nil {
			t.Errorf("Get error: %s", err.Error())
			t.Logf("dump: %s", r.LowLevelDebugDump())
		} else {
			if len(values) != 1 {
				t.Errorf("Get error: wrong number of values %#v", values)
			} else {
				val := values[0]
				for _, err := range checkValue(val, float32(i), int64(i), true, 0, 0) {
					t.Error(err)
					t.Logf("dump: %s", r.LowLevelDebugDump())
					return
				}
			}
		}
	}

	if vls, err := r.GetRange(5, -1, []int{0}, false, false); err != nil {
		t.Errorf("GetRange error: %s", err.Error())
	} else {
		if len(vls) != len(exp) {
			t.Errorf("wrong result len: %v", vls)
			t.Log("dump ", r.LowLevelDebugDump())
		}
		for i, e := range exp {
			v := vls[i].Values[0]
			for _, err := range checkValue(v, float32(e), int64(e), true, 0, 0) {
				t.Error(err)
				t.Logf("dump: %s", r.LowLevelDebugDump())
				t.Logf("res: %+v", vls)
				return
			}
		}
	}
}

func TestPutDataFuncs(t *testing.T) {
	// Test agregations
	r, _, _ := createTestDB(t)
	defer closeTestDb(t, r)

	testV := [][]int{
		{1, 1}, {1, 3}, {1, 0}, {1, 4},
		{2, 2}, {2, 6},
		{5, 5}, {5, 1},
	}

	for _, v := range testV {
		for i := 0; i < 6; i++ {
			if err := r.Put(int64(v[0]), i, float32(v[1])); err != nil {
				t.Errorf("Put error: %s", err.Error())
				return
			}
		}
	}

	expected := [][]int{
		{1, 4, 2, 8, 0, 4, 4},
		{2, 6, 4, 8, 2, 6, 2},
		{5, 1, 3, 6, 1, 5, 2},
	}

	for row, e := range expected {
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
						t.Errorf("Get value - wrong value for col %d -  %v, expected %v in row %d", col, v, exp, row)
						t.Logf("dump: %s", r.LowLevelDebugDump())
					}
				}
			}
		}
	}
}

func TestRangeFindArchive(t *testing.T) {
	r, _, _ := createTestDB(t)
	defer closeTestDb(t, r)

	// sample data
	testV := []int{1, 5, 10, 20, 100, 150, 200, 250, 300, 400, 450, 490, 495, 500}

	if errors := putTestDataInts(r, testV, 0); len(errors) > 0 {
		t.Errorf("Put data error: %v", errors)
		return
	}

	last, _ := r.Last()

	if aID, min, max := r.findArchiveForRange(0, 8000, last); aID != 2 || min != 0 || max != 8000 {
		t.Errorf("wrong archive; expected 2: %d, min=%d, max=%d", aID, min, max)
	}

	if aID, min, max := r.findArchiveForRange(420, 500, last); aID != 1 || min != 420 || max != 500 {
		t.Errorf("wrong archive; expected 1: %d, min=%d, max=%d", aID, min, max)
	}

	if aID, min, max := r.findArchiveForRange(491, 500, last); aID != 0 || min != 491 || max != 500 {
		t.Errorf("wrong archive; expected 0: %d, min=%d, max=%d", aID, min, max)
	}
}

func TestRange(t *testing.T) {
	r, _, _ := createTestDB(t)
	defer closeTestDb(t, r)

	// sample data
	testV := []int{1, 5, 10, 20, 100, 150, 200, 250, 300, 400, 450, 490, 495, 500}
	if errors := putTestDataInts(r, testV, 0); len(errors) > 0 {
		t.Errorf("Put data error: %v", errors)
		return
	}

	// get all - should use arch "a2"
	if vls, err := r.GetRange(0, -1, []int{0}, false, false); err != nil {
		t.Errorf("GetRange error: %s", err.Error())
	} else {
		exp := [][]int{{0, 20}, {100, 150}, {200, 250}, {300, 300}, {400, 495}, {500, 500}}
		if len(vls) != len(exp) {
			t.Errorf("wrong result len: %v", vls)
			t.Log("dump ", r.LowLevelDebugDump())
		}
		for i, e := range exp {
			v := vls[i].Values[0]
			for _, err := range checkValue(v, float32(e[1]), int64(e[0]), true, 2, 0) {
				t.Error(err)
				t.Logf("dump: %s", r.LowLevelDebugDump())
			}
		}
	}

	// get last - should use arch "a0"
	if vls, err := r.GetRange(491, 500, []int{0}, false, false); err != nil {
		t.Errorf("GetRange error: %s", err.Error())
	} else {
		exp := [][]int{{495, 495}, {500, 500}}

		if len(vls) != len(exp) {
			t.Errorf("wrong result len: %v", vls)
			t.Log("dump ", r.LowLevelDebugDump())
		}
		for i, e := range exp {
			v := vls[i].Values[0]
			for _, err := range checkValue(v, float32(e[1]), int64(e[0]), true, 0, 0) {
				t.Error(err)
				t.Logf("dump: %s", r.LowLevelDebugDump())
			}
		}
	}

	// a0
	if vls, err := r.GetRange(491, -1, []int{0}, false, false); err != nil {
		t.Errorf("GetRange error: %s", err.Error())
	} else {
		exp := [][]int{{495, 495}, {500, 500}}
		if len(vls) != len(exp) {
			t.Errorf("wrong result len: %v", vls)
			t.Log("dump ", r.LowLevelDebugDump())
		}
		for i, e := range exp {
			v := vls[i].Values[0]
			for _, err := range checkValue(v, float32(e[1]), int64(e[0]), true, 0, 0) {
				t.Error(err)
				t.Logf("dump: %s", r.LowLevelDebugDump())
			}
		}
	}

	if vls, err := r.GetRange(100, 300, []int{0}, false, false); err != nil {
		t.Errorf("GetRange error: %s", err.Error())
	} else {
		exp := [][]int{{100, 150}, {200, 250}, {300, 300}}
		if len(vls) != len(exp) {
			t.Errorf("wrong result len: %v", vls)
			t.Log("dump ", r.LowLevelDebugDump())
		}
		for i, e := range exp {
			v := vls[i].Values[0]
			for _, err := range checkValue(v, float32(e[1]), int64(e[0]), true, 2, 0) {
				t.Error(err)
				t.Logf("dump: %s", r.LowLevelDebugDump())
			}
		}
	}
}

func TestRangeIncludeInvalid(t *testing.T) {
	r, _, _ := createTestDB(t)
	defer closeTestDb(t, r)

	// sample data
	testV := []int{1, 5, 10, 20, 100, 150, 200, 250, 300, 400, 450, 490, 495, 500}
	if errors := putTestDataInts(r, testV, 0); len(errors) > 0 {
		t.Errorf("Put data error: %v", errors)
		return
	}

	// get last - should use arch "a0"
	if vls, err := r.GetRange(491, 500, []int{0}, true, false); err != nil {
		t.Errorf("GetRange error: %s", err.Error())
	} else {
		exp := []int{491, 492, 493, 494, 495, 496, 497, 498, 499, 500}

		if len(vls) != len(exp) {
			t.Errorf("wrong result len: %v", vls)
			t.Log("dump ", r.LowLevelDebugDump())
		}
		for i, e := range exp {
			if vls[i].TS != int64(e) {
				t.Errorf("missing ts=%d\n", e)
				t.Logf("dump: %s", r.LowLevelDebugDump())
			}
			if e == 450 || e == 490 || e == 495 || e == 500 {
				v := vls[i].Values[0]
				for _, err := range checkValue(v, float32(e), int64(e), true, 0, 0) {
					t.Error(err)
					t.Logf("dump: %s", r.LowLevelDebugDump())
				}
			} else {
				if len(vls[i].Values) > 0 {
					for _, vv := range vls[i].Values {
						if vv.Valid {
							t.Errorf("found some values when not expected: %#v\n", vls)
							t.Logf("dump: %s", r.LowLevelDebugDump())
						}
					}
				}
			}
		}
	}

	if vls, err := r.GetRange(100, 350, []int{0}, true, false); err != nil {
		t.Errorf("GetRange error: %s", err.Error())
	} else {
		exp := [][]int{{100, 150}, {200, 250}, {300, 300}}
		if len(vls) != len(exp) {
			t.Errorf("wrong result len: %v", vls)
			t.Log("dump ", r.LowLevelDebugDump())
		}
		for i, e := range exp {
			v := vls[i].Values[0]
			for _, err := range checkValue(v, float32(e[1]), int64(e[0]), true, 2, 0) {
				t.Error(err)
				t.Logf("dump: %s", r.LowLevelDebugDump())
			}
		}
	}
}

func createTestDB(t *testing.T) (*RRD, []RRDColumn, []RRDArchive) {
	c := []RRDColumn{
		RRDColumn{Name: "col1", Function: FLast},
		RRDColumn{Name: "col2", Function: FAverage},
		RRDColumn{Name: "col3", Function: FSum},
		RRDColumn{Name: "col4", Function: FMinimum},
		RRDColumn{Name: "col5", Function: FMaximum},
		RRDColumn{Name: "col6", Function: FCount},
	}
	a := []RRDArchive{
		RRDArchive{Name: "a0", Step: 1, Rows: 10},
		RRDArchive{Name: "a1", Step: 10, Rows: 10},
		RRDArchive{Name: "a2", Step: 100, Rows: 10},
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
		t.Errorf("RRD close error: %s", err.Error())
	}
}

func checkValue(v Value, eValue float32, eTS int64, eValid bool, eArchive int, eColumn int) (errors []string) {
	if v.Value != eValue {
		errors = append(errors,
			fmt.Sprintf("wrong value: %v (expected %v) %v", v.Value, eValue, v))
	}
	if v.TS >= -1 && v.TS != eTS {
		errors = append(errors,
			fmt.Sprintf("wrong ts: %v (expected %d) in %v", v.TS, eTS, v))
	}
	if v.Valid != eValid {
		errors = append(errors,
			fmt.Sprintf("wrong valid: %v (expected %v) in %v", v.Valid, eValid, v))
	}
	if eArchive >= 0 && v.ArchiveID != eArchive {
		errors = append(errors,
			fmt.Sprintf("wrong archive: %v (expected %v) in %v", v.ArchiveID, eArchive, v))
	}
	if eColumn >= 0 && v.Column != eColumn {
		errors = append(errors,
			fmt.Sprintf("wrong column: %v (expected %v) in %v", v.Column, eColumn, v))
	}

	return errors
}

func putTestDataInts(r *RRD, values []int, cols ...int) (errors []string) {
	for _, v := range values {
		for _, col := range cols {
			if err := r.Put(int64(v), col, float32(v)); err != nil {
				errors = append(errors, fmt.Sprintf("on %v:%d -> %s", v, col, err.Error()))
			}
		}
	}
	return
}
