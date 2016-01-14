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
	// Test upicates
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
	// Test upicates
	r, _, _ := createTestDB(t)
	defer closeTestDb(t, r)

	testV := []int{10, 10, 12, 13, 15, 20, 21, 22, 25, 30, 31}

	for _, v := range testV {
		if err := r.Put(int64(v), 0, float32(v)); err != nil {
			t.Errorf("Put error: %s", err.Error())
		}
		if err := r.Put(int64(v), 1, float32(v)); err != nil {
			t.Errorf("Put error: %s", err.Error())
		}
	}

	expected := [][]int{
		{10, 15, 12},
		{20, 25, 22},
		{30, 31, 31},
	}

	for _, e := range expected {
		// last
		if values, err := r.getFromArchive(0, int64(e[0]), []int{0, 1}); err != nil {
			t.Errorf("Get error: %s", err.Error())
			t.Logf("dump: %s", r.LowLevelDebugDump())
		} else {
			if len(values) != 2 {
				t.Errorf("Get value - not found %v", e)
				t.Logf("dump: %s", r.LowLevelDebugDump())
			} else {
				v := int(values[0].Value)
				if v != e[1] {
					t.Errorf("Get value - wrong value last -  %v, expected %v", v, e[1])
					t.Logf("dump: %s", r.LowLevelDebugDump())
				}
				v = int(values[1].Value)
				if v != e[2] {
					t.Errorf("Get value - wrong value avg -  %v, expected %v", v, e[2])
					t.Logf("dump: %s", r.LowLevelDebugDump())
				}
			}
		}
	}
}

func createTestDB(t *testing.T) (*RRD, []RRDColumn, []RRDArchive) {
	c := []RRDColumn{
		RRDColumn{"col1", FLast},
		RRDColumn{"col2", FAverage},
	}
	a := []RRDArchive{
		RRDArchive{"a1", 10, 60},
		RRDArchive{"a2", 60, 300},
		RRDArchive{"a3", 300, 1800},
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
