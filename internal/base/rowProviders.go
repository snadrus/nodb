package base

import (
	"reflect"
)

type SliceOfStructRowProvider struct {
	t          reflect.Value
	length     int
	currentRow int
	init       bool // first .Next() and each-loop .Next() is called 1x too much
}

func NewSliceOfStructRP(sliceOfStruct interface{}) RowProvider {
	v := reflect.ValueOf(sliceOfStruct)
	return &SliceOfStructRowProvider{
		t:          v,
		length:     v.Len(),
		currentRow: 0,
	}
}

func (s *SliceOfStructRowProvider) GetInfo() (multiPassCost int) {
	return 0
}

func (s *SliceOfStructRowProvider) SetConfig(multiPass bool) {}
func (s *SliceOfStructRowProvider) NextRow() (hasNotLooped bool) {
	if !s.init {
		s.init = true
		return true
	}
	s.currentRow = (s.currentRow + 1) % s.length
	if s.currentRow == 0 {
		s.init = false
	}
	return s.currentRow != 0
}
func (s *SliceOfStructRowProvider) GetFields(used map[string]bool, addPrefix string, dest map[string]interface{}) {
	myrow := s.t.Index(s.currentRow) // TODO interface this to allow other tables / locks
	for name := range used {         // copy my useful fields
		dest[addPrefix+name] = myrow.FieldByName(name).Interface()
	}
}
