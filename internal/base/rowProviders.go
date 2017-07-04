package base

import "reflect"

type SliceOfStructRowProvider struct {
	t          reflect.Value
	length     int
	currentRow int
	started    bool // first .Next() and each-loop .Next() is called 1x too much
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
	return 2 // a bit arbitrary. Think of it as # of mem-accesses.
}

func (s *SliceOfStructRowProvider) SetConfig(multiPass bool) {}
func (s *SliceOfStructRowProvider) NextRow() (hasNotLooped bool) {
	if !s.started {
		s.started = true
		return true
	}
	s.currentRow = (s.currentRow + 1) % s.length
	if s.currentRow == 0 {
		s.started = false // because we double visit here.
	}
	return s.currentRow != 0
}
func (s *SliceOfStructRowProvider) GetFields(used map[string]bool, addPrefix string, dest map[string]interface{}) {
	myrow := s.t.Index(s.currentRow) // TODO interface this to allow other tables / locks
	for name := range used {         // copy my useful fields
		dest[addPrefix+name] = myrow.FieldByName(name).Interface()
	}
}

type ChanOfStructRowProvider struct {
	t              reflect.Value
	init           bool
	currentRow     int
	currentRowVal  reflect.Value // during channel walk its set by nextrow
	channelReading bool          // first .Next() and each-loop .Next() is called 1x too much
	fellOffEnd     bool
	Saved          []reflect.Value
}

func NewChanOfStructRP(chanOfStruct interface{}) RowProvider {
	v := reflect.ValueOf(chanOfStruct)
	return &ChanOfStructRowProvider{
		t:              v,
		currentRow:     0,
		channelReading: true,
	}
}

func (c *ChanOfStructRowProvider) GetInfo() (multiPassCost int) {
	return 10
}

func (c *ChanOfStructRowProvider) SetConfig(multiPass bool) {
	if multiPass {
		c.Saved = []reflect.Value{} // initialize it.
	}
}

// NextRow makes readable the next row BUT at the end
// it simply returns false. another read will setup the next cycle
func (c *ChanOfStructRowProvider) NextRow() (hasNotLooped bool) {
	if c.channelReading && !c.fellOffEnd {
		var ok bool
		if c.currentRowVal, ok = c.t.Recv(); ok {
			if c.Saved != nil {
				c.Saved = append(c.Saved, c.currentRowVal)
			}
		} else {
			c.fellOffEnd = true
			return false
		}
	} else {
		if c.fellOffEnd {
			c.fellOffEnd = false // we only want to report it once
			c.channelReading = false
			c.currentRow = 0
			return true
		}
		c.currentRow = (c.currentRow + 1) % len(c.Saved)
		if c.currentRow == 0 {
			c.fellOffEnd = true
			return false
		}
	}
	return true
}
func (c *ChanOfStructRowProvider) GetFields(used map[string]bool, addPrefix string, dest map[string]interface{}) {
	if !c.channelReading {
		c.currentRowVal = c.Saved[c.currentRow]
	}
	for name := range used { // copy my useful fields
		dest[addPrefix+name] = c.currentRowVal.FieldByName(name).Interface()
	}
}
