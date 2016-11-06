package sel

import (
	"reflect"

	"github.com/kr/pretty"
	"github.com/snadrus/nodb/internal/base"
)

func getInitialRow() (chain chan row) {
	chain = make(chan row, 1)
	chain <- make(row) // Start the nested for loops
	close(chain)
	return chain
}

func goodCondition(fake map[string]interface{}) (interface{}, error) {
	return true, nil
}

func rowDup(m row) row {
	myMap := make(row)
	for k, v := range m { // duplicate the left side into an out-row
		myMap[k] = v
	}
	return myMap
}
func doNest(je *joinElement) chainType {
	ch := make(chainType, 5)
	je.resultChan = ch
	go func() {
		defer close(ch)
		var prev chan row
		if je.from == nil {
			prev = getInitialRow()
		} else {
			prev = je.from.resultChan
		}
		if je.condition == nil {
			je.condition = goodCondition
		}
		t := reflect.ValueOf(je.table.Table)
		length := t.Len()

		tname := je.table.Name
		joined := false
		base.Debug("DONEST for ", pretty.Sprint(je.table.Table))
		// TODO PERF apply flat conditions
		for m := range prev {
			// TODO Accuracy Handle Full Join
			joined = false
			for i := 0; i < length; i++ { // for every row in my table
				myMap := rowDup(m)
				myrow := t.Index(i)
				for name := range je.table.UsedFields { // copy my useful fields
					myMap[tname+"."+name] = myrow.FieldByName(name).Interface()
				}
				r, err := je.condition(myMap)
				if err != nil {
					base.Debug("JOIN Error. Should have return path!")
				}
				if r.(bool) {
					base.Debug("JOIN Condition true for ", myMap)
					ch <- myMap
					joined = true
				}
			}
			if !joined && je.fullOther { // Left join
				myMap := rowDup(m)
				base.Debug("map before nulling:", myMap, "used fields:", je.table.UsedFields)
				for name := range je.table.UsedFields {
					myMap[tname+"."+name] = nil
				}
				base.Debug("Left Join row detected for", myMap)
				ch <- myMap
			}
		}
	}()
	return ch
}
