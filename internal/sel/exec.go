package sel

import (
	"context"
	"fmt"

	"github.com/kr/pretty"
	"github.com/snadrus/nodb/internal/base"
)

// a fake FROM table to get things going
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
func doNest(je *joinElement, ctx context.Context, cancelFunc CancelWithError) chainType {
	ch := make(chainType, 5)
	je.resultChan = ch
	go func() {
		defer close(ch)
		var prev chan row
		if je.from == nil {
			prev = getInitialRow()
		} else {
			prev = je.from.resultChan
			je.table.Table.SetConfig(true)
		}
		if je.condition == nil {
			je.condition = goodCondition
		}

		tname := je.table.Name
		joined := false
		base.Debug("DONEST for ", pretty.Sprint(tname))
		// TODO PERF apply flat conditions
		for m := range prev {
			// Handle Full Join
			joined = false
			for je.table.Table.NextRow() { // for every row in my table
				myMap := rowDup(m)

				err := je.table.Table.GetFields(je.table.UsedFields, tname+".", myMap)
				if err != nil {
					cancelFunc(err)
				}

				r, err := je.condition(myMap)
				if err != nil {
					cancelFunc(fmt.Errorf("JOIN Error, %s", err.Error()))
				}
				if r.(bool) {
					base.Debug("JOIN Condition true for ", myMap)
					select {
					case ch <- myMap:
					case <-ctx.Done():
						return
					}
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
				select {
				case ch <- myMap:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return ch
}
