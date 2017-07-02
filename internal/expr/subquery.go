package expr

import (
	"context"
	"errors"
	"sync"

	"github.com/snadrus/nodb/internal/base"
	"github.com/xwb1989/sqlparser"
)

type SubqueryRunner func(selStmt sqlparser.SelectStatement, src base.Obj, ctx context.Context) (chOut chan base.GetChanError, colCh chan []string)

func (e *ExpressionBuilder) SubqueryToList(t *sqlparser.Subquery) (E, error) {
	if e.SubqueryRunner == nil {
		return nil, errors.New("Impl error: ExpressionBuilder lacks SubqueryRunner")
	}
	done := sync.Mutex{}
	var mutexedListResult []interface{}
	var mutexedErr error
	done.Lock()
	go func() {
		defer done.Unlock()
		ctx := context.Background()
		chRows, colCh := e.SubqueryRunner(t.Select, e.Obj, ctx)
		go func() {
			_ = <-colCh
		}()
		for row := range chRows {
			if row.Err != nil {
				mutexedErr = row.Err
				return
			}
			mutexedListResult = append(mutexedListResult, row.Item[0])
		}
	}()
	return func(map[string]interface{}) (interface{}, error) {
		done.Lock() // This ensures the writing has stopped
		defer done.Unlock()
		return mutexedListResult, mutexedErr
	}, nil
}
