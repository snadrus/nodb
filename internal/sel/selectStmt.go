package sel

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/mitchellh/mapstructure"
	"github.com/snadrus/nodb/internal/base"
	"github.com/snadrus/nodb/internal/expr"
	"github.com/xwb1989/sqlparser"
)

type Rows struct {
	colNamesCh    chan []string
	colNamesCache []string
	ch            chan base.GetChanError
	cancel        context.CancelFunc
}

func DoAry(tree sqlparser.SelectStatement, src base.Obj, ctx context.Context) (driver.Rows, error) {
	ctx, cancel := context.WithCancel(ctx)
	ch, colNamesCh := GetChan(tree, src, ctx)
	return &Rows{
		colNamesCh: colNamesCh,
		ch:         ch,
		cancel:     cancel,
	}, nil
}

func (r *Rows) Columns() []string {
	if r.colNamesCache == nil {
		base.Debug("colNamesCache getting populated")
		r.colNamesCache = <-r.colNamesCh
		base.Debug("colNamesCache populated to ", r.colNamesCache, r.colNamesCache == nil)
	}
	return r.colNamesCache
}

func (r *Rows) Close() error {
	r.cancel()
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	chanError, ok := <-r.ch
	if chanError.Err != nil {
		r.cancel()
		return chanError.Err
	}
	if !ok {
		return io.EOF
	}
	for i, v := range chanError.Item {
		dest[i] = v
	}
	return nil
}

// Do SELECT
func Do(tree sqlparser.SelectStatement, result interface{}, src base.Obj) error {
	ch, chColNames := GetChan(tree, src, context.Background())
	var colNames []string
	rt := reflect.ValueOf(result)
	if rt.Kind() != reflect.Ptr {
		return fmt.Errorf("Result must be a pointer. TODO: allow single rows")
	}
	rSlice := rt.Elem()
	if rSlice.Kind() != reflect.Slice {
		return fmt.Errorf("Result must be a pointer to a slice. TODO: allow single rows")
	}
	unit := rSlice.Type().Elem()
	for complex := range ch {
		base.Debug("got", complex.Item)
		if complex.Err != nil {
			base.Debug("got err", complex.Err)
			return complex.Err
		}
		v := reflect.New(unit)

		// TODO augment copier to support source & dest as map[string]interface{}
		// complex.item.(type) === map[string]interface{}
		// v.Interface().(type) is either the same (set equal) or struct

		switch v.Interface().(type) {
		case map[string]interface{}:
			rSlice.Set(reflect.Append(rSlice, reflect.ValueOf(complex.Item)))
		default:
			tmp := make(map[string]interface{})
			if colNames == nil {
				colNames = <-chColNames
			}
			for i, v := range complex.Item {
				tmp[colNames[i]] = v
			}
			mapstructure.Decode(tmp, v.Interface())
			rSlice.Set(reflect.Append(rSlice, reflect.Indirect(v)))
		}
	}
	//RT needs to point at new RE
	rt.Elem().Set(rSlice)
	return nil
}

type condition expr.E

type SubqueryRunnerImpl struct {
}

// GetChan for when you want a stream of results
func GetChan(selStmt sqlparser.SelectStatement, src base.Obj, ctx context.Context) (chOut chan base.GetChanError, colCh chan []string) {
	ch := make(chan base.GetChanError, 20)
	chColNames := make(chan []string, 1)
	var cancelCtx context.CancelFunc
	ctx, cancelCtx = context.WithCancel(ctx)
	switch u := selStmt.(type) {
	case *sqlparser.Union:
		if !(u.Type == sqlparser.AST_UNION || u.Type == sqlparser.AST_UNION_ALL) {
			ch <- base.GetChanError{nil, errors.New("Simple Union only, TODO")}
			return ch, chColNames
		}
		selStmt = u.Left
		ch2, _ := GetChan(u.Right, src, ctx)
		defer func() {
			chOut = make(chan base.GetChanError) //ipso-facto replace
		}()
		go func() {
			defer close(chOut)
			// add to ch. IF error in either, cancel other
			drain := func(src chan base.GetChanError) {
				for v := range src {
					if v.Err == nil {
						chOut <- v
					}
				}
				return
			}
			for {
				select {
				case v, ok := <-ch: // Read Left
					if !ok {
						drain(ch2)
						return
					}
					if v.Err != nil {
						cancelCtx()
					}
					chOut <- v
				case v, ok := <-ch2: // Read Right
					if !ok {
						drain(ch)
						return
					}
					if v.Err != nil {
						cancelCtx()
					}
					chOut <- v
				}
			}
		}()
	}
	tree := selStmt.(*sqlparser.Select)

	go func() {
		chReturnSimple := func() error {

			sourceTables, joins, err := fromer(tree.From, src)
			if err != nil {
				return err
			}
			WhereBuilder := expr.DefaultBuilder.Dup().Setup(sourceTables, src, GetChan)

			if tree.Where != nil {
				WhereBuilder.Expr, err = WhereBuilder.MakeBool(tree.Where.Expr)
				if err != nil {
					return err
				}
			}

			selectBuilder := WhereBuilder.Dup()
			selectBuilder.AllowAggregates()

			outputTypes, aggOutputer, colNames, err := doSelect(tree.SelectExprs, selectBuilder)
			if err != nil {
				return fmt.Errorf("DoSelect error: %v", err)
			}
			chColNames <- colNames

			plan, err := planQuery(outputTypes, joins, condition(WhereBuilder.Expr), sourceTables, ctx)
			if err != nil {
				return fmt.Errorf("Plan err: %v", err)
			}

			if tree.GroupBy != nil {
				groupByExprs, err := WhereBuilder.MakeSlice(tree.GroupBy)
				if err != nil {
					return err
				}
				if tree.Having != nil {
					havingBuilder := WhereBuilder.Dup()
					havingBuilder.AllowAggregates()
					havingBuilder.SrcTables = base.SrcTables{"1Select": selectBuilder.SrcTables["1Select"]}
					havingBuilder.Expr, err = havingBuilder.MakeBool(tree.Having.Expr)
					if err != nil {
						return fmt.Errorf("HAVING expression error: %s", err.Error())
					}

					plan.MakeGroupBy(groupByExprs, selectBuilder, havingBuilder, aggOutputer, ctx)
				} else {
					plan.MakeGroupBy(groupByExprs, selectBuilder, nil, aggOutputer, ctx)
				}
				// TODO FUTURE index on fields of interest & traverse in that order.
			} else {
				if tree.Having != nil {
					return errors.New("GROUPBY needed for HAVING")
				}
				if 0 != len(*selectBuilder.AggProcessing) {
					oneBigGroup := func(row map[string]interface{}) (interface{}, error) {
						return []interface{}{}, nil
					}

					plan.MakeGroupBy(oneBigGroup, selectBuilder, nil, aggOutputer, ctx)
				}
			}

			if tree.OrderBy != nil { // "Where" cannot access 1Select, "OrderBy" must
				base.Debug("available tables:", WhereBuilder.SrcTables)
				so, err := makeSortable(tree.OrderBy, WhereBuilder)
				if err != nil {
					return fmt.Errorf("OrderBy parse: %s", err.Error())
				}
				plan.MakeOrderBy(so)
			}

			if tree.Limit != nil {
				out := ch
				ch = make(chan base.GetChanError)
				offsetI, rowCtI, err := tree.Limit.Limits()
				if err != nil {
					return err
				}
				go func() {
					defer close(out)
					done := ctx.Done()
					if offsetI != nil {
						for a := 0; a < offsetI.(int); a++ {
							select {
							case <-ch:
							case <-done:
								return
							}
						}
					}
					var v base.GetChanError
					var ok bool
					if rowCtI != nil {
						for a := int64(0); a < rowCtI.(int64); a++ {
							select {
							case <-done:
								return
							case v, ok = <-ch: // "out" closed automatically
								if !ok {
									return
								}
								out <- v
							}
						}
					}
					cancelCtx()
				}()
			}
			if tree.Lock != "" || tree.Distinct != "" {
				return errors.New("No support for Limit, Lock, or Distinct")
			}

			selRemoveNamedItemsTable(sourceTables)

			plan.Run(ch)
			return nil
		}
		err := chReturnSimple() // easier err handling
		if err != nil {
			ch <- base.GetChanError{nil, err}
		}
		close(ch) //Lets CH redefined by Limit
	}()
	return ch, chColNames
}
