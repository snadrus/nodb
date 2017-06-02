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

// Obj conveys "table data" as Do's 3rd arg
type Obj map[string]interface{}

type Rows struct {
	colNamesCh    chan []string
	colNamesCache []string
	ch            chan GetChanError
	cancel        context.CancelFunc
}

func DoAry(tree sqlparser.SelectStatement, src Obj, ctx context.Context) (driver.Rows, error) {
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
	if chanError.err != nil {
		r.cancel()
		return chanError.err
	}
	if !ok {
		return io.EOF
	}
	for i, v := range chanError.item {
		dest[i] = v
	}
	return nil
}

// Do SELECT
func Do(tree sqlparser.SelectStatement, result interface{}, src Obj) error {
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
		base.Debug("got", complex.item)
		if complex.err != nil {
			base.Debug("got err", complex.err)
			return complex.err
		}
		v := reflect.New(unit)

		// TODO augment copier to support source & dest as map[string]interface{}
		// complex.item.(type) === map[string]interface{}
		// v.Interface().(type) is either the same (set equal) or struct

		switch v.Interface().(type) {
		case map[string]interface{}:
			rSlice.Set(reflect.Append(rSlice, reflect.ValueOf(complex.item)))
		default:
			tmp := make(map[string]interface{})
			if colNames == nil {
				colNames = <-chColNames
			}
			for i, v := range complex.item {
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

// GetChanError is the GetChan return type
type GetChanError struct {
	item []interface{}
	err  error
}

type condition expr.E

// GetChan for when you want a stream of results
func GetChan(selStmt sqlparser.SelectStatement, src Obj, ctx context.Context) (chOut chan GetChanError, colCh chan []string) {
	ch := make(chan GetChanError, 20)
	chColNames := make(chan []string, 1)
	var cancelCtx context.CancelFunc
	ctx, cancelCtx = context.WithCancel(ctx)
	switch u := selStmt.(type) {
	case *sqlparser.Union:
		if !(u.Type == sqlparser.AST_UNION || u.Type == sqlparser.AST_UNION_ALL) {
			ch <- GetChanError{nil, errors.New("Simple Union only, TODO")}
			return ch, chColNames
		}
		selStmt = u.Left
		ch2, _ := GetChan(u.Right, src, ctx)
		defer func() {
			chOut = make(chan GetChanError)
		}()
		go func() {
			// add to ch. IF error in either, cancel other
			select {
			case v := <-ch:
				if v.err != nil {
					cancelCtx()
				}
				chOut <- v
			case v := <-ch2:
				if v.err != nil {
					cancelCtx()
				}
				chOut <- v
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
			WhereBuilder := expr.DefaultBuilder.Dup().Setup(sourceTables, src)
			WhereBuilder.Obj = src // enable custom functions

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
				ch = make(chan GetChanError)
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
					var v GetChanError
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
			ch <- GetChanError{nil, err}
		}
		close(ch) //Lets CH redefined by Limit
	}()
	return ch, chColNames
}
