package sel

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/mitchellh/mapstructure"
	"github.com/snadrus/nodb/internal/base"
	"github.com/snadrus/nodb/internal/expr"
	"github.com/xwb1989/sqlparser"
)

// Obj conveys "table data" as Do's 3rd arg
type Obj map[string]interface{}

// Do SELECT
func Do(tree *sqlparser.Select, result interface{}, src Obj) error {
	ch := GetChan(tree, src)

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
			mapstructure.Decode(complex.item, v.Interface())
			rSlice.Set(reflect.Append(rSlice, reflect.Indirect(v)))
		}
	}
	//RT needs to point at new RE
	rt.Elem().Set(rSlice)
	return nil
}

// GetChanError is the GetChan return type
type GetChanError struct {
	item map[string]interface{}
	err  error
}

type condition expr.E

// GetChan for when you want a stream of results
func GetChan(tree *sqlparser.Select, src Obj) chan GetChanError {
	ch := make(chan GetChanError, 20)
	go func() {
		defer close(ch)
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
			outputTypes, aggOutputer, err := doSelect(tree.SelectExprs, selectBuilder)
			if err != nil {
				return fmt.Errorf("DoSelect error: %v", err)
			}

			plan, err := planQuery(outputTypes, joins, condition(WhereBuilder.Expr), sourceTables)
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

					plan.MakeGroupBy(groupByExprs, selectBuilder, havingBuilder, aggOutputer)
				} else {
					plan.MakeGroupBy(groupByExprs, selectBuilder, nil, aggOutputer)
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

					plan.MakeGroupBy(oneBigGroup, selectBuilder, nil, aggOutputer)
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

			if tree.Limit != nil || tree.Lock != "" || tree.Distinct != "" {
				return errors.New("No support for Limit, Lock, or Distinct")
			}

			selRemoveNamedItemsTable(sourceTables)

			plan.Run(ch)
			return nil
		}
		err := chReturnSimple()
		if err != nil {
			ch <- GetChanError{nil, err}
		}
	}()
	return ch
}
