package sel

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/fatih/structs"
	"github.com/kr/pretty"
	"github.com/snadrus/nodb/internal/base"
	"github.com/snadrus/nodb/internal/expr"
	"github.com/xwb1989/sqlparser"
)

type from struct {
	src          base.SrcTables
	joinElements []*joinElement
	exprBuilder  *expr.ExpressionBuilder
	obj          base.Obj
}
type joinElement struct {
	from       *joinElement // left side, or NULL if that would be us.
	condition  expr.E
	table      *base.SrcTable
	fullOther  bool // Do you want all their rows?
	resultChan chan row
}

func (f *from) MakeJoinTree(je *sqlparser.JoinTableExpr) (*joinElement, error) {
	// Recurse, collecting names, conditions
	if je.Join == sqlparser.AST_RIGHT_JOIN { // Swap
		je.LeftExpr, je.RightExpr = je.RightExpr, je.LeftExpr
		je.Join = sqlparser.AST_LEFT_JOIN
	}
	left, err := f.MakeJoinElement(je.LeftExpr) // ok for joinElement
	if err != nil {
		return nil, err
	}
	right, err := f.MakeJoinElement(je.RightExpr)
	if err != nil {
		return nil, err
	}
	// TODO the ON clause should only map to these 2 tables (alias) & should be available
	cnd, err := f.exprBuilder.ExprToE(je.On)
	if err != nil {
		return nil, err
	}
	right.from = left
	right.condition = cnd
	switch je.Join {
	case sqlparser.AST_NATURAL_JOIN:
		return nil, errors.New("Natural Join not supported")
	case sqlparser.AST_CROSS_JOIN:
		return nil, errors.New("Cross Join not supported")
	case sqlparser.AST_STRAIGHT_JOIN:
		return nil, errors.New("Straight Join not supported.")
	case sqlparser.AST_LEFT_JOIN:
		right.fullOther = true
	case sqlparser.AST_JOIN:
	}
	return right, nil
}

/*
Normal 3-way join:  a join b left join c
   T
	1 T
	  23

Paren 3-way join: a join (b left join c)
   T
	1 (T)
    2 3   Still added, but T23 (#2) needs no .From AND T23 result
*/

func (f *from) MakeJoinElement(table sqlparser.TableExpr) (*joinElement, error) {
	switch table.(type) {
	case *sqlparser.AliasedTableExpr:
		aliasedTable := table.(*sqlparser.AliasedTableExpr)
		switch aliasedTable.Expr.(type) {
		case *sqlparser.TableName:
			tn := aliasedTable.Expr.(*sqlparser.TableName) // already lowercased
			name := string(tn.Name)
			// TODO LATER consume tn.Qualifier
			tdata, ok := f.obj[name] // Get table from object

			if !ok {
				for ttmpName := range f.obj { /// work around auto-lowercase of tablename
					if strings.ToLower(ttmpName) == name {
						ok = true
						tdata = f.obj[ttmpName]
					}
				}
				if !ok {
					return nil, fmt.Errorf("missing table %s", tn.Name)
				}
			}
			if len(aliasedTable.As) != 0 {
				name = string(aliasedTable.As)
			}

			mySrcTable := base.SrcTable{
				//Table:      tdata,
				Name:       name,
				UsedFields: map[string]bool{},
			}

			vo := reflect.ValueOf(tdata)
			kind := vo.Kind()
			var structForFieldWalking interface{}
			if kind == reflect.Slice && vo.Type().Elem().Kind() == reflect.Struct {
				mySrcTable.Table = base.NewSliceOfStructRP(tdata)
				structForFieldWalking = reflect.New(vo.Type().Elem()).Interface()
			} else if kind == reflect.Struct {
				s := reflect.MakeSlice(reflect.SliceOf(vo.Type()), 1, 1)
				s.Index(0).Set(vo)
				mySrcTable.Table = base.NewSliceOfStructRP(s.Interface())
				structForFieldWalking = tdata
			} else if kind == reflect.Chan {
				if reflect.TypeOf(tdata).Elem().Kind() != reflect.Struct {
					return nil, fmt.Errorf("Channel tables can only be of structs, TODO fixme")
				}
				mySrcTable.Table = base.NewChanOfStructRP(tdata)
				structForFieldWalking = reflect.New(vo.Type().Elem()).Interface()
			} else { // TODO support []map[string]interface{}
				return nil, fmt.Errorf("unsupported type for table %s", string(name))
			}

			// TODO MAKE SAFER FOR NULLS
			for _, f := range structs.Fields(structForFieldWalking) {
				if f.IsExported() {
					mySrcTable.Fields = append(mySrcTable.Fields, f.Name())
				} else {
					mySrcTable.HasPrivateFields = true
				}
			}

			f.src[mySrcTable.Name] = &mySrcTable

			j := &joinElement{
				table:     &mySrcTable,
				from:      nil,
				condition: nil,
			}
			f.joinElements = append(f.joinElements, j)
			return j, nil
		case *sqlparser.Subquery:
			sub := aliasedTable.Expr.(*sqlparser.Subquery)
			chOut, chCol := GetChan(sub.Select, f.obj, context.Background())
			// Determine struct shape
			fields := []reflect.StructField{}
			fieldNames := <-chCol
			for _, v := range fieldNames {
				fields = append(fields, reflect.StructField{Name: v, Type: reflect.TypeOf([]interface{}{}).Elem()})
			}
			symChan := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, reflect.StructOf(fields)), 0)
			rp := base.NewChanOfStructRP(symChan.Interface())
			go func() {
				defer symChan.Close()
				// live-convert structs for the row-provider.
				for resMap := range chOut {
					if resMap.Err != nil {
						rp.(base.CanSetError).SetError(resMap.Err)
						return // TODO how to pass error up?
					}
					s := reflect.New(reflect.StructOf(fields)).Elem()
					for i, v := range resMap.Item {
						s.FieldByIndex([]int{i}).Set(reflect.ValueOf(v))
					}
					base.Debug("subquery FROM sending ", pretty.Sprint(s))
					symChan.Send(s)
				}
			}()
			t := &base.SrcTable{
				Table:      rp,
				Name:       string(aliasedTable.As),
				UsedFields: map[string]bool{},
				Fields:     fieldNames,
			}
			f.src[t.Name] = t
			j := &joinElement{table: t}
			f.joinElements = append(f.joinElements, j)
			return j, nil
		}

	case *sqlparser.ParenTableExpr:
		//err := f.Do([]sqlparser.TableExpr{table.(*sqlparser.ParenTableExpr).Expr})
		//if err != nil {
		//	return err
		//}

		// Mixtures of left & right & regular joins with parens are tricky
		//This needs multiple initial rows
		return nil, errors.New("Parentheses in JOIN clause not supported. TODO")
	case *sqlparser.JoinTableExpr:
		return f.MakeJoinTree(table.(*sqlparser.JoinTableExpr))
	}
	return nil, nil
}

// TODO Later: accept "table0 sorted .b.c desc" to help the planner
func (f *from) Do(t []sqlparser.TableExpr) error {
	if len(t) != 1 {
		return errors.New("cannot implicit cross-join yet")
	}
	var err error
	//for _, table := range t { // []TableExpr
	// consecutive entries should be cross-joined (per spec)
	// items nested under join should be
	_, err = f.MakeJoinElement(t[0])
	if err != nil {
		return err
	}
	//}
	return nil
}

func fromer(exprs sqlparser.TableExprs, obj base.Obj) (base.SrcTables, []*joinElement, error) {
	myFrom := from{
		src: base.SrcTables{},
		obj: obj,
	}
	myFrom.exprBuilder = expr.DefaultBuilder.Dup().Setup(myFrom.src, obj, GetChan)
	return myFrom.src, myFrom.joinElements, myFrom.Do(exprs)
}
