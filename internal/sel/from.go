package sel

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/fatih/structs"
	"github.com/snadrus/nodb/internal/base"
	"github.com/snadrus/nodb/internal/expr"
	"github.com/xwb1989/sqlparser"
)

type from struct {
	src          base.SrcTables
	joinElements []*joinElement
	exprBuilder  *expr.ExpressionBuilder
	obj          Obj
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
			if kind == reflect.Slice && vo.Index(0).Kind() == reflect.Struct {
				mySrcTable.Table = base.NewSliceOfStructRP(tdata)
			} else if kind == reflect.Struct {
				s := reflect.MakeSlice(reflect.SliceOf(vo.Type()), 1, 1)
				s.Index(0).Set(vo)
				mySrcTable.Table = base.NewSliceOfStructRP(s.Interface())
			} else { // TODO support []map[string]interface{} / chan
				return nil, fmt.Errorf("unsupported type for table", string(name))
			}

			// TODO MAKE SAFER FOR NULLS
			for _, f := range structs.Fields(vo.Index(0).Interface()) {
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
			// TODO run it & get back a channel  / DoGetChan()....
			return nil, fmt.Errorf("Subquery not implemented. TODO!!")
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

func fromer(exprs sqlparser.TableExprs, obj Obj) (base.SrcTables, []*joinElement, error) {
	myFrom := from{
		src: base.SrcTables{},
		obj: obj,
	}
	myFrom.exprBuilder = expr.DefaultBuilder.Dup().Setup(myFrom.src, obj)
	return myFrom.src, myFrom.joinElements, myFrom.Do(exprs)
}
