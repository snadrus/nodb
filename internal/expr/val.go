package expr

import (
	"fmt"
	"strconv"

	"github.com/snadrus/nodb/internal/base"
	"github.com/xwb1989/sqlparser"
)

// MakeVal renders an SQL value into a function-consumable thing
func (e *ExpressionBuilder) MakeVal(tree sqlparser.ValExpr) (E, error) {
	switch tree.(type) {
	case sqlparser.StrVal: // string constant
		v := tree.(sqlparser.StrVal)
		return retval(string(v), nil), nil
	case sqlparser.NumVal:
		str := string(tree.(sqlparser.NumVal))
		f64, _ := strconv.ParseFloat(str, 64)
		iSys, _ := strconv.Atoi(str)
		if float64(iSys) == f64 {
			return retval(iSys, nil), nil
		}
		return retval(f64, nil), nil
	case sqlparser.ValArg: // "?" solves SQL injection (~ok) & query plan reuse (useless).
		return nil, fmt.Errorf("ValArg not impl, TODO")
	case *sqlparser.NullVal:
		return retval(nil, nil), nil
	case *sqlparser.ColName:
		c := tree.(*sqlparser.ColName)
		n := string(c.Name)
		if len(c.Qualifier) > 0 {
			n = string(c.Qualifier) + "." + n
		}
		v, err := e.SrcTables.ResolveRefAndMarkUsed(n)
		if err != nil {
			return nil, err
		}
		return e.retcol(v), nil
	case sqlparser.ValTuple:
		v := tree.(sqlparser.ValTuple)
		ve := sqlparser.ValExprs(v)
		ave := ([]sqlparser.ValExpr)(ve)
		return e.MakeSlice(ave)
	case *sqlparser.Subquery: // Important architecture work needed
		return nil, fmt.Errorf("subquery not impl, TODO")
	case sqlparser.ListArg: // RARE
		return nil, fmt.Errorf("ListArg not impl, TODO")
	case *sqlparser.BinaryExpr:
		return e.binaryExpr(tree.(*sqlparser.BinaryExpr))
	case *sqlparser.UnaryExpr: // RARE, except minus?
		return nil, fmt.Errorf("Unary not impl, TODO")
	case *sqlparser.FuncExpr:
		// EZ: reuse template code. Solves basic types.
		return e.MakeFunc(tree.(*sqlparser.FuncExpr))
	case *sqlparser.CaseExpr:
		_ = tree.(*sqlparser.CaseExpr)
		// case EXPR ((WHEN boolVal) THEN (whenexpr))+ ELSE elseexpr
		return nil, fmt.Errorf("Case not impl, TODO")
	}

	return nil, fmt.Errorf("wacky value")
}

func (e *ExpressionBuilder) MakeSlice(t []sqlparser.ValExpr) (E, error) {
	res := []E{}
	// TODO determine if they're static values and pass-it-on
	for _, vale := range t {
		v, err := e.MakeVal(vale)
		if err != nil {
			return nil, err
		}
		res = append(res, v)
	}
	return func(row map[string]interface{}) (interface{}, error) {
		r2 := []interface{}{}
		for _, a := range res {
			v, err := a(row)
			if err != nil {
				return nil, err
			}
			r2 = append(r2, v)
		}
		return r2, nil
	}, nil
}

func (e *ExpressionBuilder) retcol(s string) E {
	return func(row map[string]interface{}) (val interface{}, err error) {
		v, ok := row[s]
		if !ok {
			base.Debug("cant find", s, " data avail:", row, e.SrcTables)
			return nil, fmt.Errorf("Cannot find column for ref %s", s)
		}
		return v, nil
	}
}

func retval(v interface{}, err error) func(map[string]interface{}) (interface{}, error) {
	return func(row map[string]interface{}) (val interface{}, err error) {
		return v, err // How else do you return null
	}
}
