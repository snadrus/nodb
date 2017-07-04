package expr

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/Knetic/govaluate"
	"github.com/xwb1989/sqlparser"
)

func (e *ExpressionBuilder) MakeBool(tree sqlparser.BoolExpr) (E, error) {
	if _, ok := tree.(*sqlparser.ParenBoolExpr); ok {
		tree = tree.(*sqlparser.ParenBoolExpr).Expr // short-cut it
	}
	getLR := func(l, r sqlparser.BoolExpr) (E, E, error) {
		left, err := e.MakeBool(l)
		if err != nil {
			return nil, nil, err
		}
		right, err := e.MakeBool(r)
		if err != nil {
			return nil, nil, err
		}
		return left, right, nil
	}
	switch tree.(type) {
	case *sqlparser.AndExpr:
		left, right, err := getLR(tree.(*sqlparser.AndExpr).Left, tree.(*sqlparser.AndExpr).Right)
		if err != nil {
			return nil, err
		}
		return makeAnd(left, right), nil
	case *sqlparser.OrExpr:
		left, right, err := getLR(tree.(*sqlparser.OrExpr).Left, tree.(*sqlparser.OrExpr).Right)
		if err != nil {
			return nil, err
		}
		return makeOr(left, right), nil
	case *sqlparser.NotExpr:
		e_tmp, err := e.MakeBool(tree.(*sqlparser.NotExpr).Expr)
		if err != nil {
			return nil, err
		}
		return makeNot(e_tmp), nil
	case *sqlparser.ComparisonExpr:
		return e.MakeCompare(tree.(*sqlparser.ComparisonExpr))
	case *sqlparser.RangeCond:
		return nil, errors.New("RANGE not impl, TODO")
	case *sqlparser.NullCheck:
		return nil, errors.New("IS NULL not impl, TODO")
	case *sqlparser.ExistsExpr:
		return nil, errors.New("EXISTS not impl, TODO")
	default:
		return nil, fmt.Errorf("Unknown Expr")
	}
}

// MakeCompare compares all types
func (e *ExpressionBuilder) MakeCompare(tree *sqlparser.ComparisonExpr) (E, error) {
	left, err := e.MakeVal(tree.Left)
	if err != nil {
		return nil, err
	}
	right, err2 := e.MakeVal(tree.Right)
	if err2 != nil {
		return nil, err2
	}
	op := ""
	switch tree.Operator {
	case sqlparser.AST_EQ:
		op = "=="
	case sqlparser.AST_LT:
		op = string(sqlparser.AST_LT)
	case sqlparser.AST_GT:
		op = string(sqlparser.AST_GT)
	case sqlparser.AST_LE:
		op = string(sqlparser.AST_LE)
	case sqlparser.AST_GE:
		op = string(sqlparser.AST_GE)
	case sqlparser.AST_NE:
		op = string(sqlparser.AST_NE)
	case sqlparser.AST_NSE:
		return nil, fmt.Errorf("Comparision '%s' not impl. Worthless?, TODO", tree.Operator)
	case sqlparser.AST_IN:
		return contains(right, left), nil
	case sqlparser.AST_NOT_IN:
		return makeNot(contains(right, left)), nil
	case sqlparser.AST_LIKE:
		op = "=~"
		right = likeExpr(right)
	case sqlparser.AST_NOT_LIKE:
		op = "!~"
		right = likeExpr(right)
	default:
		return nil, fmt.Errorf("Unrecognized comparison")
	}

	ee, err := govaluate.NewEvaluableExpression("l " + op + " r")
	if err != nil {
		return nil, err
	}
	return doBinOp(left, ee, right), err
}

// Translate an SQL LIKE to a REGEX
func likeExpr(e E) E {
	return func(row map[string]interface{}) (interface{}, error) {
		intf, err := e(row)
		if err != nil || intf == nil {
			return nil, err
		}
		str, ok := intf.(string)
		if !ok {
			return nil, fmt.Errorf("Bad LIKE regex type of %v", str)
		}
		return "^" + strings.Replace(str, "%", ".*", -1) + "$", nil
	}
}

var eq, _ = govaluate.NewEvaluableExpression("l == r")

func contains(sliceE, itemE E) E {
	return func(row map[string]interface{}) (val interface{}, err error) {
		slice, err := sliceE(row)

		if slice == nil || err != nil {
			return slice, err
		}
		item, err := itemE(row)

		if item == nil || err != nil {
			return item, err
		}

		rs := reflect.ValueOf(slice)
		if rs.Kind() != reflect.Slice {
			return nil, fmt.Errorf("Incorrect RHS for IN clause: %v", slice)
		}
		length := rs.Len()
		for a := 0; a < length; a++ {
			rsIntf := rs.Index(a).Interface()

			v, err := eq.Evaluate(map[string]interface{}{"l": rsIntf, "r": item})
			if (err == nil && v.(bool)) || reflect.DeepEqual(rsIntf, item) {
				return true, nil
			}
		}
		// TODO reuse map if slice is constant

		return false, nil
	}
}

func makeNot(e_tmp E) E {
	return func(row map[string]interface{}) (val interface{}, err error) {
		val, err = e_tmp(row)
		if vbool, ok := val.(bool); ok {
			return !vbool, err
		}
		if val != nil {
			err = fmt.Errorf("Couldn't parse value for ! not: %v", val)
		}
		return nil, err
	}
}

func makeOr(left, right E) E {
	return func(row map[string]interface{}) (val interface{}, err error) {
		for _, fn := range []E{left, right} {
			res, err2 := fn(row)
			if err != nil || res == nil {
				err = err2 // error & true should be true.
			}
			if res.(bool) == true {
				return true, nil
			}
		}
		return false, err
	}
}

func makeAnd(left, right E) E {
	return func(row map[string]interface{}) (val interface{}, err error) {
		for _, fn := range []E{left, right} {
			res, err2 := fn(row)
			if err != nil || res == nil {
				err = err2 // error && false should be false.
			}
			if res.(bool) == false {
				return false, nil
			}
		}
		return true, err
	}
}
