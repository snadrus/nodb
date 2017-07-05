package expr

import (
	"bytes"
	"fmt"
	"html/template"
	"math"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/xwb1989/sqlparser"
)

func (e *ExpressionBuilder) MakeFunc(fe *sqlparser.FuncExpr) (E, error) {
	argString := string(fe.Name)
	if fe.Distinct {
		if argString != "count" {
			return nil, fmt.Errorf("I don't know what DISTINCT %s should do. TODO Impl", argString)
		}
		argString = "countdistinct"
	}

	if af, ok := aggFuncs[argString]; ok {
		return e.MakeAgg(fe, af)
	}

	args := []E{}
	for i, farg := range fe.Exprs {
		if _, ok := farg.(*sqlparser.StarExpr); ok {
			return nil, fmt.Errorf("Star in Func not impl, TODO. Like SELECT count(*)")
		}
		argE, err := e.ExprToE(farg.(*sqlparser.NonStarExpr).Expr)
		if err != nil {
			return nil, err
		}
		args = append(args, argE)
		argString += " .column" + strconv.Itoa(i)
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("No arg for func: %s", fe.Name)
	}

	// Allow their own functions (instead of using funcmap)
	funcMap := FuncMap
	name := string(fe.Name)
	fn, ok := FuncMap[name]
	if !ok {
		fn, ok = e.Obj[name]
		if !ok {
			return nil, fmt.Errorf("Function not found: %s", name)
		}
		funcMap = map[string]interface{}{name: fn}
	}
	t, _ := template.New("A").Funcs(funcMap).Parse("{{" + argString + "}}")

	return func(row map[string]interface{}) (i interface{}, err error) {
		templateVars := make(map[string]interface{}, len(args))
		for i, exp := range args {
			v, err := exp(row)
			if err != nil || v == nil {
				return v, err
			}
			templateVars["column"+strconv.Itoa(i)] = v
		}
		b := bytes.Buffer{}
		if err := t.Execute(&b, templateVars); err != nil {
			return nil, err
		}

		k := reflect.ValueOf(fn).Type().Out(0).Kind()
		switch k {
		case reflect.String:
			return b.String(), nil
		case reflect.Int:
			return strconv.Atoi(b.String())
		case reflect.Float64:
			return strconv.ParseFloat(b.String(), 64)
		}
		return nil, fmt.Errorf("Your function's return type is not supported: %v", k)
	}, nil
}

var FuncMap = map[string]interface{}{
	/* String Functions */
	"char_length":  func(s string) int { return utf8.RuneCount([]byte(s)) },
	"lower":        strings.ToLower,
	"upper":        strings.ToUpper,
	"octet_length": func(s string) int { return len([]byte(s)) },
	"position":     stringFuncFind,
	"find":         stringFuncFind,
	"textpos":      stringFuncFind,
	"index":        stringFuncFind,
	"substr":       stringFuncSubstr,
	"substring":    stringFuncSubstr,

	/* Math functions */
	"abs":   math.Abs,
	"pow":   math.Pow,
	"minOf": math.Min,
	"maxOf": math.Max,
	"floor": math.Floor,
	"ceil":  math.Ceil,
	// TODO EASY add other math functions like sin/cos/log
}

func stringFuncFind(s, sep string) int {
	return strings.Index(s, sep)
}
func stringFuncSubstr(s string, start int, sArgs ...int) string {
	if start >= len(s) || start < 0 {
		return ""
	}
	if len(sArgs) == 0 {
		return s[start:]
	}
	end := start + sArgs[0]
	if end < start {
		return ""
	}
	return s[start:end]
}
