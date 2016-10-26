package expr

import (
	"bytes"
	"fmt"
	"html/template"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func (e *ExpressionBuilder) MakeFunc(fe *sqlparser.FuncExpr) (E, error) {
	argString := string(fe.Name)
	if fe.Distinct {
		return nil, fmt.Errorf("I don't know what DISTINCT %s should do. TODO Impl", argString)
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
		argString += " column" + strconv.Itoa(i)
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("No arg for func: %s", fe.Name)
	}

	// TODO allow their own functions (instead of using funcmap)
	fn, ok := FuncMap[string(fe.Name)]
	if !ok {
		fn, ok = e.Obj[string(fe.Name)]
		if !ok {
			return nil, fmt.Errorf("Function not found: %s", fe.Name)
		}
	}
	t, _ := template.New("A").Funcs(FuncMap).Parse("{{" + argString + "}}")
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
	"char_length":  func(s string) int { return len(s) },
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
func stringFuncSubstr(s string, sArgs ...int) string {
	if sArgs[0] >= len(s) || sArgs[0] < 0 {
		return ""
	}
	end := len(s) - sArgs[0]
	if len(sArgs) == 2 {
		if sArgs[1] <= sArgs[0] || sArgs[1] >= len(s) {
			return ""
		}
		end = sArgs[2]
	}
	return s[sArgs[0]:end]
}
