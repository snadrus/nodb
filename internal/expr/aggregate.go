package expr

import (
	"fmt"
	"math"
	"strconv"

	"github.com/xwb1989/sqlparser"
)

// Plan: SELECT & HAVING expressionbuilders should set
// e.AggProcessing = []expr.AggProcessing{}  to allow aggregates.
// for everything GROUPBY touches, run:
// for new groupby: DEEP_COPY e.AggProcessing
//    for _, apCopy := range groupByThingy.AggProcessingCopies {
//         err := apCopy.Incr(row)
//         ...
// Then delete the row (if not first).
// When input stops, per group: replace the e.AggProcessing and run expression
func (e *ExpressionBuilder) MakeAgg(fe *sqlparser.FuncExpr, ag func(E) AggProcessing) (E, error) {
	if e.AggProcessing == nil {
		return nil, fmt.Errorf("Illegal Location for aggregate function %s", fe.Name)
	}

	if len(fe.Exprs) != 1 {
		return nil, fmt.Errorf("bad arg count for %s", fe.Name)
	}

	selfAddr := len(*e.AggProcessing)

	var argE func(map[string]interface{}) (interface{}, error)
	if _, ok := fe.Exprs[0].(*sqlparser.StarExpr); ok {
		if string(fe.Name) != "count" {
			return nil, fmt.Errorf("Star in Func ?")
		}
	} else {
		var err error
		argE, err = e.ExprToE(fe.Exprs[0].(*sqlparser.NonStarExpr).Expr)
		if err != nil {
			return nil, err
		}
		if len(*e.AggProcessing) != selfAddr {
			return nil, fmt.Errorf("Illegal nested Aggregate functions under %s", fe.Name)
		}
	}

	self := ag(argE)
	*e.AggProcessing = append(*e.AggProcessing, self)

	// This is a tricky one. e.AggProcessing gets replaced, but initialCount is local
	return func(m map[string]interface{}) (interface{}, error) {
		return self.Value(m[aggDataKey].([]interface{})[selfAddr]), nil
	}, nil
}

// SECRET row-key that doubles as data transfer for aggregates
const aggDataKey = "nodb_aggdata"

//Aggregate functions are only for SELECT and HAVING clauses. Processed in GROUPBY.
//ExpressionBuilder.aggregate []func (if non-nil) should get added.
// Values will be placed in the same-offset e.AggregateResults

type AggProcessing interface {
	Value(interface{}) interface{}                  // Get final value & reset
	Incr(map[string]interface{}, interface{}) error // GroupBy iteration
	Initial() interface{}
}

type AggGroup struct {
	*ExpressionBuilder
	data     []interface{}
	TokenRow map[string]interface{}
}

// NewAggGroup sets-up GroupBy to easily manage aggregate expressions
func (e *ExpressionBuilder) NewAggGroup() *AggGroup {
	return &AggGroup{
		ExpressionBuilder: e,
		data:              make([]interface{}, len(*(e.AggProcessing))),
	}
}

// ConsumeRow eats a row and runs aggregate incrementers on it
func (g *AggGroup) ConsumeRow(row map[string]interface{}) error {
	var err error
	if g.TokenRow == nil {
		g.TokenRow = row
		for i, a := range *(g.ExpressionBuilder.AggProcessing) {
			g.data[i] = a.Initial()
		}
	}
	for i, a := range *(g.ExpressionBuilder.AggProcessing) {
		err = a.Incr(row, g.data[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// RenderExpression will get a result. Even works with non-aggregate expressions
func (g *AggGroup) RenderExpression(E E) (interface{}, error) {
	if g.TokenRow == nil {
		g.TokenRow = map[string]interface{}{}
	}
	g.TokenRow[aggDataKey] = g.data
	return E(g.TokenRow)
}

// Aggregate functions req. 0agg() returning the last-set float64
// TODO handle aggregate functions by saving an array
// Because:  HAVING sum(x) > sum(y)
var aggFuncs = map[string]func(E) AggProcessing{
	"count": newAggCount,
	"avg":   newAggAvg,
	"min":   newAggMin,
	"max":   newAggMax,
	"sum":   newAggSum,
}

func newAggCount(e E) AggProcessing {
	return &AggCount{}
}

type AggCount struct{}

type AggCountData struct {
	i int
}

func (a *AggCount) Initial() interface{} {
	return &AggCountData{}
}
func (a *AggCount) Incr(row map[string]interface{}, vp interface{}) error {
	vp.(*AggCountData).i++
	return nil
}
func (a *AggCount) Value(vp interface{}) (res interface{}) {
	return vp.(*AggCountData).i
}

func newAggMax(e E) AggProcessing {
	return &AggMax{E: e}
}

type AggMax struct {
	E
}

type AggFloatData struct {
	i float64
}

func (a *AggMax) Initial() interface{} {
	return &AggFloatData{math.Inf(-1)}
}
func toFloat(vI interface{}) (float64, error) {
	return strconv.ParseFloat(fmt.Sprintf("%v", vI), 64)
}

func (a *AggMax) Incr(row map[string]interface{}, vp interface{}) error {
	vI, err := a.E(row)
	if err != nil {
		return err
	}
	if vI == nil {
		return fmt.Errorf("Nil value returned")
	}
	v, err := toFloat(vI)
	if err != nil {
		return err
	}
	if tmp := vp.(*AggFloatData); tmp.i < v {
		tmp.i = v
	}
	return nil
}
func (a *AggMax) Value(vp interface{}) (res interface{}) {
	return vp.(*AggFloatData).i
}

func newAggMin(e E) AggProcessing {
	return &AggMin{E: e}
}

type AggMin struct {
	E
}

func (a *AggMin) Initial() interface{} {
	return &AggFloatData{math.Inf(1)}
}

func (a *AggMin) Incr(row map[string]interface{}, vp interface{}) error {
	vI, err := a.E(row)
	if err != nil {
		return err
	}
	if vI == nil {
		return fmt.Errorf("Nil value returned")
	}
	v, err := toFloat(vI)
	if err != nil {
		return err
	}
	if tmp := vp.(*AggFloatData); tmp.i > v {
		tmp.i = v
	}
	return nil
}
func (a *AggMin) Value(vp interface{}) (res interface{}) {
	return vp.(*AggFloatData).i
}

func newAggSum(e E) AggProcessing {
	return &AggSum{E: e}
}

type AggSum struct {
	E
}

func (a *AggSum) Initial() interface{} {
	return &AggFloatData{}
}

func (a *AggSum) Incr(row map[string]interface{}, vp interface{}) error {
	vI, err := a.E(row)
	if err != nil {
		return err
	}
	if vI == nil {
		return fmt.Errorf("Nil value returned")
	}
	v, err := toFloat(vI)
	if err != nil {
		return err
	}
	vp.(*AggFloatData).i += v
	return nil
}
func (a *AggSum) Value(vp interface{}) (res interface{}) {
	return vp.(*AggFloatData).i
}

func newAggAvg(e E) AggProcessing {
	return &AggAvg{E: e}
}

type AggAvg struct {
	E
}

type AggAvgData struct {
	V  float64
	Ct int
}

func (a *AggAvg) Initial() interface{} {
	return &AggAvgData{}
}

func (a *AggAvg) Incr(row map[string]interface{}, vp interface{}) error {
	vI, err := a.E(row)
	if err != nil {
		return err
	}
	if vI == nil {
		return fmt.Errorf("Nil value returned")
	}
	v, err := toFloat(vI)
	if err != nil {
		return err
	}

	vtmp := vp.(*AggAvgData)
	vtmp.V += v
	vtmp.Ct++
	return nil
}
func (a *AggAvg) Value(vp interface{}) (res interface{}) {
	vtmp := vp.(*AggAvgData)
	return vtmp.V / float64(vtmp.Ct)
}
