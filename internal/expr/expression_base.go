package expr

import "github.com/snadrus/nodb/internal/base"

// E -xpression function
type E func(map[string]interface{}) (interface{}, error)

// ExpressionBuilder builds expressions with regular functions
type ExpressionBuilder struct {
	base.SrcTables
	AggProcessing *[]AggProcessing
	Expr          E // Expression storage relating to this builder
	Obj           map[string]interface{}
	SubqueryRunner
}

// DefaultBuilder returns true & OK because it is the default WHERE & HAVING
var DefaultBuilder = &ExpressionBuilder{ // have a default
	Expr: func(map[string]interface{}) (interface{}, error) { return true, nil },
}

// Dup -licate an expressionbuilder that's correct (but you want agg)
func (e *ExpressionBuilder) Dup() *ExpressionBuilder {
	tmp := *e
	return &tmp
}
func (e *ExpressionBuilder) Setup(
	t base.SrcTables, Obj map[string]interface{}, sqFunc SubqueryRunner) *ExpressionBuilder {

	e.Obj = Obj // original object pass-in
	e.SrcTables = t
	e.SubqueryRunner = sqFunc
	return e
}

// AllowAggregates Indicate if this builder should enable aggregate processing.
func (e *ExpressionBuilder) AllowAggregates() {
	e.AggProcessing = &[]AggProcessing{}
}
