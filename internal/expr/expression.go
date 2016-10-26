package expr

import "github.com/snadrus/nodb/internal/base"

// E -xpression, Eventually
type E func(map[string]interface{}) (interface{}, error)

/*
type E struct {
	Vars []string // tells the planner what it uses
	Eval func(map[string]interface{}) (interface{}, error)
}*/

// ExpressionBuilder builds expressions with regular functions
type ExpressionBuilder struct {
	Funcs map[string]interface{} // Should be identical to template's function caller
	base.SrcTables
	AggProcessing *[]AggProcessing
	Expr          E // Expression storage relating to this builder
	Obj           map[string]interface{}
}

// returns true & OK because it is the default WHERE & HAVING
var DefaultBuilder = &ExpressionBuilder{ // have a default
	Expr: func(map[string]interface{}) (interface{}, error) { return true, nil },
}

func (e *ExpressionBuilder) Dup() *ExpressionBuilder {
	tmp := *e
	return &tmp
}
func (e *ExpressionBuilder) Setup(
	t base.SrcTables, f map[string]interface{}) *ExpressionBuilder {

	e.Funcs = f
	e.SrcTables = t
	return e
}
func (e *ExpressionBuilder) AllowAggregates() {
	e.AggProcessing = &[]AggProcessing{}
}
