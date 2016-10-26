package expr

import (
	"errors"

	"github.com/Knetic/govaluate"
	"github.com/snadrus/nodb/internal/base"
	"github.com/xwb1989/sqlparser"
)

func (e *ExpressionBuilder) binaryExpr(tree *sqlparser.BinaryExpr) (E, error) {
	l, err := e.ExprToE(tree.Left) // TODO if both are constants, precalculate
	if err != nil {
		return nil, err
	}
	r, err := e.ExprToE(tree.Right)
	if err != nil {
		return nil, err
	}

	// 100% perfect match for all math operators. Neat!
	ee, err := govaluate.NewEvaluableExpression("l " + string(tree.Operator) + " r")
	if err != nil {
		return nil, err
	}
	return doBinOp(l, ee, r), err
}

func doBinOp(l E, op *govaluate.EvaluableExpression, r E) E { // candidate for govaluate
	return func(row map[string]interface{}) (val interface{}, err error) {
		left, err := l(row)
		if err != nil || left == nil {
			return left, err
		}
		right, err := r(row)
		if err != nil || right == nil {
			return right, err
		}
		base.Debug("doing ", op.String(), "on", left, "and", right)
		return op.Evaluate(map[string]interface{}{"l": left, "r": right})
	}
}

func (e *ExpressionBuilder) ExprToE(tree sqlparser.Expr) (E, error) {
	bla := tree.(interface{})
	v, ok := bla.(sqlparser.ValExpr)
	if ok {
		return e.MakeVal(v)
	}
	b, ok := bla.(sqlparser.BoolExpr)
	if ok {
		return e.MakeBool(b)
	}
	return nil, errors.New("Unknown expression type")
}
