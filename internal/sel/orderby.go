package sel

import (
	"fmt"
	"sort"

	"github.com/Knetic/govaluate"
	"github.com/snadrus/nodb/internal/base"
	"github.com/snadrus/nodb/internal/expr"
	"github.com/xwb1989/sqlparser"
)

type lessFunc func(left, right row) bool

type fullAndFinal struct {
	row
	final []interface{}
}
type orderBySortable struct {
	r []fullAndFinal
	lessFunc
	err error
}

func (s *orderBySortable) Len() int      { return len(s.r) }
func (s *orderBySortable) Swap(i, j int) { s.r[i], s.r[j] = s.r[j], s.r[i] }
func (s *orderBySortable) Less(i, j int) bool {
	return s.lessFunc(s.r[i].row, s.r[j].row)
}

func (s *orderBySortable) AddRow(full row, final []interface{}) {
	base.Debug("orderby gets row:", final)
	s.r = append(s.r, fullAndFinal{full, final})
}

func (s *orderBySortable) SortAndOutput(ch chan base.GetChanError) {
	defer func() {
		if v := recover(); v != nil {
			ch <- base.GetChanError{nil, fmt.Errorf("orderby expr eval: %s", v.(error))}
		}
	}()
	sort.Sort(s)
	for _, r := range s.r {
		ch <- base.GetChanError{r.final, nil}
	}
}

var eq *govaluate.EvaluableExpression
var lt *govaluate.EvaluableExpression
var gt *govaluate.EvaluableExpression

func init() {
	eq, _ = govaluate.NewEvaluableExpression("l == r")
	lt, _ = govaluate.NewEvaluableExpression("l < r ")
	gt, _ = govaluate.NewEvaluableExpression("l > r ")
}

type orderTerm struct {
	expr.E
	ltIfAsc *govaluate.EvaluableExpression
}

func makeSortable(tob sqlparser.OrderBy, eb *expr.ExpressionBuilder) (*orderBySortable, error) {
	terms := []orderTerm{}
	for _, o := range tob {
		ltIfAsc := lt
		if o.Direction == sqlparser.AST_DESC {
			ltIfAsc = gt
		}

		e, err := eb.ExprToE(o.Expr)
		if err != nil {
			return nil, err
		}

		terms = append(terms, orderTerm{
			E:       e,
			ltIfAsc: ltIfAsc,
		})
	}
	return &orderBySortable{
		lessFunc: func(left, right row) bool {
			for _, t := range terms {
				leftV, err := t.E(left)
				if err != nil {
					panic(err)
				}
				rightV, err := t.E(right)
				if err != nil {
					panic(err)
				}
				isEq, err := eq.Evaluate(map[string]interface{}{"l": leftV, "r": rightV})
				if err != nil {
					panic(err)
				}
				if isEq.(bool) {
					continue
				}
				orderedCorrectly, err := t.ltIfAsc.Evaluate(map[string]interface{}{
					"l": leftV, "r": rightV})
				if err != nil {
					panic(err)
				}
				return orderedCorrectly.(bool)
			}
			return false // default answer
		},
	}, nil
}
