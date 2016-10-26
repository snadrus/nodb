package sel

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/snadrus/nodb/internal/base"
	"github.com/snadrus/nodb/internal/expr"
)

type plan struct {
	rowMaker       rowMaker
	where          condition
	joins          []*joinElement
	src            base.SrcTables
	GroupProcessor *groupProcessor
	so             *orderBySortable
}

func planQuery(out rowMaker, joins []*joinElement, whereCond condition, src base.SrcTables) (*plan, error) {
	for _, t := range joins {
		tv := reflect.ValueOf(t.table.Table)
		if tv.Kind() != reflect.Slice {
			return nil, fmt.Errorf("not slice of rows")
		}
	}
	return &plan{
		rowMaker: out,
		joins:    joins,
		where:    whereCond,
	}, nil
}

type row map[string]interface{}
type chainType chan row

// Run a query plan
func (p *plan) Run(ch chan GetChanError) {
	for _, t := range p.joins {
		doNest(t) // x*y strategy. Better ones later
	}

	if p.GroupProcessor != nil {
		if p.so != nil {
			p.GroupProcessor.SetSortOutput(p.so)
		}
		p.GroupProcessor.SetOutputChan(ch)
	}

	joinOutput := p.joins[len(p.joins)-1].resultChan
	for res := range joinOutput {
		ok, err := p.where(res)
		if err != nil {
			ch <- GetChanError{nil, err}
			go toDevNull(joinOutput)
			// TODO clear the goroutine recursion
			return
		}
		if !ok.(bool) { // WHERE says skip it
			continue
		}

		if p.GroupProcessor == nil { // Simple non-agg select only
			finalRow, err := p.rowMaker(res) // The SELECT processing
			if err != nil {
				ch <- GetChanError{nil, err}
				go toDevNull(joinOutput)
				return
			}

			if p.so != nil {
				p.so.AddRow(res, finalRow)
			} else {
				ch <- GetChanError{finalRow, err}
			}
		} else {
			p.GroupProcessor.Input <- res
		}
	}
	if p.GroupProcessor != nil {
		close(p.GroupProcessor.Input)
		p.GroupProcessor.Wg.Wait()
	}
	if p.so != nil {
		p.so.SortAndOutput(ch)
	}
}

func toDevNull(ch chan row) {
	for _ = range ch {
	}
}

// MakeGroupBy takes []Val maker and aggregate-possible HAVING bool.
func (p *plan) MakeGroupBy(gb expr.E, SelectExpr *expr.ExpressionBuilder, HavingExpr *expr.ExpressionBuilder, outrow aggRowMaker) error {
	// Also, SELECT expr aggregates needs dealing-with.
	p.GroupProcessor = makeGroupBy(gb, SelectExpr, HavingExpr, outrow) // save it
	return errors.New("MakeGroupBy TODO")
}

// MakeGroupBy takes []Val maker and aggregate-possible HAVING bool.
func (p *plan) MakeOrderBy(so *orderBySortable) {
	p.so = so
}
