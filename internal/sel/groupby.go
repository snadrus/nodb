package sel

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/snadrus/nodb/internal/base"
	"github.com/snadrus/nodb/internal/expr"
)

type groupProcessor struct {
	Input chan row
	out   chan base.GetChanError
	Wg    *sync.WaitGroup
	so    *orderBySortable
}

func (g *groupProcessor) SetOutputChan(ch chan base.GetChanError) {
	g.out = ch
}
func (g *groupProcessor) SetSortOutput(so *orderBySortable) {
	g.so = so
}

type aggs struct {
	selectAgg *expr.AggGroup
	havingAgg *expr.AggGroup
}

//MakeGroupBy plan:
/*
from the proto-row input channel:
  - determine GroupBy terms, which may include SELECT terms
	  - Where did we validate the column ref & GROUPBY term is non-aggregate?
	- Within that group: (goroutine?)
	  - complete SELECT on non-aggregates if it's our first time
		- process aggregate selects & havings
	- OnClose() -> propogate to row goroutines to complete & do HAVING filter & output
	ELSEWHERE: ORDERBY  lives in result receiver, DISTINCT reuses this
*/
func makeGroupBy(
	gb expr.E,
	SelectBuilder *expr.ExpressionBuilder,
	HavingBuilder *expr.ExpressionBuilder,
	outrow aggRowMaker,
	ctx context.Context) *groupProcessor {
	gp := groupProcessor{Input: make(chan row), Wg: &sync.WaitGroup{}}
	gp.Wg.Add(1)
	groups := map[string]aggs{}
	havingNeedsSelectFields := HavingBuilder != nil &&
		len(HavingBuilder.SrcTables["1Select"].UsedFields) > 0

	makeSelectAgg := func(row row) (row, error) {
		selectAgg := SelectBuilder.NewAggGroup()
		selectAgg.ConsumeRow(row)
		_, err := outrow(selectAgg)
		return selectAgg.TokenRow, err
	}
	go func() {
		defer gp.Wg.Done()
		for row := range gp.Input {
			v, err := gb(row) // Get the GB expression list
			if err != nil {
				if strings.Contains(err.Error(), "1Select.") {
					row, err = makeSelectAgg(row) // Populate row properly with Select stuff
					base.Debug("makeSelectAgg result=", row)
					if err == nil {
						v, err = gb(row)
					}
				}
				if err != nil {
					select {
					case gp.out <- base.GetChanError{Err: err}:
					case <-ctx.Done():
						return
					}
					go toDevNull(gp.Input)
					return
				}
			}
			keyB, err := json.Marshal(v) // Serialize it
			if err != nil {
				select {
				case gp.out <- base.GetChanError{Err: err}:
				case <-ctx.Done():
					return
				}
				go toDevNull(gp.Input)
				return
			}
			key := string(keyB)
			if _, ok := groups[key]; !ok { //
				tmp := aggs{SelectBuilder.NewAggGroup(), nil}
				if HavingBuilder != nil {
					tmp.havingAgg = HavingBuilder.NewAggGroup()
				}
				groups[key] = tmp
			}
			groups[key].selectAgg.ConsumeRow(row)
			if HavingBuilder != nil {
				groups[key].havingAgg.ConsumeRow(row)
			}
			base.Debug("gbe=", v)
		}

		finRend := func(sa *expr.AggGroup) bool {
			sr, err := outrow(sa)
			if err != nil {
				select {
				case gp.out <- base.GetChanError{Err: err}:
				case <-ctx.Done():
				}
				return false
			}
			if gp.so != nil {
				gp.so.AddRow(sa.TokenRow, sr)
			} else {
				select {
				case gp.out <- base.GetChanError{sr, nil}:
				case <-ctx.Done():
					return false
				}
			}
			return true
		}
		if HavingBuilder != nil {
			for _, gr := range groups {
				// MUST render the select results if we use those fields
				var sr []interface{}
				if havingNeedsSelectFields {
					var err error
					sr, err = outrow(gr.selectAgg)
					if err != nil {
						select {
						case gp.out <- base.GetChanError{Err: err}:
						case <-ctx.Done():
						}
						return
					}
				}
				gr.havingAgg.TokenRow = gr.selectAgg.TokenRow
				base.Debug("selectTokenRow", gr.selectAgg.TokenRow)
				if b, err := gr.havingAgg.RenderExpression(HavingBuilder.Expr); err != nil {
					select {
					case gp.out <- base.GetChanError{Err: err}:
					case <-ctx.Done():
					}
					return
				} else if b.(bool) {
					if havingNeedsSelectFields {
						if gp.so != nil {
							gp.so.AddRow(gr.selectAgg.TokenRow, sr)
						} else {
							select {
							case gp.out <- base.GetChanError{sr, nil}:
							case <-ctx.Done():
								return
							}

						}
					} else if !finRend(gr.selectAgg) {
						return
					}
				}
			}
		} else {
			for _, gr := range groups {
				if !finRend(gr.selectAgg) {
					return
				}
			}
		}
	}()
	return &gp
}

//groupby[+selectAgg][+groupbyAgg][+havingReducer][Collect]
//groupby[+Distinct]+orderby(reuse collection for sorting)
//orderby(collect for sorting at end)
//distinct

// OrderBy+Distinct saves some work, just distinct can be a map though
