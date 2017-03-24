package sel

import (
	"fmt"
	"reflect"

	"github.com/snadrus/nodb/internal/base"
	"github.com/snadrus/nodb/internal/expr"
	"github.com/xwb1989/sqlparser"
)

type rowMaker func(srcRow map[string]interface{}) (outRow []interface{}, e error)
type aggRowMaker func(agg *expr.AggGroup) (outRow []interface{}, e error)

type getInstructions struct {
	as           string
	E            expr.E
	addableToRow bool
}

func doSelect(s sqlparser.SelectExprs, builder *expr.ExpressionBuilder) (rowMaker, aggRowMaker, []string, error) {
	var itemsToGet = []getInstructions{}
	var colNames []string
	avoidDupe := map[string]bool{}
	selTbl := base.SrcTable{
		Name:       "1Select", //impossible
		Table:      base.NewSliceOfStructRP([]interface{}{}),
		UsedFields: map[string]bool{},
	}
	for _, exp := range s {
		switch exp.(type) {
		case *sqlparser.StarExpr:
			tmpSet := builder.SrcTables //get table names
			tname := exp.(*sqlparser.StarExpr).TableName
			if tname != nil {
				t0, ok := builder.SrcTables[string(tname)] // get table from map
				if !ok {
					return nil, nil, nil, fmt.Errorf("Invalid tablename %s", tname)
				}
				tmpSet = base.SrcTables{string(tname): t0}
			}
			for _, tbl := range tmpSet {
				if tbl.Name == "1Select" {
					continue
				}
				reflect.TypeOf(tbl.Table).Elem().NumField() // ONLY for []STRUCT{}
				if tbl.HasPrivateFields {
					return nil, nil, nil, fmt.Errorf("SELECT * FROM %s fails for private fields. Wrap %s's struct in another struct", tbl.Name, tbl.Name)
				}
				for _, fname := range tbl.Fields {
					if _, ok := avoidDupe[fname]; ok {
						continue
					}
					avoidDupe[fname] = true
					tbl.UsedFields[fname] = true
					func(fullname string) {
						colNames = append(colNames, fname)
						itemsToGet = append(itemsToGet, getInstructions{
							as: fname,
							E: func(row map[string]interface{}) (interface{}, error) {
								base.Debug("Getting", fullname, "from", row)
								return row[fullname], nil
							},
						})
					}(string(tbl.Name) + "." + fname)
				}
			}
		default:
			exp2 := exp.(*sqlparser.NonStarExpr)
			eAs := string(exp2.As)
			if len(eAs) == 0 {
				if nm, ok := exp2.Expr.(*sqlparser.ColName); ok {
					eAs = string(nm.Name)
					if len(nm.Qualifier) > 0 {
						eAs = string(nm.Qualifier) + "." + eAs
					}
				} // else don't bother giving it a name
			}
			if _, ok := avoidDupe[eAs]; ok {
				continue
			}
			avoidDupe[eAs] = true
			expE, err := builder.ExprToE(exp2.Expr)
			if err != nil {
				return nil, nil, nil, err
			}
			addableToRow := false
			if len(eAs) > 0 { // add to src table for GROUPBY, HAVING, ORDERBY
				selTbl.Fields = append(selTbl.Fields, eAs)
				addableToRow = true
			}
			colNames = append(colNames, eAs)
			itemsToGet = append(itemsToGet, getInstructions{
				as:           eAs,
				E:            expE,
				addableToRow: addableToRow,
			})
		}
	}

	colCount := len(itemsToGet)
	r := func(srcRow map[string]interface{}) (outRow []interface{}, e error) {
		outRow = make([]interface{}, colCount)
		for i, toGet := range itemsToGet {
			v, err := toGet.E(srcRow)
			if err != nil {
				return nil, err
			}
			base.Debug("will call ", v, " as ", i, " from ", srcRow)
			outRow[i] = v
			if toGet.addableToRow {
				if _, ok := selTbl.UsedFields[toGet.as]; ok {
					base.Debug("sel adding 1Select.", toGet.as, "=", v)
					srcRow["1Select."+toGet.as] = v
				}
			}
		}
		return outRow, nil
	}

	// aggregates involved
	rAgg := func(agg *expr.AggGroup) (outRow []interface{}, e error) {
		outRow = make([]interface{}, colCount)
		for i, toGet := range itemsToGet {
			v, err := agg.RenderExpression(toGet.E)
			if err != nil {
				return nil, err
			}
			base.Debug("will call ", v, " as ", i)
			outRow[i] = v

			if toGet.addableToRow {
				_, ok := selTbl.UsedFields[toGet.as]
				base.Debug("lets add ", i, "val", v, ". Is used?", ok)
				if ok {
					agg.TokenRow["1Select."+toGet.as] = v
				}
			}
		}
		return outRow, nil
	}

	builder.SrcTables["1Select"] = &selTbl

	return r, rAgg, colNames, nil
}

func selRemoveNamedItemsTable(sourceTables base.SrcTables) {
	delete(sourceTables, "1Select")
}
