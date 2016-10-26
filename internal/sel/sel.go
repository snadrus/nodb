package sel

import (
	"fmt"
	"reflect"

	"github.com/snadrus/nodb/internal/base"
	"github.com/snadrus/nodb/internal/expr"
	"github.com/xwb1989/sqlparser"
)

type rowMaker func(srcRow map[string]interface{}) (outRow map[string]interface{}, e error)
type aggRowMaker func(agg *expr.AggGroup) (outRow map[string]interface{}, e error)

type getInstructions struct {
	E            expr.E
	as           string
	addableToRow bool
}

func doSelect(s sqlparser.SelectExprs, builder *expr.ExpressionBuilder) (rowMaker, aggRowMaker, error) {
	var itemsToGet = []getInstructions{}
	avoidDupe := map[string]bool{}
	selTbl := base.SrcTable{
		Name:       "1Select", //impossible
		Table:      []interface{}{},
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
					return nil, nil, fmt.Errorf("Invalid tablename %s", tname)
				}
				tmpSet = base.SrcTables{string(tname): t0}
			}
			for _, tbl := range tmpSet {
				if tbl.Name == "1Select" {
					continue
				}
				reflect.TypeOf(tbl.Table).Elem().NumField() // ONLY for []STRUCT{}
				if tbl.HasPrivateFields {
					return nil, nil, fmt.Errorf("SELECT * FROM %s fails for private fields. Wrap %s's struct in another struct", tbl.Name, tbl.Name)
				}
				for _, fname := range tbl.Fields {
					if _, ok := avoidDupe[fname]; ok {
						continue
					}
					avoidDupe[fname] = true
					tbl.UsedFields[fname] = true
					func(fullname string) {
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
				return nil, nil, err
			}
			addableToRow := false
			if len(eAs) > 0 { // add to src table for GROUPBY, HAVING, ORDERBY
				selTbl.Fields = append(selTbl.Fields, eAs)
				addableToRow = true
			}
			itemsToGet = append(itemsToGet, getInstructions{
				as:           eAs,
				E:            expE,
				addableToRow: addableToRow,
			})
		}
	}

	r := func(srcRow map[string]interface{}) (outRow map[string]interface{}, e error) {
		outRow = make(map[string]interface{})
		for _, toGet := range itemsToGet {
			v, err := toGet.E(srcRow)
			if err != nil {
				return nil, err
			}
			base.Debug("will call ", v, " as ", toGet.as, " from ", srcRow)
			outRow[toGet.as] = v
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
	rAgg := func(agg *expr.AggGroup) (outRow map[string]interface{}, e error) {
		outRow = make(map[string]interface{})
		for _, toGet := range itemsToGet {
			v, err := agg.RenderExpression(toGet.E)
			if err != nil {
				return nil, err
			}
			base.Debug("will call ", v, " as ", toGet.as)
			outRow[toGet.as] = v

			if toGet.addableToRow {
				_, ok := selTbl.UsedFields[toGet.as]
				base.Debug("lets add ", toGet.as, "val", v, ". Is used?", ok)
				if ok {
					agg.TokenRow["1Select."+toGet.as] = v
				}
			}
		}
		return outRow, nil
	}

	builder.SrcTables["1Select"] = &selTbl

	return r, rAgg, nil
}

func selRemoveNamedItemsTable(sourceTables base.SrcTables) {
	delete(sourceTables, "1Select")
}
