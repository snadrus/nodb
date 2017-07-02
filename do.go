package nodb

import (
	"fmt"

	"github.com/kr/pretty"
	"github.com/snadrus/nodb/internal/base"
	"github.com/snadrus/nodb/internal/sel"
	"github.com/xwb1989/sqlparser"
)

// Obj is the 3rd parameter to Do.
// Use it to name tables & custom functions
type Obj map[string]interface{}

// Do an SQL 'query' against 'src' entries and (copy) results into 'result'
// See doc.go for more details. src points to tables ([]AnyStruct) and functions
func Do(query string, result interface{}, src Obj) error {
	tree, err := sqlparser.Parse(query)
	if err != nil {
		return err
	}

	base.Debug(pretty.Sprint(tree))

	switch tree.(type) {
	case sqlparser.SelectStatement:
		err := sel.Do(tree.(sqlparser.SelectStatement), result, base.Obj(src))
		return err
	//case *sqlparser.Union:
	//	tree.(*sqlparser.Union).
	default:
		return fmt.Errorf("Query type not supported")
	}
}

// Filter the input using an SQL WHERE content. 1-level-dep copy results.
// Available: AND, OR, go math symbols, (), Struct Field names, single quotes, single equal
// ( ID > 5 ) AND !(Name='Bob')
func Filter(query string, result interface{}, input interface{}) error {
	return Do(fmt.Sprintf("SELECT * FROM t0 WHERE %s", query),
		result, Obj{"t0": input})
}
