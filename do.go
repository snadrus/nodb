package nodb

import (
	"fmt"
	"strconv"

	"strings"

	"reflect"

	"time"

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
	fmt.Println(query)
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

func EnableLogging() {
	base.Debug = fmt.Println
}

// Inline SQL and argument expression, such as:
// Inline(&res,
// 	"SELECT customer.name, customer.phone, COUNT(order.id) AS count FROM ",
// 	customers,
//  " AS customers JOIN ",
//  orders,
//  " AS orders ON orders.customer_id = customers.id",
//  "WHERE ",func(d time.Time)bool{return time.Since(d)< 30*time.Day},"(order.date)" )
func Inline(result interface{}, inlineQuery ...interface{}) error {
	str := []string{}
	obj := Obj{}
	for _, v := range inlineQuery {
		switch v.(type) {
		case string:
			str = append(str, v.(string), " ")
		case int, int32, int64, uint, uint32, uint64, uint8, uint16, float32, float64, int16, int8:
			str = append(str, fmt.Sprint(v), " ")
		case time.Time:
			str = append(str, strconv.FormatInt(v.(time.Time).UnixNano(), 10), " ")
		case time.Duration:
			str = append(str, strconv.FormatInt(int64(v.(time.Duration)), 10), " ")
		default:
			sillyName := ""
			for {
				sillyName = base.GetSillyName()
				if _, ok := obj[sillyName]; !ok { // find a unique one
					break
				}
			}
			obj[sillyName] = v
			str = append(str, sillyName)

			if reflect.TypeOf(v).Kind() != reflect.Func {
				str = append(str, " ")
			}
		}
	}
	return Do(strings.Join(str, ""), result, obj)
}
