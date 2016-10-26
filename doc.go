/*
NoDB: SQL Array comprehensions in GoLang.

JS has Filter, Map, Sort, and Reduce, but GoLang has more:
- Filter: rich WHERE clause syntax including custom functions
- Map: SELECT transformations and subselections including JOINs
- Sort: ORDER BY with multiple parameters (THEN BY) and DESCending !
- Reduce: WHERE with SELECT does it

With this library,
- any struct is a table definition (its public members)
- any slice of a struct is a table itself
- Joins, arbitrary functions, rich WHERE filters and aggregations are possible.

The result is a slice of any struct, and it's appended-to in a best-effort way
based on name (either the columns or the result of an AS statement).

Capitalization is freeform, but recommended to be SQL-style (keywords capitalized, vars not).
Example:

  type inputA struct {X int, Y string, Z float}
  type inputB struct {A int, B string}

  var result = []struct{ M string, N int } {}
  nodb.Do("SELECT z * 2 AS n, upper(b) AS m " +
          "FROM t0 JOIN t1 on X=t1.A "+
          "WHERE Y='foo' "+
          "ORDER BY x DESC, z", result,
    nodb.Obj{
      "t0": []inputA{ {0, "foo", 0.1}, {1, "bar", 0.2}, {2, "foo", 0.3}, {3, "foo", 0.4}},
      "t1": []inputB{ {0, "baa"}, {2, "have"}, {3, "wool"}},
      "upper": strings.ToUpper,
  })
  //result ==  [ {0.6 WOOL} {0.8 WOOL} {0.2 BAA} ]

Note: If inputA or inputB had pointer members the pointer only would be copied.
If inputA or inputB contained structs with private members (time.Time) non-pointer copies will error.

An error state does not guarantee an empty result slice.
See the unit tests for more examples.
The goal is to increase ANSI SQL compatibility as much as anyone today would use.
Then the features will grow around making it more applicable.
*/
package nodb
