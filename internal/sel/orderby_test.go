package sel_test

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/snadrus/nodb"
)

type Foo struct {
	A int
	B string
}

var srcG = []Foo{{1, "hello"}, {2, "world"}, {3, "hello"}, {4, "world"}, {5, "hello"}}

func Test_GroupBy3(t *testing.T) {
	Convey("groupby-Number-order", t, func() {
		results := []Foo{}
		err := nodb.Do("SELECT SUM(A) AS A, B FROM src GROUP BY B ORDER BY A", &results, nodb.Obj{"src": srcG})
		So(err, ShouldBeNil)
		So(results, ShouldResemble, []Foo{{6, "world"}, {9, "hello"}})
	})
}

var srcO = []Foo{{3, "hello"}, {21, "world"}, {1, "hello"}, {40, "world"}, {5, "hello"}}

func Test_OrderBy(t *testing.T) {
	Convey("Orderby", t, func() {
		results := []Foo{}
		err := nodb.Do("SELECT * FROM src ORDER BY A", &results, nodb.Obj{"src": srcO})
		So(err, ShouldBeNil)
		So(results, ShouldResemble, []Foo{{A: 1, B: "hello"}, {A: 3, B: "hello"}, {A: 5, B: "hello"}, {A: 21, B: "world"}, {A: 40, B: "world"}})
	})
}
func Test_Desc(t *testing.T) {
	Convey("Orderby", t, func() {
		results := []Foo{}
		err := nodb.Do("SELECT * FROM src ORDER BY A DESC", &results, nodb.Obj{"src": srcO})
		So(err, ShouldBeNil)
		So(results, ShouldResemble, []Foo{{A: 40, B: "world"}, {A: 21, B: "world"}, {A: 5, B: "hello"}, {A: 3, B: "hello"}, {A: 1, B: "hello"}})
	})
}
func Test_GroupByString(t *testing.T) {
	Convey("SUM+GROUP", t, func() {
		results := []Foo{}
		err := nodb.Do("SELECT SUM(A) AS A, B FROM src GROUP BY B ORDER BY B", &results, nodb.Obj{"src": srcG})
		So(err, ShouldBeNil)
		So(results, ShouldResemble, []Foo{{9, "hello"}, {6, "world"}})
	})
}
