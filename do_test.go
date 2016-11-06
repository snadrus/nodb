package nodb

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

type Foo struct {
	A int
	B string
}

type HasPri struct {
	A int
	b string
}

type onlyA struct {
	A int
}

func Test_Easiest(t *testing.T) {
	src := []Foo{{1, "hello"}, {2, "world"}}
	Convey("get all", t, func() {
		results := []Foo{}
		So(Do("SELECT * FROM src", &results, Obj{"src": src}), ShouldBeNil)
		So(results, ShouldResemble, src)
	})
	Convey("get all", t, func() {
		results := []Foo{}
		So(Do("SELECT A FROM src", &results, Obj{"src": src}), ShouldBeNil)
		So(results, ShouldResemble, []Foo{{A: 1}, {A: 2}})
	})
	Convey("partial", t, func() {
		results := []onlyA{}
		So(Do("SELECT * FROM src", &results, Obj{"src": src}), ShouldBeNil)
		So(results, ShouldResemble, []onlyA{{1}, {2}})
	})
}

type ADC struct {
	A int
	D string
	C int
}

func Test_SomeMath(t *testing.T) {

	src := []Foo{{A: 1, B: "hello"}, {A: 2, B: "world"}, {A: 3, B: "Earth"}}
	Convey("get all", t, func() {
		results := []ADC{}
		So(Do("SELECT A, A*3+1-A AS C, B as D FROM src WHERE A > 0.25 * 4 ORDER BY A",
			&results,
			Obj{"src": src},
		), ShouldBeNil)
		So(results, ShouldResemble, []ADC{{2, "world", 5}, {3, "Earth", 7}})
	})

	src2 := []ADC{{A: 1, D: "hello", C: 0}, {A: 2, D: "world", C: 5}, {A: 3, D: "Earth", C: 2}}
	Convey("flip terms", t, func() {
		results := []ADC{}
		So(Do("SELECT C as A, D, A as C FROM src WHERE A > 0.25 * 4 AND C < A",
			&results,
			Obj{"src": src2},
		), ShouldBeNil)
		So(results, ShouldResemble, []ADC{{A: 2, D: "Earth", C: 3}})
	})
}

func Test_complex(t *testing.T) {

	Convey("Complex inputs", t, func() {
		src := []time.Time{time.Now(), time.Now()}
		results := []time.Time{}
		err := Do("SELECT * FROM src", &results, Obj{"src": src})
		So(err.Error(), ShouldResemble, "DoSelect error: SELECT * FROM src fails for private fields. Wrap src's struct in another struct")
	})
}

func Test_WhereEasy(t *testing.T) {
	src := []Foo{{1, "hello"}, {2, "world"}, {3, "hello"}}

	Convey("Simple Condition", t, func() {
		results := []Foo{}
		err := Do("SELECT * FROM src WHERE A > 1", &results, Obj{"src": src})
		So(err, ShouldBeNil)
		So(results, ShouldResemble, []Foo{{2, "world"}, {3, "hello"}})
	})
}
func Test_WhereMath(t *testing.T) {
	src := []Foo{{1, "hello"}, {2, "world"}, {3, "hello"}}

	Convey("Simple Math", t, func() {
		results := []Foo{}
		err := Do("SELECT * FROM src WHERE A = 4 - A", &results, Obj{"src": src})
		So(err, ShouldBeNil)
		So(results, ShouldResemble, []Foo{{2, "world"}})
	})
}

var srcX = []Foo{{1, "hello"}, {2, "world"}, {3, "hello"}, {4, "world"}, {5, "hello"}}

func Test_WherePrecedence(t *testing.T) {

	Convey("NOT-AND-OR Precedence1", t, func() {
		results := []Foo{}
		err := Do("SELECT * FROM src WHERE NOT A > 3  AND B='world' OR A=5", &results, Obj{"src": srcX})
		So(err, ShouldBeNil)
		So(results, ShouldResemble, []Foo{{2, "world"}, {5, "hello"}})
	})
}
func Test_WherePrecedence2(t *testing.T) {

	Convey("NOT-AND-OR Precedence2", t, func() {
		results := []Foo{}
		err := Do("SELECT * FROM src WHERE A >= 1 AND A<3 OR B='world'", &results, Obj{"src": srcX})
		So(err, ShouldBeNil)
		So(results, ShouldResemble, []Foo{{1, "hello"}, {2, "world"}, {4, "world"}})
	})
}
func Test_WherePrecedence3(t *testing.T) {

	Convey("NOT-AND-OR Precedence3", t, func() {
		results := []Foo{}
		err := Do("SELECT * FROM src WHERE B='world' OR NOT A > 1 AND A<3 ", &results, Obj{"src": srcX})
		So(err, ShouldBeNil)
		So(results, ShouldResemble, []Foo{{1, "hello"}, {2, "world"}, {4, "world"}})
	})
}

var srcG = []Foo{{1, "hello"}, {2, "world"}, {3, "hello"}, {4, "world"}, {5, "hello"}}

func Test_GroupBy(t *testing.T) {

	Convey("basic GROUPBY", t, func() {
		results := []Foo{}
		err := Do("SELECT * FROM src GROUP BY B ORDER BY A", &results, Obj{"src": srcG})
		So(err, ShouldBeNil)
		So(results, ShouldResemble, []Foo{{1, "hello"}, {2, "world"}})
	})
}
func Test_GroupBy2(t *testing.T) {

	Convey("basics without GROUPBY", t, func() {
		results := []Foo{}
		err := Do("SELECT SUM(A) AS A FROM src", &results, Obj{"src": srcG})
		So(err, ShouldBeNil)
		So(results, ShouldResemble, []Foo{{15, ""}})
	})
}
func Test_GroupBy3(t *testing.T) {

	Convey("SUM+GROUP", t, func() {
		results := []Foo{}
		err := Do("SELECT SUM(A) AS A, B FROM src GROUP BY B ORDER BY A", &results, Obj{"src": srcG})
		So(err, ShouldBeNil)
		So(results, ShouldResemble, []Foo{{6, "world"}, {9, "hello"}})
	})
}
func Test_GroupBy4(t *testing.T) {

	Convey("basic HAVING", t, func() {
		results := []Foo{}
		err := Do("SELECT SUM(A) AS A, B FROM src GROUP BY B HAVING A < 7", &results, Obj{"src": srcG})
		So(err, ShouldBeNil)
		So(results, ShouldResemble, []Foo{{6, "world"}})
	})
}

type rs struct {
	Ct  int
	Max float64
	Min float64
	Avg float64
}

func Test_GroupBy5(t *testing.T) {
	res := []rs{}
	Convey("basic aggregates", t, func() {
		err := Do("SELECT COUNT(*) AS ct, MAX(A) AS max, MIN(A) as min, AVG(a) as avg FROM src", &res, Obj{"src": srcG})
		So(err, ShouldBeNil)
		So(res, ShouldResemble, []rs{{5, 5.0, 1.0, 3.0}})
	})
}

func Test_lateadd(t *testing.T) {
	Convey("lateAddIgnoredByIterator", t, func() {
		res := []int{}
		b := []int{1, 2, 3, 4}
		for _, a := range b {
			if a == 3 {
				b = append(b, 5, 6)
			}
			res = append(res, a)
		}
		So(res, ShouldResemble, []int{1, 2, 3, 4})
	})
}

var left = []Foo{{1, "A"}, {2, "B"}, {3, "C"}}
var right = []Foo{{2, "X"}, {3, "Y"}, {4, "Z"}}

func Test_join(t *testing.T) {
	Convey("inner", t, func() {
		result := []Foo{}
		So(Do("SELECT a FROM first JOIN second ON first.A=second.A",
			&result,
			Obj{"first": left, "second": right}), ShouldBeNil)
		So(result, ShouldResemble, []Foo{{2, ""}, {3, ""}})
	})
	Convey("left", t, func() {
		result := []Foo{}
		So(Do("SELECT a, second.b as b FROM first LEFT JOIN second ON first.A=second.A",
			&result,
			Obj{"first": left, "second": right}), ShouldBeNil)
		So(result, ShouldResemble, []Foo{{1, ""}, {2, "X"}, {3, "Y"}})
	})
	Convey("right", t, func() {
		result := []Foo{}
		So(Do("SELECT second.A as a, first.b as b FROM first RIGHT JOIN second ON first.A=second.A",
			&result,
			Obj{"first": left, "second": right}), ShouldBeNil)
		So(result, ShouldResemble, []Foo{{2, "B"}, {3, "C"}, {4, ""}})
	})
}

func Test_Bools1(t *testing.T) {
	Convey("in/not-in", t, func() {
		res := []Foo{}
		So(Do("SELECT * FROM srcG WHERE A IN (1, 3, 5) AND A NOT IN (1,2)",
			&res,
			Obj{"srcG": srcG}), ShouldBeNil)
		So(res, ShouldResemble, []Foo{{3, "hello"}, {5, "hello"}})
	})
}
func Test_Bools2(t *testing.T) {
	Convey("like", t, func() {
		res := []Foo{}
		So(Do("SELECT * FROM srcG WHERE B LIKE 'he%'",
			&res,
			Obj{"srcG": srcG}), ShouldBeNil)
		So(res, ShouldResemble, []Foo{{1, "hello"}, {3, "hello"}, {5, "hello"}})
	})
}

func Test_Func1(t *testing.T) {
	Convey("function all", t, func() {
		res := []Foo{}
		So(Do("SELECT A, SUBSTR(UPPER(B), 1, 3) AS B FROM srcG WHERE A = 1",
			&res,
			Obj{"srcG": srcG}), ShouldBeNil)
		So(res, ShouldResemble, []Foo{{1, "ELL"}})
	})
}
func Test_Filter(t *testing.T) {
	Convey("filter", t, func() {
		res := []Foo{}
		So(Filter("A = 1", &res, srcG), ShouldBeNil)
		So(res, ShouldResemble, []Foo{{1, "hello"}})
	})
}
