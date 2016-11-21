package nodb

import (
	"database/sql"
	"testing"

	"github.com/jmoiron/sqlx"
	. "github.com/smartystreets/goconvey/convey"
)

type Foo2 struct {
	A int    `db:"A"`
	B string `db:"B"`
}

func Test_EasiestDB(t *testing.T) {
	src := []Foo2{{1, "hello"}, {2, "world"}}
	Add("src", src)
	Convey("Get All SQLX", t, func() {
		conn := sqlx.MustConnect("nodb", "cache")
		var results []Foo2
		err := conn.Select(&results, "SELECT * FROM src")
		So(err, ShouldBeNil)
		So(results, ShouldResemble, src)
	})
	Convey("Get All2", t, func() {
		conn, err := sql.Open("nodb", "cache")
		So(err, ShouldBeNil)
		rows, err := conn.Query("SELECT * FROM src")
		So(err, ShouldBeNil)
		So(rows.Next(), ShouldBeTrue)
		c, err := rows.Columns()
		So(err, ShouldBeNil)
		So(c, ShouldResemble, []string{"A", "B"})
		var A int
		var B string
		So(rows.Scan(&A, &B), ShouldBeNil)
		So(A, ShouldEqual, 1)
		So(B, ShouldEqual, "hello")
		So(rows.Next(), ShouldBeTrue)
		So(rows.Scan(&A, &B), ShouldBeNil)
		So(A, ShouldEqual, 2)
		So(B, ShouldEqual, "world")
		So(rows.Next(), ShouldBeFalse)

	})
}
