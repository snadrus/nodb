package nodb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"

	"github.com/kr/pretty"
	"github.com/snadrus/nodb/internal/base"
	"github.com/snadrus/nodb/internal/sel"
	"github.com/xwb1989/sqlparser"
)

type NoDBDriver struct{}

func (n NoDBDriver) Open(s string) (driver.Conn, error) {
	if strings.ToLower(s) != "cache" {
		return nil, errors.New("Unsupported")
	}

	ctx, cancel := context.WithCancel(context.Background())
	return Conn{
		Context: ctx,
		Cancel:  cancel,
	}, nil
}

type Conn struct {
	context.Context
	Cancel context.CancelFunc
}

func (c Conn) Begin() (driver.Tx, error) {
	return Tx{}, nil
}

type Tx struct{}

func (t Tx) Commit() error   { return nil }
func (t Tx) Rollback() error { return nil }

func (c Conn) Close() error {
	c.Cancel()
	return nil
}
func (c Conn) Prepare(s string) (driver.Stmt, error) {
	ctx, cancel := context.WithCancel(c.Context)
	return Stmt{
		Context: ctx,
		Cancel:  cancel,
		S:       s,
	}, nil
}

type Stmt struct {
	context.Context
	Cancel context.CancelFunc
	S      string
}

func (s Stmt) Close() error {
	s.Cancel()
	return nil
}

func (s Stmt) NumInput() int {
	return -1 // Default for We Don't Know
}

func (s Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("Exec not supported for Read-only system")
}

func (s Stmt) Query(args []driver.Value) (driver.Rows, error) {
	if len(args) != 0 {
		return nil, errors.New("Cannot take prepared statements yet, TODO")
	}
	tree, err := sqlparser.Parse(s.S)
	if err != nil {
		return nil, err
	}

	base.Debug(pretty.Sprint(tree))

	switch tree.(type) {
	case *sqlparser.Select:
		return sel.DoAry(tree.(*sqlparser.Select), sel.Obj(cache), s.Context)
	default:
		return nil, fmt.Errorf("Query type not supported")
	}
}

// TODO add locking
var cache Obj

// Add a table ([]struct) or function to the database
func Add(key string, item interface{}) {
	cache[key] = item
}

// Delete a user-added item from the database
func Delete(key string) {
	delete(cache, key)
}

func init() {
	cache = make(Obj)
	sql.Register("nodb", &NoDBDriver{})
}
