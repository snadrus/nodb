<img src="(http://snadrus.github.io/logo-nodb.png" width="400">
# nodb
SQL array comprehensions in Go. [![GoDoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](http://godoc.org/github.com/snadrus/nodb)        [![Build Status](http://img.shields.io/travis/snadrus/nodb.svg?style=flat-square)](https://travis-ci.org/snadrus/nodb)     [![Coverage Status](https://coveralls.io/repos/github/snadrus/nodb/badge.svg?branch=master)](https://coveralls.io/github/snadrus/nodb?branch=master)    [![Donate](https://www.paypalobjects.com/en_US/i/btn/btn_donate_SM.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=C6284X93YL4WA)

Featuring:
- faster development than in-memory DBs: no table creation, inserts, IPC, & de/serialization
  *  Saving dev time & run time.

- Your table rows (structs) can contain time.Time & other structs with privates
  *  And you can use your own functions on them
- Sort: "ORDER BY X, Y DESC, Z" is a dream vs Go's sort API
- Think in 1 manipulation language for all your data needs

 Uses:
- No joins policy or Cassandra?
  *  Don't lose your rich API, do it locally.
- Sort By X Then By Y?
  * Yes, it works right & uses Go's sort.
- Complex Operations?
  * Express them easily in the most recognized query language.
- Caching some data?
  * And want to join your cache in a sql call? Here's your chance.
- Building own DB?
  * Use this front-end and just provide ingestion & locking.

 API:
    It's simple, 1 function only:

    nodb.Do("SQL STATEMENT", &resultSliceOfAnyStruct,
     nodb.Obj{"table1": sliceOfAnyStruct, "table2": sliceOfAnyStruct, "unixToTime": time.Unix})

     If no error, the results' structs will contain shallow-copies of their elements.
- Why are results not saved?
  * Results reach the destination by name. Use an "AS" to select the destination field.

FAQ:
- Dual License
  * Select either GPL or purchase a commercial license.
- How compatible?
  * The parts of ANSI SQL most used. Queries execute right or will error. Case-insensitive query of public members. See TODOs for most serious omissions
- How extensible?
  * Add functions per query Obj or globally (expr.FuncMap) use them anywhere in the query.
- How can I help?
  * Open a bug in bitbucket.org/snadrus/nodb and send a merge request.
- Types?
  * Are GoLang types. Use Golang time & pass-in functions:
      func Hour(t time.Time) int { return t.Hour() }
- Where did this come from?
  * It's the personal efforts of Andrew Jackson who also had the idea.
- How fast really?
  * It is in-memory & pipelined for multicore, but Go loops are faster today. It's focussed on correctness first, query planning second.
- Out of Memory?
  * Object copying isn't light & neither is GROUPBY & ORDERBY.

Design: (the first?) 100% Go SQL engine
  External libs for SQL parsing and interface{} evaluating.
  Native GoLang libs for all else: reflect, sort, template (functions)
  Process:
  - Rich "Rows" are formed of all joined, renamed, calculated data for a source row
  - Processing occurs on these.
  - SELECT rows (just requested data) are formed.
  - The SELECT rows are mapped back to the destination by name
  Closures are the greatest! The setups return functions that have context.

TODO:
- Find & Fix TODOs in the code.
- Code coverage
- Right JOINs (just flip 'em)

- DISTINCT not implemented. It is a group-by with all. Code-savings. BUT select needs to say which it uses. Use new ExpressionBuilder returns & unify that for PLAN.

- NULL (nil) support is wonky at best. Avoid if possible.
- LIMIT  support
- UNION  support
- []non-struct Query or Return
- Subquries
- Functions on objects: Hour(t) --> t.Hour()

- OPTIMIZATION: index idea: zeroed index will represent position offset to real table
  values, so all zeroes is "this order". Then we are free to sort using govaluate
  to build an index on-the-fly without painful/risky reordering.
  Indexes are great for groupby fields: walking them in sorted order means no memalloc

- OPTIMIZATION: expr needs return a rollup of what tables a subexpression uses.
  - Inv of constant. Use to determine if needs re-eval
  - Aides planner in slicing-up processing
  - OPTIMIZATION: pre-determine the table rows that match the WHERE query.

- Time: range queries and gt/lt on time objects. also needs way to static-define a time object (like a time function) --> more MySQL time functions

- Use a mmap implementation for JOIN/GROUP intermediates, especially for channels

- Option to front Cassandra, or even MySQL (JoinAssistant).

- Chan interface{} --> different FROM fork, different PLAN fork

- Planner v2: run short-circuit expr, then recurse.
  Medium: just left-align those with indexes. leftist for 1 channel

- Joiner: hard-version:
  - if you're an inner loop, consider marking those you skip
  - if unsorted & "equals" join, map or sort

Has easy workarounds:
  - Right JOINs: Move Right to left (do first)
  - Single Return (item or struct)
      worthless unless a different interaction model was in plan
  - Item as table
