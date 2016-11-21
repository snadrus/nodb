# <img src="http://snadrus.github.io/logo-nodb.png" width="400">
SQL array comprehensions in Go. [![GoDoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](http://godoc.org/github.com/snadrus/nodb)        [![Build Status](http://img.shields.io/travis/snadrus/nodb.svg?style=flat-square)](https://travis-ci.org/snadrus/nodb)     [![Coverage Status](https://coveralls.io/repos/github/snadrus/nodb/badge.svg?branch=master)](https://coveralls.io/github/snadrus/nodb?branch=master)    [![Donate](https://www.paypalobjects.com/en_US/i/btn/btn_donate_SM.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=C6284X93YL4WA)

- Great for complex in-memory caches, or in-memory joins across various datastores
- Development faster than in-memory DBs: no table creation, inserts, IPC, & de/serialization
- Sort: "ORDER BY last_name, importance DESC" is a dream vs Go's sort API
- Every struct is a table row. A slice of them is a table.
- Pass in & use any func. time.Time is your time format.

DB Driver method:

  import "github.com/snadrus/nodb"
  ...
  // Once:
  nodb.Add("myTable", []ABC{{1, "hello", 1.0}, {2, "db", 2.0}, {3, "world", 3.0}})  
  connection := sqlx.MustConnect("nodb", "cache")         

  // As-Needed:           
  var results []ABC
  err := connection.Select(&results, "SELECT * FROM myTable WHERE C > 1.5")
  fmt.Println(results) // []ABC{{2, "db", 2.0}, {3, "world", 3.0}}

Only 1 function! Example:

     err := nodb.Do(
       "SELECT *, salutation(gender, last_name, lang) as greeting" +
       "FROM user JOIN company AS t2 ON user.company_id = t2.id" +
       "WHERE company.size = 'medium' " +
       "ORDER BY company.name",
       &resultSliceOfAnyStruct,
       nodb.Obj{
          "user": userCache,     // A slice of any struct
          "company": companies, // A slice of any struct
          "salutation": mySalutationFunc,    // Pass string-returning functions
          }
     )

     If no error, the results' structs will contain shallow-copies of their elements.


Benefitting:
- no-Join policy scenarios (like Cassandra)
- SQL in caches
- Building your own DB.

FAQ:
- Dual License
  * Select either GPL or purchase a commercial license.
- Why are nodb.Do results not saved?
  * Results reach the destination by name. Use an "AS" to select the destination field.
- How compatible?
  * Common parts of ANSI SQL. It's right or will error. Case-insensitive query of public members. See TODOs for omissions
- How extensible?
  * Add functions per query Obj & use them anywhere in the query.
- How can I help?
  * Open a bug in github.org/snadrus/nodb and send a merge request.
- Types?
  * Are GoLang types. Use Golang time & pass-in functions:
      func Hour(t time.Time) int { return t.Hour() }
- Where did this come from?
  * It's the personal efforts of Andrew Jackson who also had the idea.
- How fast really?
  * It is in-memory & pipelined for multicore. Correctness first, query planning second.
- Memory Gotchas:
  * Object copying isn't light. GROUPBY is even heavier.

Design: (the first?) 100% Go SQL engine
  Go libs for SQL parsing and interface{} evaluating.
  Native GoLang libs for all else: reflect, sort, template (functions)
  Process:
  - Rich "Rows" are formed of all joined, renamed, calculated data for a source row
  - Processing occurs on these.
  - SELECT rows (just requested data) are formed.
  - The SELECT rows are mapped back to the destination by name
  Closures are the greatest! The setups return functions that have context.

Lacking, but has easy workarounds:
    - Inputs other than []struct
    - Union: Just run the 1st query, then the 2nd.

TODO:
- TODOs in the code.

- NULL (nil) support is wonky at best. Avoid if possible.

- LIMIT support ==> with CTX, should be easy

- DISTINCT not implemented. It is mmaped hashes.

- Subquries
-- Chan interface{} --> different FROM fork, different PLAN fork

- DB Proxy (Cassandra or a variety at once)
-- SubQueries + mmap for JOIN/GROUP intermediaries

- Functions on objects: Hour(t) --> t.Hour()

- OPTIMIZATION: expr.E fault tolerance (error swallowing) for OR/AND clauses allowing Per-table elimination of rows without all data available:  A AND B => err AND False ==> False.  A OR B ==> err OR True ==> True.
-- THEN: make errored expr.E returns include partial eval.

- OPTIMIZATION: RowMap["1Prototype"] pointing to previous map. Reduces join effort.
    BETTER: have a .GetValue("name") function that works on all the various types.

- OPTIMIZATION: index idea: zeroed index will represent position offset to real table
  values, so all zeroes is "this order". Then we are free to sort using govaluate
  to build an index on-the-fly without painful/risky reordering.
  Indexes are great for groupby fields: walking them in sorted order means no memalloc

- OPTIMIZATION: expr needs return a rollup of what tables a subexpression uses.
  - Inv of constant. Use to determine if needs re-eval
  - Aides planner in slicing-up processing
  - OPTIMIZATION: pre-determine the table rows that match the WHERE query.

- Time: range queries and gt/lt on time objects. also needs way to static-define a time object (like a time function) --> more MySQL time functions

- Planner v2: run short-circuit expr, then recurse.
  Medium: just left-align those with indexes. leftist for 1 channel

- Joiner: hard-version:
  - if you're an inner loop, consider marking those you skip
  - if unsorted & "equals" join, map or sort
