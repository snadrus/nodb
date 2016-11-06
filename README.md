# <img src="http://snadrus.github.io/logo-nodb.png" width="400">
SQL array comprehensions in Go. [![GoDoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](http://godoc.org/github.com/snadrus/nodb)        [![Build Status](http://img.shields.io/travis/snadrus/nodb.svg?style=flat-square)](https://travis-ci.org/snadrus/nodb)     [![Coverage Status](https://coveralls.io/repos/github/snadrus/nodb/badge.svg?branch=master)](https://coveralls.io/github/snadrus/nodb?branch=master)    [![Donate](https://www.paypalobjects.com/en_US/i/btn/btn_donate_SM.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=C6284X93YL4WA)

- Development faster than in-memory DBs: no table creation, inserts, IPC, & de/serialization
- Sort: "ORDER BY last_name, importance DESC" is a dream vs Go's sort API
- Every struct is a table row. A slice of them is a table.
- Pass in & use any func. time.Time is your time format.

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
- When you know the SQL solution
- queries against in-memory caches
- Building your own DB.

FAQ:
- Dual License
  * Select either GPL or purchase a commercial license.
- Why are results not saved?
  * Results reach the destination by name. Use an "AS" to select the destination field.
- How compatible?
  * Common parts of ANSI SQL. Queries execute right or return error. Case-insensitive query of public members. See TODOs for most serious omissions
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
  Go libs for SQL parsing and interface{} evaluating.
  Native GoLang libs for all else: reflect, sort, template (functions)
  Process:
  - Rich "Rows" are formed of all joined, renamed, calculated data for a source row
  - Processing occurs on these.
  - SELECT rows (just requested data) are formed.
  - The SELECT rows are mapped back to the destination by name
  Closures are the greatest! The setups return functions that have context.

Lacking, but has easy workarounds:
    - Single Return (item or struct)
        worthless unless a different interaction model was in plan
    - Item as table
    - []non-struct Query or Return
    - Union: Just run the 1st query, then the 2nd.

TODO:
- Find & Fix TODOs in the code.
- Code coverage > 80%

- DISTINCT not implemented. It is a group-by with all. Code-savings. BUT select needs to say which it uses. Use new ExpressionBuilder returns & unify that for PLAN.

- NULL (nil) support is wonky at best. Avoid if possible.
- LIMIT support
- Subquries
- Functions on objects: Hour(t) --> t.Hour()

- OPTIMIZATION: expr.E fault tolerance for OR/AND clauses allowing Per-table elimination of rows without all data available:  A AND B => err AND False ==> False.  A OR B ==> err OR True ==> True.

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
