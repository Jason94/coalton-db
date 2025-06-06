# Installation

`coalton-db` currently has two requirements that are not on Quicklisp. Additionally, `coalton-db` itself is not yet on Quicklisp.

First, the library relies on a branch of the Coalton compiler providing monad transformers to the standard library which has not been merged yet. Second, it relies on a small library providing a simple IO type. You can install both of them and `coalton-db` by running:

```bash
git clone -b monad-freet https://github.com/Jason94/coalton.git ~/quicklisp/local-projects/coalton
git clone https://github.com/Jason94/coalton-simple-io.git ~/quicklisp/local-projects/coalton-simple-io
git clone https://github.com/Jason94/coalton-db.git ~/quicklisp/local-projects/coalton-db
```

## Alpha Status

`coalton-db` is *very much* still in alpha status, if that, and is subject to frequent breaking changes. It's also not yet feature complete, and is missing several important features such as modeling relationships and transaction batching/rollback.

### TODOs:

 - [x] Add internal support for query parameterization, instead of hard-coding values
 - [ ] Add tests!
 - [x] Add table constraints. [CompositeUnique and CompositePrimaryKey currently supported]
 - [ ] Add support for db schemas (probably works already? Maybe just note in readme and add a test)
 - [x] Add transaction support.
 - [ ] Add SQL functions to RowCondition
 - [ ] Add remaining SQLite column types
 - [x] Add DEFAULT column property
 - [x] Add full DEFAULT support
 - [ ] Add CHECK column property
 - [x] Add foreign key support
 - [ ] Add joins
 - [ ] Add relationships
 - [ ] Improve the imperative interface. (Lacks proper transaction support)
 - [ ] Add support for other DB's besides SQLite. Probably start with PostgreSQL.
 - [ ] Add index support
 - [ ] Add separate QueryBuilder AST to construct SQL queries, to help users manually run queries
   - Include features like ORDER BY, LIMIT, and aggregates
 - [x] Add query debugging
 - [ ] RunNonQuerySQLs should short-circuit internally. Particularly egregious for (ensure-schema (...) True)!
 - [ ] Disconnect from the DB if the thread crashes (keep it from locking until restarting SLIME)
   
### Smaller Clean Ups / Refactors:

- [ ] Delete HasTableName. Instead we should have a lower level SQL DSL API that takes strings directly.
 
### TOMaybes:

Maybe not good ideas, or maybe lower priority ok ideas:

- [ ] Add fine-grained ForeignKey options (ON DELETE CASCADE, ON UPDATE NO ACTION, etc) (DB specific? Probably?)
- [ ] Add richer ORM style features, like soft deleting and automatic updated-at/created-at cols
- [ ] Support prepared statements
- [ ] Support for schema migrations
- [ ] Either fully support the range of column and table flags, or add a string escape hatch. Not sure how portable flags like "ON CONFLICT REPLACE" would be between DB implementations. (If the SQl is different, that's not too hard a problem to solve. If some flags just aren't supported by some implementations, that's harder.)
- [ ] Add more validity checking to table definitions (not multiple PKeys, etc) (may not be necessary because the DB should do this for us)

# Examples

`coalton-db` allows you to easily define a POCO (Plain Old Coalton Object) and a related SQL table definition:

```lisp
(coalton-toplevel
  (define user-table
    (table
     "User"
     (make-list
      (column "Name" TextType (make-list NotNullable Unique))
      (default-column "Age" IntType))))

  (define-struct User
    (name String)
    (age (Optional Integer)))

  (define-instance (Into User String)
    (define (into user)
      (build-str
       "User: " (.name user))))

  (define-instance (Persistable User)
    (define schema (const user-table))
    (define (to-row user)
      (build-row user
                 ("Name" .name)
                 ("Age" .age)))
    (define (from-row col-vals)
      ;; Note: Here, the label applies to the SQL column, not the POCO constructor.
      ;; The order of fields must match the `User` constructor.
      (parse-row User col-vals
        (parse-text "Name")
        (parse-null "Age" parse-int)))))
```

Then you can query to and from the database using either a functional or imperative style:

```lisp
(coalton-toplevel
  (declare functional-operation (DbProgram SqlLiteConnection (List User)))
  (define functional-operation
    (do
     (ensure-schema (make-list user-table) True)
     (delete-all User)
      (insert-row (User "Steve" None))
      (insert-row (User "Bob" (Some 12)))
      (delete-where User (Eq_ "Name" (SqlText "Bob")))
      (update-where User
                    (m:collect (make-list (Tuple "Name"
                                                 (SqlText "Steven"))))
                    (Eq_ "Name" (SqlText "Steve")))
      (update-all User
                  (m:collect (make-list (Tuple "Age"
                                               (SqlInt 4)))))
      (results <- (select-all User))
      (match results
        ((Ok users)
         (lift (lift (tm:write-line (join-str newline (map to-string users))))))
        ((Err e)
         (lift (lift (tm:write-line (<> "Failed: " (into e)))))))
      (pure results)))

  (define (functional-ex)
    (let cnxn = (connect-sqlite! "test.db"))
    (let result = (run-sqlite! functional-operation cnxn))
    (disconnect-sqlite! cnxn)
    result))
```

```lisp
(coalton-toplevel
  (declare imperitive-ex (Unit -> QueryResult (List User)))
  (define (imperitive-ex)
    (let cnxn = (connect-sqlite! "test.db"))
    (let run-query = (run-with-sqlite-connection! cnxn))
    (run-query (ensure-schema (make-list user-table) False))
    (run-query (delete-all User))
    (run-query (insert-row (User "Steve" None)))
    (run-query (insert-row (User "Bob" (Some 12))))
    (run-query (delete-where User (Eq_ "Name" (SqlText "Bob"))))
    (run-query (update-where User
                             (m:collect (make-list (Tuple "Name"
                                                          (SqlText "Steven"))))
                             (Eq_ "Name" (SqlText "Steve"))))
    (let results = (run-sqlite! (select-all User)
                                cnxn))
    (match results
      ((Ok users)
       (for user in users
         (trace (to-string user))))
      ((Err e)
       (trace (<> "Failed: " (to-string e)))))
    (disconnect-sqlite! cnxn)
    results))
```

# Features

## Default Values

If a row for a `Persistable` instance is inserted, the value for a column will be ommitted if:

(1) The value is `None` (technically, it checks for `SqlNull` after converting to SQL values)
(2) The table in the `coalton-db` definition has a DEFAULT flag on that column. `coalton-db` does not query the SQL schema to look for default columns!

An ommitted column will be defaulted by the database. It does not matter if the database column is nullable or not.
