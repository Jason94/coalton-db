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
