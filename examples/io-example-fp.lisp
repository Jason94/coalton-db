(cl:in-package :cl-user)
(defpackage :coalton-db/examples/io-fp
  (:use
   #:coalton
   #:coalton-prelude
   #:simple-io/io
   #:simple-io/term
   #:coalton-db/util
   #:coalton-db/core
   #:coalton-db/queries
   #:coalton-db/db-m
   #:coalton-db/api-fp
   #:coalton-db/sqlite)
  (:local-nicknames
   (:u #:simple-io/unique)
   (:s #:coalton-library/string)))
(in-package :coalton-db/examples/io-fp)

(named-readtables:in-readtable coalton:coalton)

(coalton-toplevel
  (define create-user-table
    (CreateTable "users" ()
                 (("id" IntType PrimaryKey)
                  ("name" TextType Unique)
                  ("age" IntType Nullable))))

  (declare parse-age (String -> Result String (Optional Integer)))
  (define (parse-age str)
    (if (== str "")
        (Ok None)
        (match (s:parse-int str)
          ((Some i)
           (Ok (Some i)))
          ((None)
           (Err "Could not parse age.")))))

  (declare insert-tables (Unit -> DBM IO Unit))
  (define (insert-tables)
    (do
     (write-line "Name? (Required)")
     (name <- read-line)
     (write-line "Age? (Optional)")
     (age <- (map parse-age read-line))
     (match age
       ((Err e)
        (write-line e))
       ((Ok age)
        (do
         (id <- (map u:to-int u:new-unique))
         (result <- (query-rows
                     (Insert (IntoTable "users")
                             (Values id name age)
                             (Cols "id" "name" "age"))))
         (match result
           ((Err e)
            (write-line (<> "Error saving user: "
                            (force-string e))))
           ((Ok _)
            (write-line "Successfully saved user in the database."))))))
     (write-line "Continue? (Y/N)")
     (continue <- (map (== "Y") read-line))
     (if continue
         (insert-tables)
         (pure Unit))))

  (declare get-tables (DBM IO (DbResult (List Row))))
  (define get-tables
    (query-rows (Select AllCols (From "users"))))

  (declare main (IO Unit))
  (define main
    (do
     (cnxn <- (wrap-io (connect-sqlite! "database.db")))
     (run-dbm! cnxn
      (do
       (write-line "Creating user table...")
       (query-rows create-user-table)
       (insert-tables)
       (result <- get-tables)
       (match result
         ((Err e)
          (write-line (<> "Error getting tables: "
                          (force-string e))))
         ((Ok tables)
          (do
           (write-line "Tables:")
           (lift
            (traverse
             (compose write-line force-string)
             tables))
           (pure Unit))))))
      (wrap-io (disconnect-sqlite! cnxn)))))

(cl:defun run-main ()
  (coalton (run! main)))
