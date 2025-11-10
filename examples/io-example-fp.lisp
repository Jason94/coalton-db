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
   #:coalton-db/sqlite
   #:coalton-library/experimental/do-control-core
   #:coalton-library/experimental/do-control-loops
   )
  (:local-nicknames
   (:u #:simple-io/unique)
   (:s #:coalton-library/string)))
(in-package :coalton-db/examples/io-fp)

(named-readtables:in-readtable coalton:coalton)

;;;
;;; This example implements a simple database program in the pure functional style.
;;; The program uses the SQL Query API, but doesn't use the FRM.
;;;
;;; The database keeps track of users with a name and an age. The user is prompted
;;; at the terminal to enter data, which is stored in the database. When the user
;;; is done, the table is retrieved and printed back to the terminal.
;;;
;;; Usage:
;;;
;;;   CL-USER> (asdf:load-system "coalton-db/examples")
;;;   CL-USER> (in-package :coalton-db/examples/io-fp)
;;;   COALTON-DB/EXAMPLES/IO-FP> (run-main)
;;;

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
    (do-loop-while
     (write-line "Name? (Required)")
     (name <- read-line)
     (write-line "Age? (Optional)")
     (age <- (map parse-age read-line))
     (do-match age
       ((Err e)
        (write-line e))
       ((Ok age)
        (id <- (map u:to-int u:new-unique))
        (result <- (execute-query
                    (Insert (IntoTable "users")
                            (Values id name age)
                            (Cols "id" "name" "age"))))
        (match result
          ((Err e)
           (write-line (<> "Error saving user: "
                           (force-string e))))
          ((Ok _)
           (write-line "Successfully saved user in the database.")))))
     (write-line "Continue? (Y/N)")
     (input <- read-line)
     (pure (== "Y" input))))

  (declare get-tables (DBM IO (DbResult (List Row))))
  (define get-tables
    (query-sql-rows (Select AllCols (From "users"))))

  (declare main (IO Unit))
  (define main
    (do
     (cnxn <- (wrap-io (connect-sqlite! "database.db")))
     (run-dbm! cnxn
      (do
       (write-line "Creating user table...")
       (execute-query create-user-table)
       (insert-tables)
       (result <- get-tables)
       (do-match result
         ((Err e)
          (write-line (<> "Error getting tables: "
                          (force-string e))))
         ((Ok tables)
          (write-line "Tables:")
          (do-foreach-io (t tables)
            (write-line (force-string t)))))))
      (wrap-io (disconnect-sqlite! cnxn)))))

(cl:defun run-main ()
  (coalton (run! main)))
