(cl:in-package :cl-user)
(defpackage :coalton-db/examples/io-imp
  (:use
   #:coalton
   #:coalton-prelude
   #:simple-io/io
   #:simple-io/term
   #:coalton-db/util
   #:coalton-db/core
   #:coalton-db/queries
   #:coalton-db/db-m
   #:coalton-db/api-imp
   #:coalton-db/sqlite)
  (:local-nicknames
   (:c #:coalton-library/cell)
   (:lp #:coalton-library/experimental/loops)
   (:s #:coalton-library/string)))
(in-package :coalton-db/examples/io-imp)

(named-readtables:in-readtable coalton:coalton)

(coalton-toplevel
  (define *next-id* (c:new 0))

  (declare next-id! (Unit -> Integer))
  (define (next-id!)
    (c:increment! *next-id*))

  (declare read-line! (Unit -> String))
  (define (read-line!)
    (lisp :a ()
      (cl:read-line))))

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

  (declare insert-tables! (DatabaseAdapter :d => :d -> Unit))
  (define (insert-tables! cnxn)
    (print "Name? (Required)")
    (let name = (read-line!))
    (print "Age? (Optional)")
    (let age = (parse-age (read-line!)))
    (match age
      ((Err e)
       (print e))
      ((Ok age)
       (let id = (next-id!))
       (let result =
         (query-rows! cnxn
                      (Insert (IntoTable "users")
                              (Values id name age)
                              (Cols "id" "name" "age"))))
       (match result
         ((Err e)
          (print (<> "Error saving user: " (force-string e))))
         ((Ok _)
          (print "Successfully saving user in the database.")))))
    (print "Continue? (Y/N)")
    (let continue = (== "Y" (read-line!)))
    (if continue
        (insert-tables! cnxn)
        Unit))

  (declare get-tables! (DatabaseAdapter :d => :d -> DbResult (List Row)))
  (define (get-tables! cnxn)
    (query-rows! cnxn (Select AllCols (From "users"))))

  (declare main (Unit -> Unit))
  (define (main)
    (let cnxn = (connect-sqlite! ":memory:"))
    (print "Creating user table...")
    (query-rows! cnxn create-user-table)
    (insert-tables! cnxn)
    (let result = (get-tables! cnxn))
    (match result
      ((Err e)
       (print (<> "Error getting tables: "
                  (force-string e))))
      ((Ok tables)
       (print "Tables:")
       (lp:dolist (t tables)
         (print (force-string t)))))
    (disconnect-sqlite! cnxn)))

(cl:defun run-main ()
  (coalton (main)))
