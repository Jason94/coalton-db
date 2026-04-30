(cl:in-package :cl-user)
(defpackage :coalton-db/sqlite
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/core)
  (:local-nicknames
   (:sl #:sqlite))
  (:export
   ;;; Library Public

   #:SqliteConnection
   #:connect-sqlite!
   #:disconnect-sqlite!

   ;;; Library Private
   ))

(in-package :coalton-db/sqlite)

(named-readtables:in-readtable coalton:coalton)

(coalton-toplevel
  (repr :native sl:sqlite-handle)
  (define-type SqliteConnection)

  (declare connect-sqlite! (String -> SqliteConnection))
  (define (connect-sqlite! connection-spec)
    (lisp (-> :a) (connection-spec)
      (sl:connect connection-spec)))

  (declare disconnect-sqlite! (SqliteConnection -> Void))
  (define (disconnect-sqlite! connection)
    (lisp (-> :a) (connection)
      (sl:disconnect connection))
    (values))

  (declare norm-sqlite-types (SqlValue -> SqlValue))
  (define (norm-sqlite-types val)
    "Handle boolean values."
    (match val
      ((SqlBool b)
       (SqlText
        (if b "TRUE" "FALSE")))
      (_ val)))

  (define-instance (DatabaseAdapter SqliteConnection)
    (define (next-placeholder _ _)
      "?")
    (define (auto-increment-syntax _)
      (AutoIncrementSyntax "" "AUTOINCREMENT"))
    (define (run-query! cnxn (SqlQuery sql params))
      (let normed-params = (map norm-sqlite-types params))
      (lisp (-> :x) (cnxn sql normed-params)
        (cl:handler-case
            (cl:let* ((unwrapped-params (cl:mapcar #'unwrap-sql-value normed-params))
                      (rows (cl:apply
                             #'sl:execute-to-list
                             (cl:cons cnxn (cl:cons sql unwrapped-params)))))
              (Ok (cl:mapcar
                   (cl:lambda (row)
                     (cl:mapcar #'wrap-raw-sql-value row))
                   rows)))
          (cl:error (e)
            (Err (QueryError (cl:format cl:nil "~a" e)))))))))
