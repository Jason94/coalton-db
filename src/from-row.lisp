(cl:in-package :cl-user)
(defpackage :coalton-db/from-row
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/util
   #:coalton-db/core)
  (:local-nicknames
   )
  (:export
   ;;; Library Public

   #:ParseSql
   #:parse-sql

   #:ParseSqlValue
   #:parse-row
   #:define-row-parser
   #:sql-value-parser

   ;;; Library Private
   ))

(in-package :coalton-db/from-row)

(named-readtables:in-readtable coalton:coalton)

;;;
;;; Parse SQL Value
;;;

(coalton-toplevel
  (define-class (ParseSql :a)
    (parse-sql (SqlValue -> DbResult :a)))

  (define-instance (ParseSql Integer)
    (define (parse-sql val)
      (match val
        ((SqlInt i) (Ok i))
        (_ (Err (ResultParseError
                 (<> (<> "Could not convert " (force-string val))
                     " to an integer.")))))))

  (define-instance (ParseSql String)
    (define (parse-sql val)
      (match val
        ((SqlText i) (Ok i))
        (_ (Err (ResultParseError
                 (<> (<> "Could not convert " (force-string val))
                     " to a string.")))))))

  (define-instance (ParseSql Boolean)
    (define (parse-sql val)
      (match val
        ((SqlBool b) (Ok b))
        ((SqlText s)
         (cond
           ((== s "FALSE") (Ok False))
           ((== s "TRUE") (Ok True))
           (True (Err (ResultParseError
                       (<> (<> "Could not convert " (force-string val))
                           " to a boolean."))))))
        (_ (Err (ResultParseError
                 (<> (<> "Could not convert " (force-string val))
                     " to a boolean.")))))))

  (define-instance (ParseSql :a => ParseSql (Optional :a))
    (define (parse-sql val)
      (match val
        ((SqlNull) (Ok None))
        (_ (map Some (parse-sql val)))))))

;;;
;;; Parse Rows
;;;

(coalton-toplevel

  (repr :transparent)
  (define-type (RowParser :a)
    (RowParser (Row -> DbResult (Tuple :a Row))))

  (inline)
  (declare run-row-parser (RowParser :a -> Row -> DbResult (Tuple :a Row)))
  (define (run-row-parser (RowParser f))
    f)

  (define-instance (Functor RowParser)
    (inline)
    (define (map f (RowParser p))
      (RowParser (fn (input)
                    (match (p input)
                      ((Ok (Tuple a rest))
                       (Ok (Tuple (f a) rest)))
                      ((Err e)
                       (Err e)))))))

  (define-instance (Applicative RowParser)
    (inline)
    (define (pure x)
      (RowParser (fn (input)
                    (Ok (Tuple x input)))))
    (inline)
    (define (liftA2 a->b->c (RowParser pa) (RowParser pb))
      (RowParser (fn (input)
                    (do
                     ((Tuple a rest1) <- (pa input))
                     ((Tuple b rest2) <- (pb rest1))
                     (pure (Tuple (a->b->c a b) rest2)))))))

  (define-class (ParseSqlValue :p)
    (sql-value-parser (RowParser :p)))

  (declare parse-row (ParseSqlValue :a => Row -> DbResult :a))
  (define (parse-row input)
    (do
     ((Tuple result rest) <- (run-row-parser sql-value-parser input))
     (match rest
       ((Nil)
        (Ok result))
       (_
        (Err (ResultParseError "Unexpected SQL values to parse."))))))
  )

(cl:defmacro define-simple-parser (output-type expected-sqlvalue)
  `(define-instance (ParseSqlValue ,output-type)
     (define sql-value-parser
       (RowParser (fn (row)
                    (match row
                      ((Nil)
                       (Err (ResultParseError "Ran out of SQL values to parse.")))
                      ((Cons (,expected-sqlvalue x) rest)
                       (Ok (Tuple x rest)))
                      ((Cons val _)
                       (Err (ResultParseError (build-str "Expected "
                                                         ,(cl:string expected-sqlvalue)
                                                         " received: "
                                                         (force-string val)))))))))))

(cl:defmacro define-row-parser (constructor cl:&rest sub-parsers)
  `(define-instance (ParseSqlValue ,constructor)
     (define sql-value-parser
       (liftAn ,constructor ,@sub-parsers))))

(coalton-toplevel
  (define-simple-parser Integer SqlInt)
  (define-simple-parser String SqlText)

  (define-instance (ParseSqlValue Boolean)
    (define sql-value-parser
      (RowParser (fn (row)
                   (match row
                     ((Nil)
                      (Err (ResultParseError "Ran out of SQL values to parse.")))
                     ((Cons (SqlBool b) rest)
                      (Ok (Tuple b rest)))
                     ((Cons (SqlText "TRUE") rest)
                      (Ok (Tuple True rest)))
                     ((Cons (SqlText "FALSE") rest)
                      (Ok (Tuple False rest)))
                     ((Cons val _)
                      (Err (ResultParseError (build-str "Expected "
                                                        "SqlBool"
                                                        " received: "
                                                        (force-string val))))))))))

  (define-instance (ParseSqlValue :p => ParseSqlValue (Optional :p))
    (define sql-value-parser
      (RowParser (fn (row)
                   (match row
                     ((Nil)
                      (Err (ResultParseError "Ran out of SQL values to parse.")))
                     ((Cons (SqlNull) rest)
                      (Ok (Tuple None rest)))
                      (_
                       (do
                        ((Tuple val rest) <- (run-row-parser sql-value-parser row))
                        (pure (Tuple (Some val) rest)))))))))

  (define-instance ((ParseSqlValue :a) (ParseSqlValue :b) => ParseSqlValue (Tuple :a :b))
    (define sql-value-parser
      (RowParser (fn (row)
                   (match row
                     ((Nil)
                      (Err (ResultParseError "Ran out of SQL values to parse.")))
                     ((Cons _ (Nil))
                      (Err (ResultParseError "Ran out of SQL values to parse.")))
                      (_
                       (do
                        ((Tuple a rest1) <- (run-row-parser sql-value-parser row))
                        ((Tuple b rest2) <- (run-row-parser sql-value-parser rest1))
                        (pure (Tuple (Tuple a b) rest2)))))))))
  )
