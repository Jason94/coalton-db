(cl:in-package :cl-user)
(defpackage :coalton-db/from-row
  (:use
   #:coalton
   #:coalton-prelude
   #:coalton-db/util
   #:coalton-db/core)
  (:local-nicknames
   (:c #:coalton-library/cell)
   )
  (:import-from #:coalton-library/experimental/loops
   #:dolist)
  (:export
   ;;; Library Public

   #:ParseSqlValue
   #:parse-val

   #:ParseSqlRow
   #:parse-row
   #:parse-rows
   #:define-row-parser
   #:define-row-parser-from-val-parser
   #:sql-value-parser

   ;;; Library Private
   #:err-out-of-vals
   ))

(in-package :coalton-db/from-row)

(named-readtables:in-readtable coalton:coalton)

;;;
;;; Parse SQL Value
;;;

(coalton-toplevel
  (define-class (ParseSqlValue :a)
    (parse-val (SqlValue -> DbResult :a)))

  (declare wrong-type-err (SqlValue * String -> DbResult :a))
  (define (wrong-type-err val expected-type)
    (Err (ResultParseError
          (build-str "Could not convert" (force-string val) " to " expected-type "."))))

  (define-instance (ParseSqlValue Integer)
    (define (parse-val val)
      (match val
        ((SqlInt i) (Ok i))
        (_ (wrong-type-err val "Integer")))))

  (define-instance (ParseSqlValue String)
    (define (parse-val val)
      (match val
        ((SqlText i) (Ok i))
        (_ (wrong-type-err val "String")))))

  (define-instance (ParseSqlValue Boolean)
    (define (parse-val val)
      (match val
        ((SqlBool b) (Ok b))
        ((SqlText s)
         (cond
           ((== s "FALSE") (Ok False))
           ((== s "TRUE") (Ok True))
           (True (wrong-type-err val "Boolean"))))
        (_ (wrong-type-err val "Boolean")))))

  (define-instance (ParseSqlValue :a => ParseSqlValue (Optional :a))
    (define (parse-val val)
      (match val
        ((SqlNull) (Ok None))
        (_ (map Some (parse-val val)))))))

;;;
;;; Parse Rows
;;;

(coalton-toplevel

  (repr :transparent)
  (define-type (RowParser :a)
    (RowParser (Row -> DbResult (Tuple :a Row))))

  (inline)
  (declare run-row-parser (RowParser :a * Row -> DbResult (Tuple :a Row)))
  (define (run-row-parser (RowParser f) row)
    (f row))

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

  (define-class (ParseSqlRow :p)
    (sql-value-parser (RowParser :p)))

  (inline)
  (define (err-out-of-vals)
    (Err (ResultParseError "Unexpected SQL values to parse.")))

  (declare parse-row (ParseSqlRow :a => Row -> DbResult :a))
  (define (parse-row input)
    (do
     ((Tuple result rest) <- (run-row-parser sql-value-parser input))
     (match rest
       ((Nil)
        (Ok result))
       (_ (err-out-of-vals)))))

  (declare parse-rows (ParseSqlRow :a => List Row -> DbResult (List :a)))
  (define (parse-rows rows)
    "Parse a list of rows. When the first parsing error is encountered,
abort parsing the whole list."
    (let results = (c:new Nil))
    (foreach (row rows)
      (match (parse-row row)
        ((Ok a) (c:push! results a))
        ((Err e) (return (Err e)))))
    (OK (reverse (c:read results))))
  )

(cl:defmacro define-row-parser-from-val-parser (output-type cl:&optional quals)
  (cl:let ((quals-clause (cl:when quals
                                `(,@quals =>))))
    `(define-instance (,@quals-clause ParseSqlRow ,output-type)
       (define sql-value-parser
         (RowParser (fn (row)
                      (match row
                        ((Nil)
                         (err-out-of-vals))
                        ((Cons x rest)
                         (do
                          (val <- (parse-val x))
                          (Ok (Tuple val rest)))))))))))

(cl:defmacro define-row-parser (constructor cl:&rest sub-parsers)
  ;; Note: Inlining this causes Coalton to fail because it sees an invalid recursive
  ;; value definition.
  (cl:let ((value-fn-sym (cl:intern (cl:symbol-name (cl:gensym "value-fn")))))
    `(progn
       (define ,value-fn-sym
         (liftAn ,constructor ,@sub-parsers))

       (define-instance (ParseSqlRow ,constructor)
         (define sql-value-parser
           ,value-fn-sym)))))

(coalton-toplevel
  (define-row-parser-from-val-parser Integer)
  (define-row-parser-from-val-parser String)
  (define-row-parser-from-val-parser Boolean)
  (define-row-parser-from-val-parser (Optional :p) (ParseSqlValue :p))

  (define-instance ((ParseSqlRow :a) (ParseSqlRow :b) => ParseSqlRow (Tuple :a :b))
    (define sql-value-parser
      (RowParser (fn (row)
                   (match row
                     ((Nil)
                      (err-out-of-vals))
                     ((Cons _ (Nil))
                      (err-out-of-vals))
                      (_
                       (do
                        ((Tuple a rest1) <- (run-row-parser sql-value-parser row))
                        ((Tuple b rest2) <- (run-row-parser sql-value-parser rest1))
                        (pure (Tuple (Tuple a b) rest2)))))))))
  )
