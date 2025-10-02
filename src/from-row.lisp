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
   #:parse-row
   #:define-row-parser
   #:sql-value-parser

   ;;; Library Private
   ))

(in-package :coalton-db/from-row)

(named-readtables:in-readtable coalton:coalton)

;;;
;;; Row Parser
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

(coalton-toplevel
  (define-simple-parser Integer SqlInt)
  (define-simple-parser String SqlText)
  (define-simple-parser Boolean SqlBool)
  )

(cl:defmacro define-row-parser (constructor cl:&rest sub-parsers)
  `(define-instance (ParseSqlValue ,constructor)
     (define sql-value-parser
       (liftAn ,constructor ,@sub-parsers))))
