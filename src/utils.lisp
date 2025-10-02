(cl:in-package :cl-user)
(defpackage :coalton-db/util
  (:use
   #:coalton
   #:coalton-prelude
   )
  (:local-nicknames
   (:l  #:coalton-library/list)
   (:opt #:coalton-library/optional)
   )
  (:export
   #:join-str
   #:build-str
   #:i#
   #:force-string
   #:contains?
   #:contains-where?
   ))
(in-package :coalton-db/util)

(named-readtables:in-readtable coalton:coalton)

(coalton-toplevel
  (declare join-str (String -> List String -> String))
  (define (join-str sep strs)
    (match (length strs)
      (0 "")
      (1 (l:car strs))
      (_
       (fold (fn (a b)
               (<> a (<> sep b)))
             (l:car strs)
             (l:cdr strs)))))

  (declare i# (UFix -> List :a -> :a))
  (define (i# i lst)
    (opt:from-some "List index out of bounds." (l:index i lst)))

  (declare force-string (:a -> String))
  (define (force-string x)
    (lisp String (x)
      (cl:format cl:nil "~a" x)))

  (declare contains? (Eq :a => :a -> List :a -> Boolean))
  (define (contains? elt lst)
    (match (l:elemindex elt lst)
      ((Some _) True)
      ((None) False)))

  (declare contains-where? ((:a -> Boolean) -> List :a -> Boolean))
  (define (contains-where? f lst)
    (match lst
      ((Nil) False)
      ((Cons x rem)
       (if (f x)
           True
           (contains-where? f rem))))))

(cl:defmacro build-str (cl:&rest str-parts)
  "Concatenate all STR-PARTS."
  `(fold <> "" (make-list ,@str-parts)))
