(cl:in-package :cl-user)
(defpackage :coalton-db/util
  (:use
   #:coalton
   #:coalton-prelude)
  (:local-nicknames
   (:l  #:coalton-library/list)
   )
  (:export
   #:join-str
   #:build-str))
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
             (l:cdr strs))))))

(cl:defmacro build-str (cl:&rest str-parts)
  "Concatenate all STR-PARTS."
  `(fold <> "" (make-list ,@str-parts)))
