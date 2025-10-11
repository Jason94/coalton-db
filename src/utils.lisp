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
   #:liftAn
   #:optional-clause
   #:chunk-list
   #:left-pad
   #:right-pad
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

(cl:defun liftAn_ (f rest)
  (cl:let ((len (cl:length rest)))
    (cl:cond
      ((cl:< len 2) (cl:error "liftAn requires two or more terms!"))
      ((cl:eq len 2)
       `(liftA2 ,f ,@rest))
      (cl:t
       (cl:let* ((flipped (cl:reverse rest))
                 (elt (cl:car flipped))
                 (rem (cl:reverse (cl:cdr flipped))))
         `(<*> ,(liftAn_ f rem) ,elt))))))

(cl:defmacro liftAn (f cl:&rest rest)
  (liftAn_ f rest))

(cl:defun optional-clause (val)
  "Generate code to wrap a possibly Common Lisp val (particularly a macro arg),
in a Coalton Optional."
  (cl:if val
         `(Some ,val)
         `None))

(coalton-toplevel
  (declare chunk-list (UFix -> List :a -> List (List :a)))
  (define (chunk-list n lst)
    (rec % ((ret Nil)
            (rem lst))
      (match rem
        ((Nil) (reverse ret))
        (_ (% (Cons (l:take n rem)
                    ret)
              (l:drop n rem))))))

  (declare left-pad (String -> String))
  (define (left-pad str)
    "Add a blank space to the left of `str` if it is not the empty string."
    (if (== str "")
        ""
        (<> " " str)))

  (declare right-pad (String -> String))
  (define (right-pad str)
    "Add a blank space to the right of `str` if it is not the empty string."
    (if (== str "")
        ""
        (<> str " ")))
  )
