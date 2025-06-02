(cl:in-package :cl-user)
(defpackage :citations/util
  (:use
   #:coalton
   #:coalton-prelude)
  (:local-nicknames
   (:ax #:alexandria)
   (:m  #:coalton-library/ord-map)
   (:l  #:coalton-library/list)
   (:it #:coalton-library/iterator)
   )
  (:export
   #:map-empty?
   #:derive-eq
   #:build-str
   #:newline
   #:to-string
   #:join-str
   #:liftAn))
(in-package :citations/util)

(named-readtables:in-readtable coalton:coalton)

(cl:eval-when (:compile-toplevel :load-toplevel :execute)
  (cl:defun make-match (cstr-form)
    "Converts A => ((Tuple (A) (A)) True) and (A a b) to
((Tuple (A a1 b1) (A a2 b2)) (and (== a1 a2) (== b1 b2))"
    (cl:let ((cst (cl:car cstr-form)))
      (cl:if (cl:eq 1 (cl:length cstr-form))
             `((Tuple (,cst) (,cst))
               True)
             (cl:let ((syms1 (cl:mapcar (cl:lambda (_) (cl:gensym))
                                        (cl:cdr cstr-form)))
                      (syms2 (cl:mapcar (cl:lambda (_) (cl:gensym))
                                        (cl:cdr cstr-form))))
               `((Tuple (,cst ,@syms1) (,cst ,@syms2))
                 (and
                  ,@(cl:mapcar (cl:lambda (s1 s2)
                                 `(== ,s1 ,s2))
                               syms1
                               syms2))))))))

(cl:defmacro derive-eq (type-form cstr-forms)
  (cl:let* ((norm-cstr-forms (cl:mapcar #'ax:ensure-list cstr-forms))
            (matches (cl:mapcar #'make-match norm-cstr-forms))
            (sym1 (cl:gensym))
            (sym2 (cl:gensym)))
    `(define-instance (Eq ,type-form)
       (inline)
       (define (== ,sym1 ,sym2)
         (match (Tuple ,sym1 ,sym2)
           ,@matches
           (_ False))))))

(cl:defmacro build-str (cl:&rest str-parts)
  "Concatenate all STR-PARTS."
  `(fold <> "" (make-list ,@str-parts)))

(coalton-toplevel
  (declare map-empty? (m:Map :k :v -> Boolean))
  (define (map-empty? m)
    (l:null? (it:collect! (it:into-iter m))))

  (declare newline String)
  (define newline (lisp String () (cl:format nil "~%")))

  (declare to-string (Into :a String => :a -> String))
  (define to-string into)

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

(coalton-toplevel
  (declare <*> (Applicative :f => :f (:a -> :b) -> :f :a -> :f :b))
  (define <*> (liftA2 id)))

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
