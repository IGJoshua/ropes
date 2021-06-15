(ns ropes.core
  (:refer-clojure :exclude [concat split replace])
  (:import
   (clojure.lang
    Counted IHashEq IMeta IObj IPersistentCollection
    Seqable SeqIterator Sequential)
   (java.io Writer)
   (java.util List))
  (:require [clojure.string :as str]))

(declare rope)

(deftype Rope [left right weight count data meta]
  Counted
  (count [_] count)

  IHashEq
  (hasheq [this]
    (hash-ordered-coll (seq this)))

  IMeta
  (withMeta [_ meta]
    (Rope. left right weight count data meta))

  IObj
  (meta [_] meta)

  IPersistentCollection
  (cons [this s]
    (Rope. this (rope [s]) count (inc count) nil meta))
  (empty [this]
    (Rope. nil nil 0 0 nil meta))
  (equiv [this other]
    (cond
      (identical? this other) true
      (or (instance? Rope other)
          (instance? List other)
          (sequential? other)) (= (seq this) (seq other))
      :else false))

  Seqable
  (seq [_]
    (if (or (some? data)
            (and (nil? left)
                 (nil? right)))
      (seq data)
      (lazy-cat (seq left) (seq right))))

  Sequential

  Iterable
  (iterator [this]
    (SeqIterator. (seq this))))

(defn rope
  ([] (rope nil nil))
  ([s] (rope s nil))
  ([s m] (Rope. nil nil (count s) (count s) s m)))

(defn rope?
  [r]
  (instance? Rope r))

(defn concat
  ([] (rope))
  ([x]
   (if-not (rope? x) (rope x) x))
  ([x y]
   (let [x (if-not (rope? x) (rope x) x)
         y (if-not (rope? y) (rope y) y)]
     (Rope. x y (.-count x) (+ (.-count x) (.-count y))
            nil (merge (meta x) (meta y)))))
  ([x y & more]
   (reduce concat (list* x y more))))

(defn split
  [^Rope r idx]
  {:pre [(< idx (count r))]}
  (letfn [(s [^Rope r idx]
            (if (some? (.-data r))
              (cond
                (zero? idx) [(rope) r]
                (= idx (.-count r)) [r (rope)]
                :else (mapv #(rope % (.-meta r)) (split-at idx (.-data r))))
              (if (< idx (.-weight r))
                (let [[r1 r2] (split (.-left r) idx)]
                  [r1
                   (Rope. r2 (.-right r) (.-count r2) (+ (.-count r2) (.-count (.-right r)))
                          nil (.-meta r))])
                (let [[r1 r2] (split (.-right r) (- idx (.-weight r)))]
                  [(Rope. (.-left r) r1 (.-weight r) (+ (.-weight r) (.-count r1)) nil (.-meta r))
                   r2]))))]
    (s (if-not (rope? r) (rope r) r) idx)))

(defn snip
  ([^Rope r start]
   (first (split r start)))
  ([^Rope r start end]
   (let [[r1 r2] (split r start)
         [_ r2] (split r2 (- end start))]
     (concat r1 r2))))

(defn view
  ([^Rope r start]
   (second (split r start)))
  ([^Rope r start end]
   (first (split (second (split r start)) (- end start)))))

(defn insert
  [^Rope r idx s]
  (if (< idx (count r))
    (let [[r1 r2] (split r idx)]
      (concat r1 (if-not (rope? s) (rope s) s) r2))
    (concat r (if-not (rope? s) (rope s) s))))

(defn replace
  [^Rope r start end s]
  (insert (snip r start end) start s))

(defmethod print-method Rope
  [v w]
  (.write ^Writer w "#rope [ ")
  (doseq [item (seq v)]
    (print-method item w)
    (.write ^Writer w " "))
  (.write ^Writer w "]"))

(defmethod print-dup Rope
  [v w]
  (.write ^Writer w "#=(ropes.core/rope [ ")
  (doseq [item (.-data ^Rope v)]
    (print-dup item w)
    (.write ^Writer w " "))
  (.write ^Writer w "] ")
  (print-dup (meta v) w)
  (.write ^Writer w ")"))
