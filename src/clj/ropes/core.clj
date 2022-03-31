(ns ropes.core
  "A persistent data structure for efficient inserts and views of long sequences.

  The data structure is a sequential collection with metadata support, Clojure's
  collection equality, ordered collection hashing, and is
  [[clojure.lang.Counted]]. Individual elements can be [[conj]]oined onto the
  end of the rope."
  (:refer-clojure :exclude [concat split replace])
  (:import
   (clojure.lang
    Counted IHashEq IMeta Indexed IObj IPersistentCollection
    Seqable SeqIterator Sequential)
   (java.io Writer)
   (java.util List)))

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
  (empty [_this]
    (Rope. nil nil 0 0 nil meta))
  (equiv [this other]
    (cond
      (identical? this other) true
      (or (instance? Rope other)
          (instance? List other)
          (sequential? other)) (= (seq this) (seq other))
      :else false))

  Indexed
  (nth [this idx]
    (nth this idx nil))
  (nth [_this idx not-found]
    (if data
      (if (< idx count)
        (nth data idx not-found)
        (throw (IndexOutOfBoundsException.)))
      (cond
        (< idx weight) (nth left idx not-found)
        (< idx count) (nth right (- idx weight) not-found)
        :else (throw (IndexOutOfBoundsException.)))))

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

(defn rope?
  "Returns true if `r` is a [[Rope]]."
  [r]
  (instance? Rope r))

(defn concat
  "Constructs a [[Rope]] with the elements of each input sequence in order in
  constant time."
  (^Rope [] (rope))
  (^Rope [x]
   (if-not (rope? x) (rope x) x))
  (^Rope [x y]
   (let [^Rope x (if-not (rope? x) (rope x) x)
         ^Rope y (if-not (rope? y) (rope y) y)]
     (Rope. x y (.-count x) (+ (.-count x) (.-count y))
            nil (merge (meta x) (meta y)))))
  (^Rope [x y & more]
   (reduce concat (list* x y more))))

(defn rope
  "Constructs a [[Rope]].
  With no arguments, constructs an empty rope with no elements. With one seqable
  argument, constructs a rope with its elements. The second argument is a
  metadata map used to construct the rope. The third optional argument is a
  count of elements to partition the sequence into in order to ensure that
  operations upon the rope are not too expensive."
  ([] (rope nil))
  ([s] (Rope. nil nil (count s) (count s) s nil))
  ([s n] (apply concat (partition-all n s)))
  ([s n m] (with-meta (rope s n) m)))

(defn split
  "Constructs two [[Rope]]s with all the elements before and after `idx` in logarithmic time.

  If the index is within a node, that node's sequence will be split
  with [[split-at]]."
  [^Rope r idx]
  {:pre [(<= idx (count r))]}
  (letfn [(s [^Rope r idx]
            (if (some? (.-data r))
              (cond
                (zero? idx) [(rope) r]
                (= idx (.-count r)) [r (rope)]
                :else (mapv #(with-meta (rope %) (.-meta r)) (split-at idx (.-data r))))
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
  "Constructs a new rope without the elements from `start` to `end` in logarithmic
  time."
  (^Rope [r start]
   (first (split r start)))
  (^Rope [r start end]
   (let [[r1 r2] (split r start)
         [_ r2] (split r2 (- end start))]
     (concat r1 r2))))

(defn view
  "Constructs a new rope with only the elements from `start` to `end` in
  logarithmic time."
  (^Rope [r start]
   (second (split r start)))
  (^Rope [r start end]
   (first (split (second (split r start)) (- end start)))))

(defn insert
  "Constructs a new rope with the elements of `s` inserted at `idx` in logarithmic
  time."
  ^Rope [r idx s]
  (if (< idx (count r))
    (let [[r1 r2] (split r idx)]
      (concat r1 (if-not (rope? s) (rope s) s) r2))
    (concat r (if-not (rope? s) (rope s) s))))

(defn replace
  "Constructs a new rope with the elements from `start` to `end` substituted for
  the elements of `s` in logarithmic time."
  ^Rope [r start end s]
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
  (when (meta v)
    (.write ^Writer w "^")
    (print-dup (meta v) w)
    (.write ^Writer w " "))
  (.write ^Writer w "#=(ropes.core/rope [ ")
  (doseq [item (.-data ^Rope v)]
    (print-dup item w)
    (.write ^Writer w " "))
  (.write ^Writer w "] 100)"))
