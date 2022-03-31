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

(def ^:private max-size-for-collapse 512)

(deftype Rope [left right weight cnt data meta]
  Counted
  (count [_] cnt)

  IHashEq
  (hasheq [this]
    (hash-ordered-coll (seq this)))

  IMeta
  (withMeta [_ meta]
    (Rope. left right weight cnt data meta))

  IObj
  (meta [_] meta)

  IPersistentCollection
  (cons [this s]
    (cond
      (and data
           (<= (count data) max-size-for-collapse))
      (Rope. nil nil (inc weight) (inc cnt)
             (cond
               (and (string? data)
                    (or (char? s)
                        (string? s))) (str data s)
               (vector? data) (conj data s))
             meta)

      (and left right
           (some? (.-data ^Rope right)))
      (Rope. left (conj right s) weight (inc cnt) nil meta)

      (not (or left right data))
      (Rope. nil nil 1 1 [s] meta)

      :else (Rope. this (rope [s]) cnt (inc cnt) nil meta)))
  (empty [_this]
    (Rope. nil nil 0 0 nil meta))
  (equiv [this other]
    (cond
      (identical? this other) true
      (and (counted? other)
           (not= cnt (count other))) false
      (or (instance? Rope other)
          (instance? List other)
          (sequential? other))
      (= (seq this) (seq other))
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

(defn flat?
  "Checks if the given rope consists of only one node."
  [^Rope r]
  (or (.-data r)
      (not (or (.-left r)
               (.-right r)))))

(defn node-count
  "Counts the number of nodes used to construct the rope."
  [^Rope r]
  (if-not (flat? r)
    (+ (node-count (.-left r))
       (node-count (.-right r))
       1)
    1))

(defn concat
  "Constructs a [[Rope]] with the elements of each input sequence in order in
  constant time."
  (^Rope [] (rope))
  (^Rope [x]
   (if-not (rope? x) (rope x) x))
  (^Rope [x y]
   (let [^Rope x (if-not (rope? x) (rope x) x)
         ^Rope y (if-not (rope? y) (rope y) y)]
     (or
      (cond
        (and (flat? x)
             (flat? y))
        (cond
          (and (string? (.-data x))
               (<= (count (.-data x)) max-size-for-collapse)
               (string? (.-data y))
               (<= (count (.-data y)) max-size-for-collapse))
          (with-meta (rope (str (.-data x) (.-data y))) (.-meta x))
          (and (vector? (.-data x))
               (<= (count (.-data x)) max-size-for-collapse)
               (vector? (.-data y))
               (<= (count (.-data y)) max-size-for-collapse))
          (with-meta (rope (into (.-data x) (.-data y))) (.-meta x)))

        (and (flat? y)
             (flat? (.-right x)))
        (with-meta (concat (.-left x) (concat (.-right x) y))
          (.-meta x)))
      (Rope. x y (.-cnt x) (+ (.-cnt x) (.-cnt y))
             nil (.-meta x)))))
  (^Rope [x y & more]
   (reduce concat (list* x y more))))

(defn rope
  "Constructs a [[Rope]] from the given sequence `s`.

  The performance of various operations will be dependant on the type of
  sequence used to construct a rope. For arbitrary values use a vector. A string
  may be used if the rope is intended to store text."
  (^Rope [] (rope nil))
  (^Rope [s] (Rope. nil nil (count s) (count s) s nil)))

(defn split
  "Constructs two [[Rope]]s with all the elements before and after `idx` in logarithmic time.

  If the index is within a node, that node's sequence will be split with
  [[split-at]] or a method specific to the sequence the rope is built over for
  better performance when possible."
  [r idx]
  (let [^Rope r (if-not (rope? r) (rope r) r)]
    (when (> idx (.-cnt r))
      (throw (IndexOutOfBoundsException.)))
    (letfn [(s [^Rope r idx]
              (if-some [data (.-data r)]
                (cond
                  (zero? idx) [(with-meta (rope) (.-meta r)) r]
                  (= idx (.-cnt r)) [r (with-meta (rope) (.-meta r))]
                  (string? data) [(rope (subs data 0 idx)) (rope (subs data idx))]
                  (vector? data) [(rope (subvec data 0 idx)) (rope (subvec data idx))]
                  :else (mapv #(with-meta (rope %) (.-meta r))
                              (split-at idx data)))
                (if (< idx (.-weight r))
                  (let [[^Rope r1 ^Rope r2] (split (.-left r) idx)]
                    [r1
                     (Rope. r2 (.-right r) (.-cnt r2) (+ (.-cnt r2) (.-cnt ^Rope (.-right r)))
                            nil (.-meta r))])
                  (let [[^Rope r1 ^Rope r2] (split (.-right r) (- idx (.-weight r)))]
                    [(Rope. (.-left r) r1 (.-weight r) (+ (.-weight r) (.-cnt r1)) nil (.-meta r))
                     r2]))))]
      (s r idx))))

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
  (.write ^Writer w "])"))
