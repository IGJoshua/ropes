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
    IReduce IReduceInit Seqable SeqIterator Sequential)
   (java.io Writer)
   (java.util List)))

(declare rope ^:private flat?)

(def ^:private max-size-for-collapse 512)

;; TODO(Joshua): Consider introducing a tree depth value that will trigger a
;; rebalance on largely imbalanced trees, e.g. ones with a branch depth > 64,
;; which would happen infrequently and which, when balanced, could only approach
;; that depth by vastly exceeding system memory.
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
      (and (flat? this)
           (< (count data) max-size-for-collapse))
      (Rope. nil nil (inc weight) (inc cnt)
             (cond
               (and (or (nil? data)
                        (string? data))
                    (or (char? s)
                        (string? s))) (str data s)
               (vector? data) (conj data s)
               (nil? data) [s]
               :else (throw (ex-info "UNREACHABLE: attempted to cons onto a badly-typed rope" {})))
             meta)

      (and (not (flat? this))
           (flat? right))
      (Rope. left (.cons ^Rope right s) weight (inc cnt) nil meta)

      (not (or left right data))
      (Rope. nil nil 1 1 [s] meta)

      :else (Rope. this (rope (if (or (char? s) (string? s))
                                (str s)
                                [s]))
                   cnt (inc cnt) nil meta)))
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

  IReduceInit
  (reduce [this f init]
    (if (flat? this)
      (reduce f init data)
      (->> init
        (.reduce ^Rope left f)
        (.reduce ^Rope right f))))

  IReduce
  (reduce [this f]
    (cond
      (zero? cnt) (f)
      ;; NOTE(Joshua): The way ropes are constructed guarantees that if the
      ;; count of the rope is 1, this must be the only leaf node.
      (= 1 cnt) (first data)
      (flat? this) (reduce f data)
      :else (letfn [(step-reduce
                      [acc r]
                      (if (flat? r)
                        (reduce f acc data)
                        (-> acc
                            (step-reduce left)
                            (step-reduce right))))
                    (left-reduce
                      [r]
                      (if (flat? r)
                        (reduce f data)
                        (-> (left-reduce left)
                            (step-reduce right))))]
              (-> (left-reduce left)
                  (step-reduce right)))))

  Indexed
  (nth [this idx]
    (nth this idx nil))
  (nth [_this idx not-found]
    (if data
      (if (< idx cnt)
        (nth data idx not-found)
        (throw (IndexOutOfBoundsException.)))
      (cond
        (< idx weight) (.nth ^Rope left idx not-found)
        (< idx cnt) (.nth ^Rope right (- idx weight) not-found)
        :else (throw (IndexOutOfBoundsException.)))))

  Seqable
  (seq [this]
    (if (flat? this)
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
  (boolean
   (or (.-data r)
       (not (or (.-left r)
                (.-right r))))))

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
  "Constructs a [[Rope]] from the given vector or string `s`.

  If `s` is not a vector or string, acts like calling [[into]] with an empty
  rope and the result of calling [[seq]] on `s`."
  (^Rope [] (rope nil))
  (^Rope [s]
   (if (or (string? s) (vector? s))
     (Rope. nil nil (count s) (count s) s nil)
     (into (Rope. nil nil 0 0 nil nil) (seq s)))))

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
                  :else (throw (ex-info (str "UNREACHABLE: attempted to index an empty or badly-typed rope, but it was not out of bounds") {})))
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
