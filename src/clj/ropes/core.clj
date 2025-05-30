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
   (java.util List)
   (java.util.stream IntStream)))

(declare rope ^:private flat? ^:private rebalance view)

(def ^:private max-size-for-collapse 512)
(def ^:private max-depth 64)

(deftype Rope [left right depth weight cnt data meta
               ^:unsynchronized-mutable _hasheq]
  Counted
  (count [_] cnt)

  IHashEq
  (hasheq [this]
    (or _hasheq
        (set! _hasheq (hash-ordered-coll this))))

  IMeta
  (meta [_] meta)

  IObj
  (withMeta [_ meta]
    (Rope. left right depth weight cnt data meta _hasheq))

  IPersistentCollection
  (cons [this s]
    (let [^Rope res
          (cond
            (and (flat? this)
                 (< (count data) max-size-for-collapse))
            (Rope. nil nil 0 (inc weight) (inc cnt)
                   (cond
                     (and (or (nil? data)
                              (string? data))
                          (char? s)) (str data s)
                     (vector? data) (conj data s)
                     (nil? data) [s]
                     :else (throw (ex-info "UNREACHABLE: attempted to cons onto a badly-typed rope" {})))
                   meta nil)

            (and (not (flat? this))
                 (flat? right)
                 (< (.-cnt ^Rope right) max-size-for-collapse))
            (Rope. left (.cons ^Rope right s)
                   (inc (max (.-depth ^Rope left)
                             (.-depth ^Rope right)))
                   weight (inc cnt) nil meta nil)

            (not (or left right data))
            (Rope. nil nil 0 1 1 [s] meta nil)

            :else (Rope. this (rope (if (char? s) (str s) [s]))
                         (inc depth)
                         cnt (inc cnt) nil meta nil))]
      (if (> (.-depth res) max-depth)
        (rebalance res)
        res)))
  (empty [_this]
    (Rope. nil nil 0 0 0 nil meta nil))
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
  (nth [_this idx]
    (if data
      (if (< idx cnt)
        (nth data idx)
        (throw (IndexOutOfBoundsException.)))
      (cond
        (< idx weight) (.nth ^Rope left idx)
        (< idx cnt) (.nth ^Rope right (- idx weight))
        :else (throw (IndexOutOfBoundsException.)))))
  (nth [_this idx not-found]
    (if data
      (if (< idx cnt)
        (nth data idx)
        not-found)
      (cond
        (< idx weight) (.nth ^Rope left idx not-found)
        (< idx cnt) (.nth ^Rope right (- idx weight) not-found)
        :else not-found)))

  Seqable
  (seq [this]
    (if (flat? this)
      (seq data)
      (lazy-cat (seq left) (seq right))))

  Sequential

  Iterable
  (iterator [this]
    (SeqIterator. (seq this)))

  CharSequence
  (charAt [this index]
    (.nth this index))
  (length [_this] (int cnt))
  (subSequence [this start end]
    (view this start end))
  (chars [this]
    (if (flat? this)
      (if (string? data)
        (.chars ^CharSequence data)
        (throw (IllegalStateException. "Attempted to treat a non-string rope as a character sequence")))
      (IntStream/concat (.chars ^CharSequence left)
                        (.chars ^CharSequence right))))
  (codePoints [this]
    (if (flat? this)
      (if (string? data)
        (.codePoints ^CharSequence data)
        (throw (IllegalStateException. "Attempted to treat a non-string rope as a character sequence")))
      (IntStream/concat (.codePoints ^CharSequence left)
                        (.codePoints ^CharSequence right))))

  Object
  (toString [this]
    (.toString
     ^StringBuilder
     (reduce #(.append ^StringBuilder %1 ^String (str %2))
             ;; NOTE(Joshua): Start at `cnt` because if this is a string-based
             ;; rope it will never resize. If it is a value-based rope of
             ;; reasonable size, it will resize fewer times than with the
             ;; default buffer size.
             (StringBuilder. (int cnt))
             this))))

(defn- rotate-right
  "Rotates deep ropes to the right."
  ^Rope [^Rope r]
  (if (and (not (flat? r))
           (not (flat? (.-left r))))
    (let [^Rope lr (.-right ^Rope (.-left r))
          ^Rope right (Rope. lr (.-right r)
                             (inc (max (.-depth lr)
                                       (.-depth ^Rope (.-right r))))
                             (.-cnt lr) (+ (.-cnt lr) (.-cnt ^Rope (.-right r)))
                             nil nil nil)
          ^Rope left (.-left ^Rope (.-left r))]
      (Rope. left right
             (inc (max (.-depth left)
                       (.-depth right)))
             (.-cnt left) (+ (.-cnt left) (.-cnt right))
             nil (.-meta r) nil))
    r))

(defn- rotate-left
  "Rotates deep ropes to the left."
  ^Rope [^Rope r]
  (if (and (not (flat? r))
           (not (flat? (.-right r))))
    (let [^Rope rl (.-left ^Rope (.-right r))
          left-cnt (.-cnt ^Rope (.-left r))
          ^Rope left (Rope. (.-left r) rl
                            (inc (max (.-depth rl)
                                      (.-depth ^Rope (.-left r))))
                            left-cnt (+ left-cnt (.-cnt rl))
                            nil nil nil)
          ^Rope right (.-right ^Rope (.-right r))]
      (Rope. left right
             (inc (max (.-depth left)
                       (.-depth right)))
             (.-cnt left) (+ (.-cnt left) (.-cnt right))
             nil (.-meta r) nil))
    r))

(defn- rebalance
  "Recursively balances the rope to be a tree of near-equal depth."
  ^Rope [^Rope r]
  (if-not (flat? r)
    (let [^Rope left (.-left r)
          ^Rope right (.-right r)
          ^long diff (- (.-depth right)
                        (.-depth left))]
      (if (>= (Math/abs diff) 2)
        (let [f (if (neg? diff)
                  rotate-right
                  rotate-left)]
          (loop [times (quot (Math/abs diff) 2)
                 acc r]
            (if (pos? times)
              (recur (dec times) (f acc))
              (let [^Rope left (rebalance (.-left acc))
                    ^Rope right (rebalance (.-right acc))]
                (Rope. left right
                       (inc (max (.-depth left)
                                 (.-depth right)))
                       (.-weight acc) (.-cnt acc)
                       nil (.-meta acc) nil)))))
        r))
    r))

(defn rope?
  "Returns true if `r` is a [[Rope]]."
  [r]
  (instance? Rope r))

(defn- flat?
  "Checks if the given rope consists of only one node."
  [^Rope r]
  (boolean
   (or (.-data r)
       (not (or (.-left r)
                (.-right r))))))

(defn- node-count
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
         ^Rope y (if-not (rope? y) (rope y) y)
         ^Rope res
         (or
          (cond
            (and (flat? x)
                 (flat? y))
            (cond
              (and (string? (.-data x))
                   (string? (.-data y))
                   (< (+ (count (.-data x)) (count (.-data y))) max-size-for-collapse))
              (with-meta (rope (str (.-data x) (.-data y))) (.-meta x))
              (and (vector? (.-data x))
                   (vector? (.-data y))
                   (< (+ (count (.-data x)) (count (.-data y))) max-size-for-collapse))
              (with-meta (rope (into (.-data x) (.-data y))) (.-meta x)))

            (and (flat? y)
                 (flat? (.-right x))
                 (< (+ (count (.-data ^Rope (.-right x))) (count (.-data y))) max-size-for-collapse))
            (let [^Rope new-left (.-left x)
                  ^Rope new-right (let [r-data (.-data ^Rope (.-right x))
                                        y-data (.-data y)]
                                    (cond
                                      (and (string? r-data)
                                           (string? y-data))
                                      (rope (str r-data y-data))

                                      (and (vector? r-data)
                                           (vector? y-data))
                                      (rope (into r-data y-data))))]
              (when new-right
                (Rope. new-left new-right
                       (inc (max (.-depth new-left)
                                 (.-depth new-right)))
                       (.-cnt new-left) (+ (.-cnt new-left) (.-cnt new-right))
                       nil (.-meta x) nil))))
          (Rope. x y
                 (inc (max (.-depth x)
                           (.-depth y)))
                 (.-cnt x) (+ (.-cnt x) (.-cnt y))
                 nil (.-meta x) nil))]
     (if (> (.-depth res) max-depth)
       (rebalance res)
       res)))
  (^Rope [x y & more]
   (reduce concat (list* x y more))))

(defn rope
  "Constructs a [[Rope]] from the given vector or string `s`.

  If `s` is not a vector or string, acts like calling [[into]] with an empty
  rope and the result of calling [[seq]] on `s`."
  (^Rope [] (rope nil))
  (^Rope [s]
   (if (or (string? s) (vector? s))
     (Rope. nil nil 0 (count s) (count s) s nil nil)
     (into (Rope. nil nil 0 0 0 nil nil nil) (seq s)))))

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
                     (Rope. r2 (.-right r)
                            (inc (max (.-depth r2)
                                      (.-depth ^Rope (.-right r))))
                            (.-cnt r2) (+ (.-cnt r2) (.-cnt ^Rope (.-right r)))
                            nil (.-meta r) nil)])
                  (let [[^Rope r1 ^Rope r2] (split (.-right r) (- idx (.-weight r)))]
                    [(Rope. (.-left r) r1
                            (inc (max (.-depth ^Rope (.-left r))
                                      (.-depth r1)))
                            (.-weight r) (+ (.-weight r) (.-cnt r1)) nil (.-meta r) nil)
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
