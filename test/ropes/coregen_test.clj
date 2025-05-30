(ns ropes.coregen-test
  (:require
   [clojure.edn :as edn]
   [clojure.test :as t]
   [ropes.core :as sut]
   [clojure.spec.alpha :as s]
   [clojure.spec.gen.alpha :as gen]
   [clojure.spec.test.alpha :as stest]
   )
  (:import
   (java.util ArrayList)))


;; Instead of generating regular indices,
;; generate numbers in the range [0.0 , 1.0]
;; so that the index can apply to any size rope.
;; These "normalized" indices are then converted
;; to actual indices when the op is applied.
(s/def ::normalized-index (s/double-in :min 0 :max 1))
(defn ->index
  "Given a double, in the range [0.0,1.0] return an index [0, (count `coll`)].

  Given two doubles in the range [0.0, 1.0] return a tuple of 2 indices in the range [0, (count `coll`)] 
  where the 2nd index is guaranteed to be greater than or equal to the first. "
  ([coll normalized-start normalized-end]
   (let [cnt (count coll)
         start (->index coll normalized-start)
         end (+ start
                (Math/round (* (- cnt start) normalized-end)))]
     [start end]))
  ([coll normalized-index]
   (Math/round (* (count coll) normalized-index))))

(s/def ::new-rope-content (s/or :nil nil?
                                :seq seq?
                                :string string?
                                :vector (s/coll-of any? :into [])))

(s/def ::new-rope-op (s/spec (s/cat :op #{:new} :s (s/? ::new-rope-content))))

(s/def ::view-op (s/spec (s/cat :op #{:view} :start ::normalized-index :end (s/? ::normalized-index))) )

;; % is used as a placeholder for referencing the previous rope passed to the op.
(s/def ::concat-op (s/spec (s/cat :op #{:concat} :ropes (s/* (s/or :new-rope ::new-rope-op
                                                                   :last-rope '#{%})) )))

(s/def ::insert-op (s/spec (s/cat :op #{:insert} :idx ::normalized-index :s ::new-rope-content)))

(s/def ::snip-op (s/spec (s/cat :op #{:snip} :start ::normalized-index :end (s/? ::normalized-index))))

(s/def ::replace-op (s/spec (s/cat :op #{:replace} :start ::normalized-index :end ::normalized-index :s ::new-rope-content)))

(s/def ::split-op (s/spec (s/cat :op #{:split} :idx ::normalized-index :left-or-right #{:left :right})))

(s/def ::op (s/or :new ::new-rope-op
                  :view ::view-op
                  :concat ::concat-op
                  :insert ::insert-op
                  :snip ::snip-op
                  :replace ::replace-op
                  :split ::split-op))

(s/def ::op-not-new (s/or :view ::view-op
                          :concat ::concat-op
                          :insert ::insert-op
                          :snip ::snip-op
                          :replace ::replace-op
                          :split ::split-op))

(s/def ::seq-op (s/spec (s/cat :f #{`seq})))
(s/def ::count-op (s/spec (s/cat :f #{`count})))
(defn ->nth-index [coll normalized-index]
  (Math/round (* (dec (count coll)) normalized-index)))
(defn nth-normalized
  ([o idx]
   (when (seq o)
     (nth o (->nth-index o idx))))
  ([o idx not-found]
   (nth o (->nth-index o idx) not-found)))
(s/def ::nth-op (s/spec (s/cat :f #{`nth-normalized} :idx ::normalized-index :not-found (s/? any?) )))

(defn hasheq [o] (.hasheq o))
(s/def ::hasheq-op (s/spec (s/cat :f #{`hasheq})))

(s/def ::conj-op (s/spec (s/cat :f #{`conj} :args (s/* any?))))
(defn reduce-test
  ([coll]
   (reduce (fn
             ([] coll)
             ([coll x] coll))
           coll))
  ([coll init]
    (reduce (fn [coll x] coll) init coll)))
(s/def ::reduce-op (s/spec (s/cat :f #{`reduce-test}
                                  :init (s/? any?))))

(s/def ::protocol-op (s/or :nth ::nth-op
                           :hasheq ::hasheq-op
                           :conj ::conj-op
                           :reduce ::reduce-op
                           :seq ::seq-op
                           :count ::count-op))


;; make sure we start with :new op.
;; protocol ops might not return a rope, so they can only be last.
(s/def ::ops (s/cat :new ::new-rope-op :more (s/* ::op-not-new) :last (s/? ::protocol-op)))

(comment
  (gen/sample (s/gen ::new-rope-op))
  (gen/sample (s/gen ::normalized-index))
  (gen/sample (s/gen ::view-op))
  (gen/sample (s/gen ::concat-op))
  (gen/sample (s/gen ::insert-op))
  (gen/sample (s/gen ::snip-op))
  (gen/sample (s/gen ::split-op))
  (gen/sample (s/gen ::replace-op))
  (gen/sample (s/gen ::ops))

  ,)



(defmulti apply-op (fn [impl-type prev op-type & op-args]
                     [impl-type op-type]))

;; by default assume op is a single dispatch function
(defmethod apply-op :default
  ([impl-type prev op & op-args]
   (let [f (if (symbol? op)
             (resolve op)
             op)]
     (apply f prev op-args))))

(defmethod apply-op [:vector :new]
  ([_ _ _]
   [])
  ([_ _ _ s]
   (into [] s)))

(defmethod apply-op [:vector :view]
  ([_ prev _ start]
   (subvec prev
           (->index prev start)))
  ([_ prev _ start end]
   (let [[start end] (->index prev start end)]
    (subvec prev start end))))

;; % is used as a placeholder for referencing the previous rope passed to the op.
(defmethod apply-op [:vector :concat]
  ([_ prev _ & args]
   (into []
         (comp (map (fn [arg]
                      (if (= '% arg)
                        prev
                        (apply apply-op :vector prev arg))))
               cat)
         args)))

(defmethod apply-op [:vector :insert]
  ([_ prev _ idx s]
   (let [idx (->index prev idx)]
     (into []
           cat
           [(subvec prev 0 idx)
            s
            (subvec prev idx)]))))

(defmethod apply-op [:vector :snip]
  ([_ prev _ start]
   (subvec prev 0 (->index prev start)))
  ([_ prev _ start end]
   (let [[start end] (->index prev start end)]
     (into []
           cat
           [(subvec prev 0 start)
            (subvec prev end (count prev))]))))

(defmethod apply-op [:vector :split]
  ([_ prev _ idx left-or-right]
   (let [idx (->index prev idx)]
     (case left-or-right
       :left (subvec prev 0 idx)
       :right (subvec prev idx)))))

(defmethod apply-op [:vector :replace]
  ([_ prev _ start end s]
   (let [[start end] (->index prev start end)]
    (into []
          cat
          [(subvec prev 0 start)
           s
           (subvec prev end)]))))


(defmethod apply-op [:rope :new]
  ([_ _ _]
   (sut/rope))
  ([_ _ _ s]
   (sut/rope s)))

(defmethod apply-op [:rope :view]
  ([_ prev _ start]
   (sut/view prev (->index prev start)))
  ([_ prev _ start end]
   (let [[start end] (->index prev start end)]
     (sut/view prev start end))))

;; % is used as a placeholder for referencing the previous rope passed to the op.
(defmethod apply-op [:rope :concat]
  ([_ prev _ & args]
   (apply sut/concat
          (eduction
                (map (fn [arg]
                       (if (= '% arg)
                         prev
                         (apply apply-op :rope prev arg))))
                args))))

(defmethod apply-op [:rope :insert]
  ([_ prev _ idx s]
   (sut/insert prev (->index prev idx) s)))

(defmethod apply-op [:rope :snip]
  ([_ prev _ start]
   (sut/snip prev (->index prev start)))
  ([_ prev _ start end]
   (let [[start end] (->index prev start end)]
     (sut/snip prev start end))))

(defmethod apply-op [:rope :split]
  ([_ prev _ idx left-or-right]
   (let [idx (->index prev idx)]
     (case left-or-right
       :left (-> (sut/split prev idx) first)
       :right (-> (sut/split prev idx) second)))))

(defmethod apply-op [:rope :replace]
  ([_ prev _ start end s]
   (let [[start end] (->index prev start end)]
     (sut/replace prev start end s))))


(defn apply-ops [impl-type ops]
  (reduce
   (fn [prev op]
     (apply apply-op impl-type prev op))
   nil
   ops))

(defn compare-ops [ops]
  (= (apply-ops :vector ops)
     (apply-ops :rope ops)))
(s/fdef compare-ops
  :args (s/cat :ops (s/spec ::ops))
  :ret true?)


(defn results->shrunk [results]
  (-> results
      first
      :clojure.spec.test.check/ret
      :shrunk
      :smallest
      first
      first))

(defn results->fail [results]
  (-> results
      first
      :clojure.spec.test.check/ret
      :fail
      first
      first))

(comment
  (def results (stest/check `compare-ops))
  
  ,)

(t/deftest reduce-ops
  (t/testing "reduce no-init"
    (t/is (compare-ops '((:new nil)
                         (:concat (:new [nil]))
                         (:replace 1.0 1.0 "00")
                         (ropes.coregen-test/reduce-test)))
          "reduce with 1 size data on left")
    (t/is (compare-ops '((:new nil)
                         (:concat (:new (0)) (:new))
                         (ropes.coregen-test/reduce-test)))
          "reduce with concatting empty ropes")))

(t/deftest switching-data-storage
  (t/testing "switching string to vector storage"
    (t/is (compare-ops '((:new nil)
                         (:insert 1.0 (\space 0))
                         (ropes.coregen-test/nth-normalized 1.0 nil))))))

(t/deftest split-ops
  (t/testing "splitting"
    (t/is (compare-ops '((:new nil)
                         (:view 1.0 1.0)))
          "Splitting empty rope.")))

(t/deftest nth-ops
  (t/testing "calling nth on empty rope"
    (t/is (compare-ops '((:new nil)
                         (ropes.coregen-test/nth-normalized 1.0 nil))))))
