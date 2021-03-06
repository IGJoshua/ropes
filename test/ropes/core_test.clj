(ns ropes.core-test
  (:require
   [clojure.edn :as edn]
   [clojure.test :as t]
   [ropes.core :as sut])
  (:import
   (java.util ArrayList)))

(t/deftest collections-implementation
  (t/testing "ropes are counted"
    (t/is (counted? (sut/rope)) "a rope satisfies the counted? predicate")
    (t/is (zero? (count (sut/rope))) "an empty rope has a count of zero")
    (t/is (= 1 (count (sut/rope [1]))) "a rope with one element has a count of one")
    (t/is (= 1 (count (sut/snip (sut/rope (range 5)) 1))) "a snipped rope keeps its count")
    (t/is (= 2 (count (sut/concat [1] [2]))) "concatenated ropes add their count"))
  (t/testing "ropes are indexed"
    (t/is (= 1 (nth (sut/rope [0 1 2]) 1))))
  (t/testing "ropes have collection equality"
    (t/is (= [1 2 3] (sut/rope [1 2 3])) "single node ropes are equal to collections")
    (t/is (= [1 2 3] (conj (sut/rope [1 2]) 3)) "multi-node ropes are equal to collections")
    (t/is (= (seq "abc") (sut/rope "abc")) "ropes from strings are equal to sequences from strings")
    (t/is (= (doto (ArrayList.)
               (.add 1)
               (.add 2)
               (.add 3))
             (sut/rope [1 2 3]))
          "ropes are equal to java.util.Lists"))
  (t/testing "ropes can have metadata"
    (t/is (= {:a 1} (meta (with-meta (sut/rope) {:a 1}))) "meta fetched from a rope it's stored on is the same"))
  (t/testing "ropes conform to the hash spec"
    (t/is (= (hash (sut/rope [1 2 3])) (hash [1 2 3]))
          "a rope's hash is the same as a vector's with the same elements"))
  (t/testing "ropes are reducible"
    (t/is (let [s [1 2 3]]
            (= (reduce + 0 s) (reduce + 0 (sut/rope s))))
          "reducing a function over a sequence and a rope produces the same value")
    (t/is (= 0 (reduce + (sut/rope)))
          "reducing without an init value and zero elements calls the function with zero arguments")
    (t/is (= 1 (reduce (constantly :fail) (sut/rope [1])))
          "reducing without an init value and one element returns the element without calling the function")))

(t/deftest construction
  (t/testing "empty ropes"
    (t/is (= [] (sut/rope)) "empty ropes are equal to empty collections")
    (t/is (= [] (empty (sut/rope [1 2 3]))) "empty ropes constructed with empty are equal to empty collections"))
  (t/testing "ropes with elements"
    (t/is (pos? (count (sut/rope [1 2 3]))) "ropes can be constructed with multiple elements"))
  (t/testing "ropes are ropes"
    (t/is (sut/rope? (sut/rope)) "empty ropes are ropes")
    (t/is (sut/rope? (sut/rope [1 2 3])) "ropes with elements are ropes")))

(t/deftest manipulation
  (t/testing "concatenation"
    (t/is (= [1] (sut/concat [] [1])) "an empty rope concatenated with one with elements has elements")
    (t/is (= [1 2] (sut/concat [1] [2])) "two ropes concatenated together have both their elements")
    (t/is (= [1 2 3] (sut/concat [1] [2] [3])) "three ropes concatenated together have all their elements"))
  (t/testing "snipping"
    (t/is (= [1 3] (sut/snip [1 2 3] 1 2)) "elements are snipped from the middle")
    (t/is (= [1] (sut/snip [1 2 3] 1)) "elements are snipped till the end")
    (t/is (= [] (sut/snip [1 2 3] 0)) "all elements can be snipped"))
  (t/testing "insertion"
    (t/is (= [1 :blah :blah2 2 3] (sut/insert [1 2 3] 1 [:blah :blah2]))
          "insert adds elements in the middle of ropes")
    (t/is (= [:blah 1 2 3] (sut/insert [1 2 3] 0 [:blah]))
          "insert adds elements to the beginning of ropes"))
  (t/testing "replacement"
    (t/is (= [1 :blah :blah2 3] (sut/replace [1 2 3] 1 2 [:blah :blah2]))
          "replaces elements from the middle of a rope")
    (t/is (= [1 :blah :blah2 2 3] (sut/replace [1 2 3] 1 1 [:blah :blah2]))
          "acts as insert with equal start and end"))
  (t/testing "view"
    (t/is (= [:blah :blah2] (sut/view [1 2 :blah :blah2 3] 2 4))
          "grabs elements from the middle of the rope")
    (t/is (= [:blah :blah2] (sut/view [1 2 3 :blah :blah2] 3))
          "grabs elements to the end")))

(t/deftest serialization
  (t/testing "print-dup"
    (t/is (let [r (sut/rope [1 2 3])]
            (= r (read-string (binding [*print-dup* true]
                                (pr-str r)))))
          "read ropes are equal to constructed ones")
    (t/is (let [m {:meta ::metadata}
                r (with-meta (sut/rope [1 2 3]) m)]
            (= m (meta (read-string (binding [*print-dup* true]
                                      (pr-str r))))))
          "metadata of ropes serialized with print-dup is retained after reading back in"))
  (t/testing "print-method"
    (t/is (let [r (sut/rope [1 2 3])]
            (= r (edn/read-string {:readers {'rope sut/rope}} (pr-str r))))
          "read ropes are equal to constructed ones")))

(t/deftest regex
  (t/is (re-matches #"a+" (sut/rope "aaaaa"))
        "flat ropes match regexes")
  (t/is (re-matches #"a+" (into (sut/rope) (repeat 1000 \a)))
        "deep ropes match regexes"))
