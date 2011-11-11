(ns cascalog.conj.tunisia
  (:use [cascalog api playground])
  (:use [cascalog.contrib.incanter])
  (:require [cascalog [ops :as c] [vars :as v]])
  (:require [clojure.data [json :as json]]))

(bootstrap-emacs)

(defn- parse-str [^String s]
  (seq (.split s "\t")))

(defn longify [str]
  (Long/parseLong str))

(def ROOT "/tmp/tunisia")

;; Parse reaction data into 2-tuples of longs
(def reaction-data
  (let [source (hfs-textline (str ROOT "/reaction"))]
    (<- [?reaction ?to]
        (source ?line)
        (parse-str ?line :> ?reaction-str ?to-str)
        (longify ?reaction-str :> ?reaction)
        (longify ?to-str :> ?to)
        (:distinct false))
    ))

;; Same as before, except more concise
(def reaction-data
  (let [source (hfs-textline  (str ROOT "/reaction"))]
    (<- [?reaction ?to]
        (source ?line)
        (parse-str ?line :> ?reaction-str ?to-str)
        ((c/each #'longify) ?reaction-str ?to-str :> ?reaction ?to)
        (:distinct false))
    ))

(defn- tab-parsed-longs
  "Query builder that parses any number of longs out of a tab delimited file"
  [name num-fields]
  (let [source (hfs-textline (str ROOT "/" name))
        int-vars (v/gen-nullable-vars num-fields)
        out-vars (v/gen-nullable-vars num-fields)]
    (<- out-vars
        (source ?line)
        (parse-str ?line :>> int-vars)
        ((c/each #'longify) :<< int-vars :>> out-vars)
        (:distinct false))
    ))

(def reaction-data
  (tab-parsed-longs "reaction" 2))

(def reactor-data
  (tab-parsed-longs "reactor" 2))

(defn- decode-property [^String json]
  (let [m (json/read-json json)]
    [(long (:id m)) (:val m)]))

(defn- property-data
  "Query builder that parses properties out of a json-encoded file"
  [name]
  (let [source (hfs-textline (str ROOT "/" name))]
    (<- [?id ?val]
        (source ?line)
        (decode-property ?line :> ?id ?val)
        (:distinct false))
    ))

(def description-data
  (property-data "description"))

(def name-data
  (property-data "name"))

(def following-count-data
  (property-data "following"))

(def followers-count-data
  (property-data "followers"))

(def statuses-count-data
  (property-data "statuses"))

(def location-data
  (property-data "location"))

(comment
  ;; names of people with more than 10000 followers
  (?<- (stdout) [?name]
       (followers-count-data ?person ?count)
       (name-data ?person ?name)
       (> ?count 10000))

  ;; Average number of followers
  (?<- (stdout) [?avg-followers]
       (followers-count-data _ ?followers)
       (c/count ?count)
       (c/sum ?followers :> ?sum)
       (div ?sum ?count :> ?avg-followers))

  ;; First attempt at abstracting average as its own operation
  (defbufferop bad-avg [tuples]
    (let [ages (map first tuples)]
      (div (reduce + tuples)
           (count tuples))
      ))

  ;; Using the inefficient version of average
  (?<- (stdout) [?avg-age]
       (age _ ?age)
       (bad-avg ?age :> ?avg-age))

  ;; The problem with this average definition is twofold. First of all,
  ;; the entire aggregation is done on the reducer, but average should
  ;; take advantage of map-side combiners. The second problem is that
  ;; this definition of average is re-implementing the "count" and "sum"
  ;; aggregators within it. Shouldn't there be a way to define average
  ;; in terms of other aggregators?

  ;; Here's how to define average as the composition of the count aggregator,
  ;; sum aggregator, and the division function. This is an example of a
  ;; "predicate macro". Since "count" and "sum" make use of map-side
  ;; combiners, average benefits from it as well. Note that this operation
  ;; is defined in Cascalog as cascalog.ops/avg
  (def good-avg
    (<- [!val :> !avg]
        (c/count !count)
        (c/sum !val :> !sum)
        (div !sum !count :> !avg)))

  ;; Here's how to use good-avg. Before compiling the query plan, Cascalog
  ;; expands good-avg into its constituent predicates of count, sum, and div
  ;; (hence why it's called "predicate macro").
  (?<- (stdout) [?avg-age]
       (age _ ?age)
       (good-avg ?age :> ?avg-age))
  )


(defn- bucketize [count]
  (->> [0 10 100 1000 10000 100000 nil]
       (partition 2 1)
       (filter (fn [[low up]]
                 (or (not up)
                     (and
                      (<= low count)
                      (< count up)))))
       first
       first))

(comment
  (bar?- (<- [?bucket ?count]
             (followers-count-data _ ?fc)
             (bucketize ?fc :> ?bucket)
             (c/count ?count))
         :x-label "# followers"
         :y-label "# people"
         :title "Follower distribution")


  )
  
  
