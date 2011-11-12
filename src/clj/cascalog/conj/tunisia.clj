(ns cascalog.conj.tunisia
  (:use [cascalog api playground])
  (:use [cascalog.contrib.incanter])
  (:use [cascalog.util :only [collectify]])
  (:use [clojure.contrib.def :only [defnk]])
  (:require [cascalog [ops :as c] [vars :as v]])
  (:require [clojure.data [json :as json]]))

(bootstrap-emacs)

(defn- parse-str [^String s]
  (seq (.split s "\t")))

(defn longify [str]
  (Long/parseLong str))

(defmapcatop split [sentence]
    (seq (.split sentence "\\s+")))

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
  ;; bar?- is from cascalog-contrib and creates a bar graph from the
  ;; passed in subquery. This query here graphs the follower count distribution
  (bar?- (<- [?bucket ?count]
             (followers-count-data _ ?fc)
             (bucketize ?fc :> ?bucket)
             (c/count ?count))
         :x-label "# followers"
         :y-label "# people"
         :title "Follower distribution")


  )
  
(def location-tweets
  (<- [?location ?amt]
      (location-data ?person ?location)
      (reactor-data ?person _)
      (c/count ?amt)))

(def top-locations
  (<- [?location ?amt]
      (location-tweets ?location-in ?amt-in)
      (:sort ?amt-in) (:reverse true)
      (c/limit [100] ?location-in ?amt-in :> ?location ?amt)))

(comment
  (defnk first-n
    [gen n :sort nil :reverse false]
    (let [in-fields (get-out-fields gen)
          out-vars  (v/gen-nullable-vars (count in-fields))]
      (<- out-vars
          (gen :>> in-fields)
          (:sort :<< (if sort (collectify sort)))
          (:reverse reverse)
          (c/limit [n] :<< in-fields :>> out-vars))))
  )

(def top-locations
  (c/first-n location-tweets 100 :sort "?amt" :reverse true))

(def retweet-counts
  (<- [?tweet ?count]
      (reaction-data ?reaction ?tweet)
      (c/count ?count)))

(def top-tweets
  (c/first-n retweet-counts 100 :sort "?count" :reverse true))

(def total-retweets
  (<- [?reactor ?total]
      (reaction-data _ ?to)
      (reactor-data ?reactor ?to)
      (c/count ?total)))

(def comparatively-influential
  (<- [?bucket ?influencer ?name-out ?count-out ?rank]
      (followers-count-data ?person ?fc)
      (total-retweets ?person ?count)
      (name-data ?person ?name)
      (bucketize ?fc :> ?bucket)
      (:sort ?count) (:reverse true)
      (c/limit-rank [10] ?person ?name ?count :> ?influencer ?name-out ?count-out ?rank)))

(defn to-lower [^String s]
  (.toLowerCase s))

(def stop-words #{"and" "the" "a" "of" "in" "i" "to" "for" "&" "my" "is" "on" "with" "at" "i'm" "de"
"-" "you" "from" "am" "all" "me" "about" "an" "it" "be" "that" "by" "who"
"not" "are" "your" "en" "la" "as" "we" "/" "but" "do" "this" "y" "one" "," "what" "et"
"e" "or" "also" "so" "our" "don't" "." "@" "!" "..."})

(defn valid-word? [word]
  (and (> (count word) 2)
       (not (contains? stop-words word))))

(def description-word-count
  (<- [?word ?count]
      (description-data _ ?description)
      (split ?description :> ?word-raw)
      (to-lower ?word-raw :> ?word)
      (valid-word? ?word)
      (c/count ?count)
      ))

(def description-tag-cloud
  (c/first-n description-word-count 100 :sort "?count" :reverse true))


(defn chained-pairs-simple [pairs chain-length]
  (let [out-vars (v/gen-nullable-vars chain-length)
        var-pairs (partition 2 1 out-vars)]
    (construct out-vars
               (concat
                (for [var-pair var-pairs]
                  [pairs :>> var-pair])
                [[:distinct false]]))
    ))


(defn attach-chains [chain1 chain2]
  (let [out1 (get-out-fields chain1)
        out2-chained (-> chain2 num-out-fields dec v/gen-nullable-vars)
        out-vars (concat out1 out2-chained)
        out2 (cons (last out1) out2-chained)]
    (<- out-vars
        (chain1 :>> out1)
        (chain2 :>> out2)
        (:distinct false))
    ))

(defn binary-rep
  [anum]
  (for [char (Integer/toBinaryString anum)]
    (if (= char \1) 1 0)
    ))

(defn chained-pairs-smart [pairs chain-length]
  (let [chains (iterate (fn [chain]
                          (attach-chains chain chain))
                        pairs)
        binary (-> chain-length dec binary-rep reverse)
        chains-to-use (filter identity
                       (map (fn [chain bit]
                              (if (= bit 1) chain))
                            chains
                            binary))]
    (reduce attach-chains chains-to-use)
    ))


;; look at any and all

(deffilterop bieberless? [description]
  (not (.contains (.toLowerCase description) "bieber")))

(deffilterop reasonable-size? [description]
  (> (count description) 20))

(deffilterop political? [description]
  (.contains (.toLowerCase description) "politic"))

(comment
  (?<- (stdout) [?description]
       (description-data _ ?description)
       (bieberless? ?description)
       (reasonable-size? ?description)
       (political? ?description))

  (?<- (stdout) [?description]
       (description-data _ ?description)
       ((c/all bieberless? reasonable-size? political?) ?description)
       )
  
  (defn all [& ops]    
    (construct
     [:<< "!invars" :>]       
     (map (fn [o] [o :<< "!invars"]) ops)
     ))

  (?<- (stdout) [?description]
       (description-data _ ?description)
       ((c/all (c/negate bieberless?)
               reasonable-size?
               political?)
        ?description))

  (?<- (stdout) [?description]
       (description-data _ ?description)
       ((c/negate (c/any bieberless?
                         reasonable-size?
                         political?))
        ?description))

  (defn bool-or [& vars]
    (boolean (some identity vars)))
  
  (defn any [& ops]
    (let [outvars (v/gen-nullable-vars (count ops))]
      (construct
       [:<< "!invars" :> "!true?"]
       (conj
        (map (fn [o v] [o :<< "!invars" :> v]) ops outvars)
        [#'bool-or :<< outvars :> "!true?"]))))
  )

;; go back to description-word-count and show use of c/comp to make it more concise



;; look at ops implementations
