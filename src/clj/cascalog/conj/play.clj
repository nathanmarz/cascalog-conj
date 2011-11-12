(ns cascalog.conj.play
  (:use [cascalog api playground])
  (:require [cascalog [ops :as c] [vars :as v]]))

(bootstrap-emacs)

(comment
  ;; People who are less than 30 years old
  (?<- (stdout) [?person]
       (age ?person ?age)
       (< ?age 30))

  ;; People who are less than 30 years old with their ages
  (?<- (stdout) [?person ?age]
       (age ?person ?age)
       (< ?age 30))

  ;; Numbers from "integer" dataset that equal themselves when squared
  (?<- (stdout) [?n]
       (integer ?n)
       (* ?n ?n :> ?n))

  ;; Numbers from "integer" dataset that equal themselves when cubed
  (?<- (stdout) [?n]
       (integer ?n)
       (* ?n ?n ?n :> ?n))

  ;; Age and gender of every person in both "age" and "gender" datasets
  (?<- (stdout) [?person ?age ?gender]
       (age ?person ?age)
       (gender ?person ?gender))

  ;; All "follows" relationships where the follower is older than the followed
  (?<- (stdout) [?person1 ?person2]
       (age ?person1 ?age1)
       (follows ?person1 ?person2)
       (age ?person2 ?age2)
       (< ?age2 ?age1))

  ;; Number of people less than 30 years old
  (?<- (stdout) [?count]
       (age _ ?age)
       (< ?age 30)
       (c/count ?count))

  ;; Number of people each person follows
  (?<- (stdout) [?person ?count]
       (follows ?person _)
       (c/count ?count))

  ;; All follows relationships where each person follows more than 2 people
  (let [many-follows (<- [?person]
                         (follows ?person _)
                         (c/count ?count)
                         (> ?count 2))]
    (?<- (stdout) [?person1 ?person2]
         (many-follows ?person1)
         (many-follows ?person2)
         (follows ?person1 ?person2)))


  ;; An operation that emits many word tuples form a single sentence tuple
  (defmapcatop split [sentence]
    (seq (.split sentence "\\s+")))

  ;; Word count in Cascalog
  (?<- (stdout) [?word ?count]
       (sentence ?sentence)
       (split ?sentence :> ?word)
       (c/count ?count))
  )


