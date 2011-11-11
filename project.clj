(defproject cascalog-conj "1.0.0"
  :source-path "src/clj"
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.clojure/data.json "0.1.1"]
		 [cascalog "1.8.3"]
		 [cascalog-contrib "1.0.0-SNAPSHOT"]
                 ]
  :dev-dependencies [
                     [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [swank-clojure "1.2.1"]])
