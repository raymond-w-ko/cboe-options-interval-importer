(ns app.core
  (:import
   [java.io File])
  (:require
   [clj-async-profiler.core :as prof]
   [app.option-intervals :as oi]
   [app.config :as config]
   [taoensso.timbre :as timbre
    :refer [log  trace  debug  info  warn  error  fatal  report
            logf tracef debugf infof warnf errorf fatalf reportf]]
   [clojure.string :as str]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn >=-than-year? [year ^File file]
  (let [path (.getPath file)
        a (str/last-index-of path "/")
        path (subs path (inc a))
        b (str/index-of path "-")
        path (subs path 0 b)
        c (str/last-index-of path "_")
        token (subs path (inc c))]
    (>= (Integer/parseInt token) year)))

(defn import-puts []
  (let [zip-files-xf (comp (filter (partial >=-than-year? 2016))
                           (take 1))
        args (merge
              (oi/create-base-args)
              {:src-dir config/spx-zip-dir
               :lmdb-env-dir config/spx-puts-lmdb-dir
               :zip-files-xf zip-files-xf})]
    (oi/import! args)))

(defn profiled-run []
  (prof/profile (import-puts)))

(defn -main []
  (println (/ (-> (Runtime/getRuntime) .maxMemory) 1024 1024) "G")

  (println "BEGIN")
  (println "END"))
