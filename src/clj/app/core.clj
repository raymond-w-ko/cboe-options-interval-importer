(ns app.core
  (:import
   [java.io File]
   [java.util HashMap])
  (:require
   [clj-async-profiler.core :as prof]
   [taoensso.timbre :as timbre
    :refer [log  trace  debug  info  warn  error  fatal  report
            logf tracef debugf infof warnf errorf fatalf reportf]]
   [clojure.string :as str]
   [app.config :as config]
   [app.lmdb :as lmdb]
   [app.option-intervals :as oi]
   [app.prices :as prices]
   [app.macros :as mac :refer [->hash field cond-let cond-xlet]]))

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

(defn is-put? [^"[Ljava.lang.String;" line] (= "P" (field line "option_type")))
(defn is-spxw-root? [^"[Ljava.lang.String;" line] (= "SPXW" (field line "root")))

(defn import-puts []
  (let [zip-files-xf (comp)
        line-xf (comp (filter is-put?)
                      (filter is-spxw-root?))
        args (merge
              (oi/create-base-args)
              {:src-dir config/spx-zip-dir
               :lmdb-env-dir config/spx-puts-lmdb-dir
               :db-name "SPXW"
               :zip-files-xf zip-files-xf
               :line-xf line-xf
               :write-to-lmdb true})]
    (oi/import! args)))

(defn profiled-run []
  (prof/profile (import-puts)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-underlying-data-from-line [^"[Ljava.lang.String;" line]
  (let [quote-datetime (field line "quote_datetime")
        bid (field line "underlying_bid")
        ask (field line "underlying_ask")
        price (field line "active_underlying_price")]
    (->hash quote-datetime bid ask price)))

(defn extract-underlying-data []
  (let [zip-files-xf (comp)
        gen-fns
        (fn [args]
          (let [capacity (-> (/ 252 12)
                             (* (+ 390 15))
                             (+ 128))
                hm (new java.util.HashMap)
                process-line!
                (fn [^"[Ljava.lang.String;" line]
                  (let [^String qdt (field line "quote_datetime")]
                    (when-not (.containsKey hm qdt)
                      (.put hm qdt (get-underlying-data-from-line line)))
                    nil))]
            {:line-xf (map process-line!)
             :post-process-fn
             (fn [{:keys [fname]}]
               (let [xs (-> hm .values seq vec)
                     yyyy-mm (re-find #"\d\d\d\d-\d\d" fname)
                     path (str "out/" yyyy-mm ".edn")]
                 (assert yyyy-mm)
                 (infof "%s has %d quotes" yyyy-mm (count xs))
                 (spit path (pr-str xs))))}))
        args (merge
              (oi/create-base-args)
              {:src-dir config/spx-zip-dir
               :lmdb-env-dir config/spx-puts-lmdb-dir
               :db-name "SPXW"
               :zip-files-xf zip-files-xf
               :gen-fns gen-fns
               :write-to-lmdb false})]
    (oi/import! args)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn print-oi-db-stats []
  (with-open [env (lmdb/create-read-env config/spx-puts-lmdb-dir)]
    (with-open [db (lmdb/open-db env "SPXW")]
      (lmdb/print-stats env db))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn import-prices []
  (let [args {:edn-src-dir "out/"
              :lmdb-env-dir config/spx-prices-dir
              :db-name "SPX"}]
    (prices/import! args)))

(defn read+print-some-prices []
  (let [args {:lmdb-env-dir config/spx-prices-dir
              :db-name "SPX"}]
    (prices/read+print-some-prices args)))

(defn print-prices-db-stats []
  (with-open [env (lmdb/create-read-env config/spx-prices-dir)]
    (with-open [db (lmdb/open-db env "SPX")]
      (lmdb/print-stats env db))))

(defn write-prices-parquet! []
  (let [args {:lmdb-env-dir config/spx-prices-dir
              :db-name "SPX"
              :output-path "spx.parquet"}]
    (prices/write-parquet! args)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn -main []
  (println (/ (-> (Runtime/getRuntime) .maxMemory) 1024 1024) "G")

  (println "BEGIN")
  (println "END"))
