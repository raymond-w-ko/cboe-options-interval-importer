(ns app.reduce
  (:import
   [java.io ByteArrayInputStream StringWriter]
   [java.util.zip ZipInputStream]
   [java.io LineNumberReader InputStreamReader BufferedReader]
   [com.amazonaws.services.s3.model S3Object])
  (:require
   [taoensso.timbre :as timbre
    :refer [log  trace  debug  info  warn  error  fatal  report
            logf tracef debugf infof warnf errorf fatalf reportf
            spy get-env]]
   [clojure.edn :as edn]
   [clojure.core.async
    :as async
    :refer [go go-loop >! >!! <! <!! chan put! thread timeout close! onto-chan to-chan
            pipeline pipeline-blocking]]
   [clojure.java.io :as io]
   [fipp.edn :refer [pprint] :rename {pprint fipp}]

   [app.macros :refer [->hash field cond-let]]
   [app.db :refer [create-db-connection
                   start-bulk-loading!
                   create-insert-option-pstmt insert-option-batch
                   create-insert-price-pstmt insert-price-batch]]
   [app.s3 :as s3 :refer [get-objects get-object-keys get-object]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(set! *warn-on-reflection* true)

(defn start-measurement-loop [*num-items]
  (let [start-instant (System/currentTimeMillis)]
    (go-loop []
      (let [ms (- (System/currentTimeMillis) start-instant)
            sec (/ ms 1000.0)
            rate (/ @*num-items sec)]
        (debug "processing rate" (int rate) "items/sec"))
      (<! (timeout (* 2 1000)))
      (recur))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn reduce-options []
  (let [option-keys (->> (get-object-keys "options/")
                         (to-chan))
        ch (chan 8)
        *num-items (atom 0)

        ->edn
        (fn [object-key]
          (with-open [^S3Object obj (get-object object-key)]
            (let [sw (StringWriter.)
                  rdr (-> obj .getObjectContent io/reader)]
              (.transferTo rdr sw)
              (let [options (edn/read-string (.toString sw))]
                options))))

        fix-strike (fn [x]
                     (update x 3 pr-str))

        xf (comp (map ->edn)
                 (mapcat identity)
                 (map fix-strike))]

    (start-measurement-loop *num-items)
    (pipeline-blocking 4 ch xf option-keys)

    (let [unique-options-ch (->> (async/into [] ch)
                                 (<!!)
                                 (distinct)
                                 (partition-all 20000)
                                 (to-chan)) 
          db (create-db-connection)
          _ (start-bulk-loading! db)
          insert-option-pstmt (create-insert-option-pstmt db)
          args (->hash insert-option-pstmt)]
      (loop [options (<!! unique-options-ch)]
        (when options
          (insert-option-batch args options)
          (swap! *num-items + (count options))
          (recur (<!! unique-options-ch)))))
    :done))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn reduce-quotes []
  (let [object-keys (->> (get-object-keys "quotes/")
                         (to-chan))
        ch (chan 8)
        *num-items (atom 0)

        ->edn
        (fn [object-key]
          (with-open [^S3Object obj (get-object object-key)]
            (let [sw (StringWriter.)
                  rdr (-> obj .getObjectContent io/reader)]
              (.transferTo rdr sw)
              (let [options (edn/read-string (.toString sw))]
                options))))

        flatten-kv (fn [[[sym quote_datetime] [bid ask active_price]]]
                     [sym quote_datetime bid ask active_price])

        xf (comp (map ->edn)
                 (mapcat identity)
                 (map flatten-kv)
                 (partition-all 100000))]

    (start-measurement-loop *num-items)
    (pipeline-blocking 4 ch xf object-keys)

    (let [db (create-db-connection)
          _ (start-bulk-loading! db)
          insert-price-pstmt (create-insert-price-pstmt db)
          args (->hash insert-price-pstmt)]
      (loop [options (<!! ch)]
        (when options
          (insert-price-batch args options)
          (swap! *num-items + (count options))
          (recur (<!! ch)))))
    :done))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn -main []
  (reduce-options)
  ; (reduce-quotes)
  nil)
