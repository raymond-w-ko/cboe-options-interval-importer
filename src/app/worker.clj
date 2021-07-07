(ns app.worker
  (:require
   [taoensso.timbre :as timbre
    :refer [log  trace  debug  info  warn  error  fatal  report
            logf tracef debugf infof warnf errorf fatalf reportf
            spy get-env]]

   [app.macros :refer [->hash field cond-let]]
   [app.utils :refer [str->bytes ->core-filename]]

   [app.s3 :refer [zip-object-key->line-seq
                   open-object-output-stream! close-object-output-stream!
                   put-string-object]]))

(defn extract-underlying-quotes [object-key]
  (let [core-filename (->core-filename object-key)
        done-object-key (format "extract-underlying-quotes/%s.done" core-filename)

        {:keys [lines]} (zip-object-key->line-seq object-key)


        ->kv
        (fn [line]
          (let [arr (.split line ",")

                underlying_symbol (field arr "underlying_symbol")
                quote_datetime (field arr "quote_datetime")
                bid (field arr "underlying_bid")
                ask (field arr "underlying_ask")
                active_price (field arr "active_underlying_price")
                k [underlying_symbol quote_datetime]
                v [bid ask active_price]]
            [k v]))
        xf (comp (map ->kv))
        rf
        (fn [m [k v]]
          (if (contains? m k)
            (when (not= v (get m k))
              (error "mismatch bid/ask/active_price" k v (get m k))
              (assert false)))
          (assoc! m k v))

        quotes-edn-string
        (->> (transduce xf (completing rf) (transient {}) lines)
             (persistent!)
             (pr-str)
             (str->bytes))]

    (let [{:as stream :keys [output-stream]}
          (open-object-output-stream! (format "quotes/%s.edn" core-filename))]
      (.write output-stream quotes-edn-string)
      (close-object-output-stream! stream))

    (put-string-object done-object-key "DONE!")))

(defn -main [verb & args]
  (let [[arg0] args]
    (case verb
      "extract-underlying-quotes" (extract-underlying-quotes arg0)
      (println "unknown command: " verb args))))
