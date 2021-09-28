(ns app.worker
  (:import
   [java.io InputStream OutputStream]
   [java.util.zip GZIPOutputStream])
  (:require
   [taoensso.timbre :as timbre
    :refer [log  trace  debug  info  warn  error  fatal  report
            logf tracef debugf infof warnf errorf fatalf reportf
            spy get-env]]

   [app.macros :as mac :refer [->hash field cond-let]]
   [app.utils :refer [str->bytes ->core-filename]]
   [app.sentinels :refer [->done-object-key]]

   [app.s3 :refer [zip-object-key->line-seq
                   open-object-output-stream! close-object-output-stream!
                   zip-object-key->input-stream put-string-object]]))

(set! *warn-on-reflection* true)

(defn extract-underlying-quotes [op object-key]
  (let [core-filename (->core-filename object-key)
        done-object-key (->done-object-key op core-filename)

        {:keys [lines]} (zip-object-key->line-seq object-key)

        ->kv
        (fn [^String line]
          (let [arr (.split line ",")

                underlying_symbol (field arr "underlying_symbol")
                quote_datetime (field arr "quote_datetime")
                bid (field arr "underlying_bid")
                ask (field arr "underlying_ask")
                active_price (field arr "active_underlying_price")
                k [underlying_symbol quote_datetime]
                v [bid ask active_price]]
            [k v]))
        xf (comp (drop 1)
                 (map ->kv))
        rf
        (fn [m [k v]]
          ; (if (contains? m k)
          ;   (when (not= v (get m k))
          ;     (error "mismatch bid/ask/active_price" k v (get m k))
          ;     (assert false)))
          (assoc! m k v))

        ^bytes quotes-edn-string
        (->> (transduce xf (completing rf) (transient {}) lines)
             (persistent!)
             (pr-str)
             (str->bytes))]

    (let [{:as stream :keys [^OutputStream output-stream]}
          (open-object-output-stream! (format "quotes/%s.edn" core-filename))]
      (.write output-stream quotes-edn-string)
      (close-object-output-stream! stream))

    (put-string-object done-object-key "DONE!")))

(defn convert-to-gzip [op object-key]
  (let [core-filename (->core-filename object-key)
        done-object-key (->done-object-key op core-filename)

        zip-input-stream (zip-object-key->input-stream object-key)
        entry (.getNextEntry zip-input-stream)
        fname (.getName entry)

        {:as stream :keys [output-stream]}
        (open-object-output-stream! (format "spx.gz/%s.csv.gz" core-filename))
        
        gzip-os (GZIPOutputStream. output-stream)
        buf (byte-array (* 1024 1024 64))]

    ;; drop header row
    (loop [b (.read zip-input-stream)]
      (println b)
      (when-not (= 0x0a b)
        (recur (.read zip-input-stream))))
    (info "skipped CSV header row")

    (loop [n (.read zip-input-stream buf)]
      (when (not= -1 n)
        (.write gzip-os buf 0 n)
        (recur (.read zip-input-stream buf))))
    (.finish gzip-os)

    (close-object-output-stream! stream)

    (put-string-object done-object-key "DONE!")))

(defn extract-options [op object-key]
  (let [core-filename (->core-filename object-key)
        done-object-key (->done-object-key op core-filename)

        {:keys [lines]} (zip-object-key->line-seq object-key)

        ->k
        (fn [^String line]
          (let [arr (.split line ",")
                underlying_symbol (field arr "underlying_symbol")
                root (field arr "root")
                expiration (field arr "expiration")
                strike (field arr "strike")
                option_type (field arr "option_type")]
            [underlying_symbol root expiration strike option_type]))
        xf (comp (drop 1)
                 (map ->k))
        rf
        (fn [se k]
          (conj! se k))
        ^bytes options-edn-string
        (->> (transduce xf (completing rf) (transient #{}) lines)
             (persistent!)
             (pr-str)
             (str->bytes))]

    (let [{:as stream :keys [^OutputStream output-stream]}
          (open-object-output-stream! (format "options/%s.edn" core-filename))]
      (.write output-stream options-edn-string)
      (close-object-output-stream! stream))

    (put-string-object done-object-key "DONE!")))

(defn -main [op & args]
  (let [[arg0] args]
    (case op
      "extract-underlying-quotes" (extract-underlying-quotes op arg0)
      "convert-to-gzip" (convert-to-gzip op arg0)
      "extract-options" (extract-options op arg0)
      (println "unknown command: " op args))))
