(ns app.core
  (:require
   [taoensso.timbre :as timbre
    :refer [log  trace  debug  info  warn  error  fatal  report
            logf tracef debugf infof warnf errorf fatalf reportf
            spy get-env]]
   [tick.alpha.api :as t]

   ; [app.options :as options]
   ; [app.option-intervals :as option-intervals]
   ; [app.prices :as prices]
   
   [app.lmdb]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(set! *warn-on-reflection* true)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn internal-consistency-check! []
  (assert (= (t/date "1970-01-01") (t/date "1970-01-01")))
  (assert (not= (t/date "1970-01-01") (t/date "1970-01-02"))))

(defn -main []
  (println (/ (-> (Runtime/getRuntime) .maxMemory) 1024 1024) "G")
  (internal-consistency-check!)

  (println "BEGIN")
  ; (option-intervals/run)
  ; (prices/run)
  (println "END"))
