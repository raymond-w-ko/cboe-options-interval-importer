(ns app.batch
  (:import
   [java.util ArrayList]
   [com.amazonaws.auth BasicAWSCredentials AWSStaticCredentialsProvider]
   [com.amazonaws.services.batch AWSBatchClientBuilder]
   [com.amazonaws.services.batch.model SubmitJobRequest ContainerOverrides])

  (:require
   [app.creds :refer [creds-provider]]
   [app.config :refer [aws-access-key-id aws-secret-access-key
                       zip-bucket zip-bucket-dir]]
   [app.utils :refer [->core-filename]]
   [app.sentinels :refer [->done-object-key]]
   [app.s3 :refer [exists-object? get-zip-object-keys]]
   [clojure.string :as str]))

(set! *warn-on-reflection* true)

(defonce batch (-> (AWSBatchClientBuilder/standard)
                   (.withCredentials creds-provider)
                   (.withRegion "us-east-1")
                   (.build)))

(defn submit-jobs [op & {:keys [one]}]
  (let [zip-keys (cond->> (get-zip-object-keys)
                   one (take 1))]
    (dorun
     (for [object-key zip-keys]
       (let [core-filename (->core-filename object-key)
             done-object-key (->done-object-key op core-filename)
             cmd ["/task/worker.sh" op object-key]
             overrides (-> (ContainerOverrides.)
                           (.withCommand (ArrayList. cmd)))
             jr (-> (SubmitJobRequest.)
                    (.withJobDefinition "cboe:2")
                    (.withJobQueue "markets")
                    (.withContainerOverrides overrides)
                    (.withJobName core-filename))]
         (when-not (exists-object? done-object-key)
           (println cmd)
           (-> (.submitJob batch jr)
               (.toString)
               (println))
           nil))))))

(defn -main []
  (submit-jobs "extract-underlying-quotes"))
