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
   [app.s3 :refer [exists-object? get-zip-object-keys]]
   [clojure.string :as str]))

(defonce batch (-> (AWSBatchClientBuilder/standard)
                   (.withCredentials creds-provider)
                   (.withRegion "us-east-1")
                   (.build)))

(defn submit-jobs [verb & {:keys [one]}]
  (let [zip-keys (get-zip-object-keys)]
    (dorun
     (for [object-key zip-keys]
       (let [core-filename (->core-filename object-key)
             done-object-key (format "extract-underlying-quotes/%s.done" core-filename)
             cmd ["/task/worker.sh" verb object-key]
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
