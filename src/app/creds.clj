(ns app.creds
  (:import
   [com.amazonaws.auth BasicAWSCredentials AWSStaticCredentialsProvider])
  (:require
   [app.config :refer [aws-access-key-id aws-secret-access-key ]]))

(defonce credentials (BasicAWSCredentials. aws-access-key-id aws-secret-access-key))
(defonce creds-provider (AWSStaticCredentialsProvider. credentials))
