(ns app.sentinels)

(defn ->done-object-key [op core-filename]
  (format "%s/%s.done" op core-filename))
