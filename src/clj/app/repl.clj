(ns app.repl)

(defn sort-roots []
  (->> (slurp "roots.edn")
       (read-string)
       (sort)
       (vec)
       (pr-str)
       (spit "roots.edn")))
(comment (sort-roots))

(defn max-root-size []
  (->> (slurp "roots.edn")
       (read-string)
       (map count)
       (apply max)))
(comment (max-root-size))
