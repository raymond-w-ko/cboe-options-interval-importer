(ns build
  (:require [badigeon.javac :as j]))

(defn javac []
  (println "Compiling Java")
  (j/javac "src" {:compile-path     "classes"
                  :compiler-options ["-cp" "src:classes" "-target" "1.10"
                                     "-source" "1.10" "-Xlint:-options"]})
  (println "Compilation Completed"))

(defn -main []
  (javac))
