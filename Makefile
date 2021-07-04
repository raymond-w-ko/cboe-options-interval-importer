JVM_ARGS := -J-Dclojure.core.async.go-checking=true

repl:
	clojure $(JVM_ARGS) -M:repl
run:
	clojure $(JVM_ARGS) -M:none -m app.core
upgrade:
	clojure -M:outdated
