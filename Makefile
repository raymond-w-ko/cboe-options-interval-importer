IMAGE := 237991343424.dkr.ecr.us-east-1.amazonaws.com/cboe:latest
JVM_ARGS := -J-Dclojure.core.async.go-checking=true

repl:
	clojure $(JVM_ARGS) -M:repl
run:
	clojure -J-Xmx16G -M:none -m app.core
upgrade:
	clojure -M:outdated --upgrade


build-worker-image:
	docker build -t $(IMAGE) .
sh: build-worker
	docker run -it $(IMAGE)
push-worker-image: build-worker-image
	docker push $(IMAGE)

submit-jobs:
	clojure -M:none -m app.batch

reduce:
	clojure -M:none -m app.reduce
