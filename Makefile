IMAGE := 237991343424.dkr.ecr.us-east-1.amazonaws.com/cboe:latest

repl: rebel-repl
classic-repl:
	exec clojure -M:repl
rebel-repl:
	exec clojure -M:repl/rebel
run:
	exec clojure -J-Xmx16G -M:none -m app.core
upgrade-deps:
	exec clojure -M:outdated --upgrade
javac:
	exec clojure -T:build javac


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
