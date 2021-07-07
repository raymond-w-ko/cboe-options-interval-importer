#!/usr/bin/bash
set -euo pipefail

clojure -J-Xmx4G -M:none -m app.worker "$@"
