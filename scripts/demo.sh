#!/usr/bin/env bash
# End-to-end demo against a local dev server (in-memory store, fake cloud).
set -euo pipefail
cd "$(dirname "$0")/.."

PORT=${PORT:-8080}
export STRATA_SERVER="http://localhost:${PORT}"

go build -o bin/strata-server ./cmd/strata-server
go build -o bin/strata ./cmd/strata

PORT=$PORT ./bin/strata-server --dev >/tmp/strata-demo.log 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null || true' EXIT

for _ in $(seq 1 50); do
  curl -fsS "${STRATA_SERVER}/healthz" >/dev/null 2>&1 && break
  sleep 0.2
done

step() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

step "Synthesize the orders platform from Go constructs"
go run ./examples/orders-platform -out strata.out

step "Validate"
./bin/strata validate -f strata.out/orders.template.json

step "Deploy (create)"
./bin/strata deploy -s orders -f strata.out/orders.template.json -p Env=prod --yes

step "Describe"
./bin/strata describe -s orders

step "Change the API image: an in-place update"
./bin/strata deploy -s orders -f strata.out/orders.template.json -p Env=prod -p ApiImage=gcr.io/demo/orders-api:v2 --yes

step "Drift detection"
./bin/strata drift -s orders || true

step "Destroy"
./bin/strata destroy -s orders --yes

printf '\n\033[1;32mDemo complete.\033[0m Server log: /tmp/strata-demo.log\n'
