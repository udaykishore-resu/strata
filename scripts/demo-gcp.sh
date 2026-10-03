#!/usr/bin/env bash
# Guided Strata demo against a REAL Google Cloud project.
#
# Runs the engine on your laptop (in-memory state, your gcloud credentials),
# then: deploy a Cloud Run service + bucket → break it and watch the
# automatic rollback → change it by hand and detect drift → destroy.
# No Cloud SQL and no bootstrap needed; costs pennies; ~5 minutes.
#
#   PROJECT_ID=my-sandbox ./scripts/demo-gcp.sh
#   NO_PAUSE=1 ...          run straight through (no "press enter" stops)
#   STRATA_PROVIDER=fake ...rehearse offline against the fake cloud
set -euo pipefail
cd "$(dirname "$0")/.."

PROVIDER=${STRATA_PROVIDER:-gcp}
REGION=${REGION:-us-central1}
PORT=${PORT:-8085}
STACK=hello
export STRATA_SERVER="http://localhost:${PORT}"

bold() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
note() { printf '\033[2m%s\033[0m\n' "$*"; }
pause() { [[ -n "${NO_PAUSE:-}" ]] || read -rp $'\033[33m↵ press enter: '"$1"$'\033[0m ' _; }
die() { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }

# ------------------------------------------------------------- preflight
if [[ "$PROVIDER" == "gcp" ]]; then
  command -v gcloud >/dev/null || die "gcloud is not installed"
  PROJECT_ID=${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}
  [[ -n "$PROJECT_ID" ]] || die "set PROJECT_ID (use a sandbox project)"
  gcloud auth application-default print-access-token >/dev/null 2>&1 ||
    die "no application-default credentials: run 'gcloud auth application-default login'"
  bold "Project $PROJECT_ID ($REGION)"
  note "Strata enables the APIs each stack needs; it only needs these two to start:"
  gcloud services enable serviceusage.googleapis.com cloudresourcemanager.googleapis.com --project "$PROJECT_ID"
else
  PROJECT_ID=${PROJECT_ID:-fake-project}
  bold "Rehearsal mode: fake cloud, nothing is created in GCP"
fi

# ------------------------------------------------------- engine (local)
go build -o bin/strata-server ./cmd/strata-server
go build -o bin/strata ./cmd/strata
LOG=/tmp/strata-demo-gcp.log
PORT=$PORT STRATA_STORE=memory STRATA_PROVIDER=$PROVIDER STRATA_AUTH=none \
STRATA_PROJECT=$PROJECT_ID STRATA_REGION=$REGION STRATA_QUOTA_PROJECT=$PROJECT_ID \
STRATA_FAKE_LATENCY=400ms STRATA_FAKE_FAULTS="update:Web*1" STRATA_LOG_FORMAT=text \
  ./bin/strata-server >"$LOG" 2>&1 &
SERVER_PID=$!
# State lives in this engine's memory, so if the script stops early for any
# reason (error, Ctrl-C) destroy the stack before the engine goes away;
# otherwise the resources would be left running with no record of them.
DEPLOYED=0
cleanup() {
  code=$?
  if [[ $DEPLOYED -eq 1 ]]; then
    printf '\n\033[33mstopping early: destroying stack %s so nothing is left running\033[0m\n' "$STACK"
    ./bin/strata destroy -s "$STACK" --yes || note "destroy failed; delete the resources by hand (see $LOG)"
  fi
  kill "$SERVER_PID" 2>/dev/null || true
  exit $code
}
trap cleanup EXIT
for _ in $(seq 1 50); do curl -fsS "$STRATA_SERVER/healthz" >/dev/null 2>&1 && break; sleep 0.2; done
curl -fsS "$STRATA_SERVER/healthz" >/dev/null || die "engine did not start; see $LOG"
note "engine running locally (logs: $LOG)"

strata() { ./bin/strata "$@"; }
output() { strata describe -s "$STACK" --json | python3 -c "import sys,json; print(json.load(sys.stdin)['outputs'].get('$1',''))"; }
resource() { strata describe -s "$STACK" --json | python3 -c "import sys,json; print(json.load(sys.stdin)['resources']['$1']['physicalId'])"; }

go run ./examples/hello-gcp -out strata.out >/dev/null
TEMPLATE=strata.out/hello.template.json

# --------------------------------------------------------------- 1 plan
bold "1/4  Go constructs → template → plan"
note "examples/hello-gcp/main.go: a bucket, a public Cloud Run service, one GrantRead call."
strata validate -f "$TEMPLATE"
pause "show the plan"
strata diff -s "$STACK" -f "$TEMPLATE"

# ------------------------------------------------------------- 2 deploy
pause "deploy"
bold "2/4  Deploy"
DEPLOYED=1
strata deploy -s "$STACK" -f "$TEMPLATE" --yes
URL=$(output Url)
if [[ "$PROVIDER" == "gcp" ]]; then
  note "calling $URL"
  for _ in $(seq 1 10); do curl -fsS "$URL" >/dev/null 2>&1 && break; sleep 3; done
  curl -fsS "$URL" | grep -o '<title>[^<]*</title>' || curl -fsSI "$URL" | sed -n 1p
fi

# ---------------------------------------------------- 3 failure + rollback
pause "break it: deploy an image that does not exist"
bold "3/4  Failed update → automatic rollback"
if [[ "$PROVIDER" == "fake" ]]; then
  note "(rehearsal: the fake cloud is told to reject this update)"
fi
set +e
strata deploy -s "$STACK" -f "$TEMPLATE" --yes \
  -p Image=us-docker.pkg.dev/cloudrun/container/does-not-exist
code=$?
set -e
[[ $code -eq 3 ]] && note "exit code 3: the deploy failed and was rolled back, as intended" ||
  note "unexpected exit code $code"
strata describe -s "$STACK" | sed -n 1,4p
if [[ "$PROVIDER" == "gcp" ]]; then
  note "the service still answers with the previous revision:"
  curl -fsS -o /dev/null -w '%{http_code}\n' "$URL"
fi

# ---------------------------------------------------------------- 4 drift
pause "change the bucket by hand, then detect drift"
bold "4/4  Out-of-band change → drift detection → redeploy"
BUCKET=$(resource Assets)
if [[ "$PROVIDER" == "gcp" ]]; then
  note "gcloud storage buckets update gs://$BUCKET --no-versioning"
  gcloud storage buckets update "gs://$BUCKET" --no-versioning --quiet
  # `strata drift` exits 2 when it finds drift; that is the expected result.
  set +e; strata drift -s "$STACK"; set -e
  pause "redeploy: the plan refreshes live state and restores versioning"
  strata deploy -s "$STACK" -f "$TEMPLATE" --yes
  set +e; strata drift -s "$STACK"; code=$?; set -e
  [[ $code -eq 0 ]] && note "back in sync" || note "still drifted (exit $code)"
else
  note "(rehearsal: drift needs the real gcloud CLI; showing a clean drift check)"
  set +e; strata drift -s "$STACK"; set -e
fi

# --------------------------------------------------------------- cleanup
# The engine keeps state in memory for this demo, so always clean up here.
pause "destroy everything"
bold "Destroy"
strata destroy -s "$STACK" --yes
DEPLOYED=0
printf '\n\033[1;32mDemo complete.\033[0m Enabled APIs are left on (shared project setting).\n'
