#!/usr/bin/env bash
# Bootstraps the Strata engine into a GCP project:
#   APIs → Artifact Registry → engine service account + roles → Cloud SQL
#   (PostgreSQL 16) → DB password in Secret Manager → image build → Cloud Run.
#
# Re-runnable: existing resources are reused, the service is redeployed.
#
#   PROJECT_ID=my-proj REGION=us-central1 \
#   ALLOWED_CALLERS="user:you@example.com,group:platform@example.com" \
#   ./deploy/bootstrap.sh
set -euo pipefail

PROJECT_ID=${PROJECT_ID:?set PROJECT_ID}
REGION=${REGION:-us-central1}
SERVICE=${SERVICE:-strata}
REPO=${REPO:-strata}
SA_NAME=${SA_NAME:-strata-engine}
SQL_INSTANCE=${SQL_INSTANCE:-strata-pg}
SQL_TIER=${SQL_TIER:-db-custom-1-3840}
SQL_AVAILABILITY=${SQL_AVAILABILITY:-zonal}   # set to "regional" for HA
DB_NAME=${DB_NAME:-strata}
DB_USER=${DB_USER:-strata}
DB_SECRET=${DB_SECRET:-strata-db-password}
ALLOWED_CALLERS=${ALLOWED_CALLERS:-}           # IAM members allowed to call the API
TAG=${TAG:-$(git rev-parse --short HEAD 2>/dev/null || date +%Y%m%d%H%M%S)}

SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/strata-server:${TAG}"
CONNECTION="${PROJECT_ID}:${REGION}:${SQL_INSTANCE}"

log() { printf '\033[1;34m==> %s\033[0m\n' "$*"; }
exists() { "$@" >/dev/null 2>&1; }

gcloud config set project "$PROJECT_ID" >/dev/null

log "Enabling APIs"
gcloud services enable \
  run.googleapis.com sqladmin.googleapis.com secretmanager.googleapis.com \
  artifactregistry.googleapis.com cloudbuild.googleapis.com iam.googleapis.com \
  iamcredentials.googleapis.com serviceusage.googleapis.com \
  cloudresourcemanager.googleapis.com pubsub.googleapis.com storage.googleapis.com

log "Artifact Registry repository ${REPO}"
exists gcloud artifacts repositories describe "$REPO" --location "$REGION" ||
  gcloud artifacts repositories create "$REPO" --location "$REGION" --repository-format docker \
    --description "Strata engine images"

log "Engine service account ${SA_EMAIL}"
exists gcloud iam service-accounts describe "$SA_EMAIL" ||
  gcloud iam service-accounts create "$SA_NAME" --display-name "Strata deployment engine"

# Roles the engine needs to manage the resource types it supports. Narrow
# this list if you disable resource types you do not use.
ROLES=(
  roles/storage.admin
  roles/pubsub.admin
  roles/run.admin
  roles/secretmanager.admin
  roles/iam.serviceAccountAdmin
  roles/iam.serviceAccountUser
  roles/resourcemanager.projectIamAdmin
  roles/serviceusage.serviceUsageAdmin
  roles/cloudsql.client
  roles/logging.logWriter
  roles/monitoring.metricWriter
)
log "Granting engine roles"
for role in "${ROLES[@]}"; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" --member "serviceAccount:${SA_EMAIL}" \
    --role "$role" --condition None >/dev/null
done

log "Cloud SQL instance ${SQL_INSTANCE} (PostgreSQL 16)"
if ! exists gcloud sql instances describe "$SQL_INSTANCE"; then
  gcloud sql instances create "$SQL_INSTANCE" --database-version POSTGRES_16 --region "$REGION" \
    --edition ENTERPRISE --tier "$SQL_TIER" --availability-type "$SQL_AVAILABILITY" \
    --storage-auto-increase --backup-start-time 03:00 --enable-point-in-time-recovery \
    --deletion-protection
fi
exists gcloud sql databases describe "$DB_NAME" --instance "$SQL_INSTANCE" ||
  gcloud sql databases create "$DB_NAME" --instance "$SQL_INSTANCE"

log "Database credentials in Secret Manager (${DB_SECRET})"
if ! exists gcloud secrets describe "$DB_SECRET"; then
  PASSWORD=$(openssl rand -base64 32 | tr -d '/+=' | cut -c1-32)
  printf '%s' "$PASSWORD" | gcloud secrets create "$DB_SECRET" --replication-policy automatic --data-file -
  if exists gcloud sql users describe "$DB_USER" --instance "$SQL_INSTANCE"; then
    gcloud sql users set-password "$DB_USER" --instance "$SQL_INSTANCE" --password "$PASSWORD"
  else
    gcloud sql users create "$DB_USER" --instance "$SQL_INSTANCE" --password "$PASSWORD"
  fi
  unset PASSWORD
fi
gcloud secrets add-iam-policy-binding "$DB_SECRET" --member "serviceAccount:${SA_EMAIL}" \
  --role roles/secretmanager.secretAccessor >/dev/null

log "Building ${IMAGE}"
gcloud builds submit --tag "$IMAGE" .

log "Deploying Cloud Run service ${SERVICE}"
# Cloud Run IAM authenticates callers (roles/run.invoker); the engine
# applies STRATA_ALLOWED_CALLERS on top. CPU is always allocated and one
# instance stays warm because workers run operations in the background.
gcloud run deploy "$SERVICE" --image "$IMAGE" --region "$REGION" \
  --service-account "$SA_EMAIL" --no-allow-unauthenticated \
  --add-cloudsql-instances "$CONNECTION" \
  --set-env-vars "^|^STRATA_PROJECT=${PROJECT_ID}|STRATA_REGION=${REGION}|STRATA_STORE=postgres|STRATA_PROVIDER=gcp|STRATA_AUTH=cloudrun-iam|STRATA_ALLOWED_CALLERS=${ALLOWED_CALLERS}|STRATA_DENY_PUBLIC_MEMBERS=false|DB_HOST=/cloudsql/${CONNECTION}|DB_NAME=${DB_NAME}|DB_USER=${DB_USER}" \
  --set-secrets "DB_PASSWORD=${DB_SECRET}:latest" \
  --no-cpu-throttling --min-instances 1 --max-instances 3 \
  --cpu 1 --memory 512Mi --concurrency 80 --timeout 3600 \
  --labels app=strata

if [[ -n "$ALLOWED_CALLERS" ]]; then
  log "Granting roles/run.invoker to allowed callers"
  IFS=',' read -ra MEMBERS <<<"$ALLOWED_CALLERS"
  for m in "${MEMBERS[@]}"; do
    m=$(echo "$m" | xargs)
    [[ "$m" == domain:* || "$m" == user:* || "$m" == group:* || "$m" == serviceAccount:* ]] || m="user:${m}"
    gcloud run services add-iam-policy-binding "$SERVICE" --region "$REGION" \
      --member "$m" --role roles/run.invoker >/dev/null
  done
fi

URL=$(gcloud run services describe "$SERVICE" --region "$REGION" --format 'value(status.url)')
cat <<EOF

Strata is running at ${URL}

  export STRATA_SERVER=${URL}
  strata types
  go run ./examples/orders-platform -out strata.out
  strata deploy -s orders -f strata.out/orders.template.json

The CLI authenticates with \`gcloud auth print-identity-token\`; your account
needs roles/run.invoker on the service (see ALLOWED_CALLERS).
EOF
