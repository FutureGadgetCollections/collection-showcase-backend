#!/usr/bin/env bash
# Sets up Cloud Run Jobs and Cloud Scheduler for the collection-showcase project.
#
# Jobs managed here:
#   collection-showcase-market-price-update       — Cloud Run Job: fetch prices from TCGPlayer → BQ
#   collection-showcase-market-price-update-daily — Cloud Scheduler: triggers above job daily at 8 AM ET
#   collection-showcase-data-sync                 — Cloud Run Job: sync BQ → GCS + GitHub JSON files
#   collection-showcase-daily-sync                — Cloud Scheduler: triggers above job daily at 2 AM UTC
#
# Usage: bash scripts/setup-price-scheduler.sh
set -euo pipefail

PROJECT=future-gadget-labs-483502
REGION=us-central1
IMAGE=us-central1-docker.pkg.dev/${PROJECT}/tcg-collection/collection-showcase:latest
LABEL=service=collection-showcase

SCHEDULER_SA="$(gcloud projects describe $PROJECT --format='value(projectNumber)')-compute@developer.gserviceaccount.com"

upsert_run_job() {
  local name=$1; shift
  if gcloud run jobs describe "$name" --region=$REGION --project=$PROJECT &>/dev/null; then
    gcloud run jobs update "$name" --region=$REGION --project=$PROJECT "$@"
    echo "Updated Cloud Run Job: $name"
  else
    gcloud run jobs create "$name" --region=$REGION --project=$PROJECT "$@"
    echo "Created Cloud Run Job: $name"
  fi
}

upsert_scheduler() {
  local name=$1; shift
  if gcloud scheduler jobs describe "$name" --location=$REGION --project=$PROJECT &>/dev/null; then
    gcloud scheduler jobs update http "$name" --location=$REGION --project=$PROJECT "$@"
    echo "Updated scheduler: $name"
  else
    gcloud scheduler jobs create http "$name" --location=$REGION --project=$PROJECT "$@"
    echo "Created scheduler: $name"
  fi
}

grant_invoker() {
  local job=$1
  gcloud run jobs add-iam-policy-binding "$job" \
    --project=$PROJECT \
    --region=$REGION \
    --member="serviceAccount:${SCHEDULER_SA}" \
    --role="roles/run.invoker" \
    2>/dev/null || true
}

job_uri() {
  echo "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${1}:run"
}

# ── Market price update ────────────────────────────────────────────────────────
echo "--- collection-showcase-market-price-update ---"
upsert_run_job collection-showcase-market-price-update \
  --image=$IMAGE \
  --command="/fetchprices" \
  --task-timeout=30m \
  --max-retries=0 \
  --memory=2Gi \
  --labels=$LABEL \
  --set-env-vars="BQ_PROJECT=${PROJECT},BQ_INVENTORY_DATASET=inventory,BQ_MARKET_DATASET=market_data"

grant_invoker collection-showcase-market-price-update

upsert_scheduler collection-showcase-market-price-update-daily \
  --schedule="0 8 * * *" \
  --time-zone="America/New_York" \
  --uri="$(job_uri collection-showcase-market-price-update)" \
  --message-body="" \
  --oauth-service-account-email="${SCHEDULER_SA}" \
  --http-method=POST \
  --attempt-deadline=30m \
  --description="collection-showcase: daily TCGPlayer price fetch for all tracked products"

# ── Data sync ──────────────────────────────────────────────────────────────────
echo "--- collection-showcase-data-sync ---"

# Read GITHUB_TOKEN from the existing Cloud Run service if not already set.
if [[ -z "${GITHUB_TOKEN:-}" ]]; then
  GITHUB_TOKEN=$(gcloud run services describe collection-showcase \
    --region=$REGION --project=$PROJECT \
    --format="json(spec.template.spec.containers[0].env)" \
    | python3 -c "
import sys, json
envs = json.load(sys.stdin)['spec']['template']['spec']['containers'][0]['env']
for e in envs:
    if e['name'] == 'GITHUB_TOKEN':
        print(e['value'])
        break
")
fi

upsert_run_job collection-showcase-data-sync \
  --image=$IMAGE \
  --command="/syncdata" \
  --task-timeout=10m \
  --max-retries=0 \
  --memory=512Mi \
  --labels=$LABEL \
  --set-env-vars="BQ_PROJECT=${PROJECT},BQ_INVENTORY_DATASET=inventory,BQ_MARKET_DATASET=market_data,GCS_DATA_BUCKET=collection-showcase-data,GITHUB_OWNER=FutureGadgetCollections,GITHUB_REPO=collection-showcase-data,GITHUB_TOKEN=${GITHUB_TOKEN}"

grant_invoker collection-showcase-data-sync

upsert_scheduler collection-showcase-daily-sync \
  --schedule="0 3 * * *" \
  --time-zone="Etc/UTC" \
  --uri="$(job_uri collection-showcase-data-sync)" \
  --message-body="" \
  --oauth-service-account-email="${SCHEDULER_SA}" \
  --http-method=POST \
  --attempt-deadline=15m \
  --description="collection-showcase: daily BQ to GCS+GitHub JSON data file sync"

echo ""
echo "Done."
echo "Manual triggers:"
echo "  gcloud run jobs execute collection-showcase-market-price-update --region=$REGION --project=$PROJECT"
echo "  gcloud run jobs execute collection-showcase-data-sync --region=$REGION --project=$PROJECT"
