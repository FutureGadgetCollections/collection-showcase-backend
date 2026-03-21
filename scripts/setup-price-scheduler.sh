#!/usr/bin/env bash
set -euo pipefail

PROJECT=future-gadget-labs-483502
REGION=us-central1
IMAGE=us-central1-docker.pkg.dev/${PROJECT}/tcg-collection/collection-showcase:latest
JOB_NAME=fetch-prices-daily
SCHEDULER_NAME=fetch-prices-daily

# Service account used by Cloud Scheduler to trigger the job.
# Defaults to the project's Compute Engine default service account.
SCHEDULER_SA="${PROJECT_NUMBER:-$(gcloud projects describe $PROJECT --format='value(projectNumber)')}"-compute@developer.gserviceaccount.com

echo "Setting up Cloud Run Job: $JOB_NAME"

gcloud run jobs create $JOB_NAME \
  --project=$PROJECT \
  --region=$REGION \
  --image=$IMAGE \
  --command="/fetchprices" \
  --task-timeout=30m \
  --max-retries=0 \
  --memory=2Gi \
  --set-env-vars="BQ_PROJECT=${PROJECT},BQ_INVENTORY_DATASET=inventory,BQ_MARKET_DATASET=market_data" \
  2>/dev/null \
  && echo "Created job: $JOB_NAME" \
  || { \
    gcloud run jobs update $JOB_NAME \
      --project=$PROJECT \
      --region=$REGION \
      --image=$IMAGE \
      --command="/fetchprices" \
      --task-timeout=30m \
      --max-retries=0 \
      --memory=2Gi \
      --set-env-vars="BQ_PROJECT=${PROJECT},BQ_INVENTORY_DATASET=inventory,BQ_MARKET_DATASET=market_data" \
    && echo "Updated job: $JOB_NAME"; \
  }

# Grant the scheduler's service account permission to run the job.
gcloud run jobs add-iam-policy-binding $JOB_NAME \
  --project=$PROJECT \
  --region=$REGION \
  --member="serviceAccount:${SCHEDULER_SA}" \
  --role="roles/run.invoker" \
  2>/dev/null || true

JOB_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB_NAME}:run"

echo "Setting up Cloud Scheduler: $SCHEDULER_NAME"

gcloud scheduler jobs create http $SCHEDULER_NAME \
  --project=$PROJECT \
  --location=$REGION \
  --schedule="0 8 * * *" \
  --time-zone="America/New_York" \
  --uri="$JOB_URI" \
  --message-body="" \
  --oauth-service-account-email="${SCHEDULER_SA}" \
  --http-method=POST \
  --attempt-deadline=35m \
  2>/dev/null \
  && echo "Created scheduler: $SCHEDULER_NAME" \
  || { \
    gcloud scheduler jobs update http $SCHEDULER_NAME \
      --project=$PROJECT \
      --location=$REGION \
      --schedule="0 8 * * *" \
      --time-zone="America/New_York" \
      --uri="$JOB_URI" \
      --message-body="" \
      --oauth-service-account-email="${SCHEDULER_SA}" \
      --http-method=POST \
      --attempt-deadline=35m \
    && echo "Updated scheduler: $SCHEDULER_NAME"; \
  }

echo "Done. Runs daily at 8 AM Eastern."
echo "To trigger manually: gcloud run jobs execute $JOB_NAME --region=$REGION --project=$PROJECT"
