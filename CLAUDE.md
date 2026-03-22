# collection-showcase-backend

## Project Overview

This is the **backend** of the FutureGadgetCollections application. It has two entrypoints:
the **API server** (`main.go`) and the **sync job** (`cmd/syncdata/main.go`).

## Related Repositories

All repos are siblings under the same parent directory:

```
FutureGadgetLabs/
├── collection-showcase-frontend/           ← Admin frontend
├── collection-showcase-backend/            ← This repo
├── collection-showcase-immortal-frontend/  ← Public frontend
└── collection-showcase-data/              ← JSON data files
```

GitHub org: https://github.com/FutureGadgetCollections/

## Two Entrypoints

| Entrypoint | File | Cloud Run Resource | Purpose |
|-----------|------|-------------------|---------|
| API server | `main.go` | Service: `collection-showcase` | REST API |
| Sync job | `cmd/syncdata/main.go` | Job: `collection-showcase-data-sync` | Daily data export |

Both share `internal/datasync` for the actual sync logic.

## Package Structure

```
main.go                      ← API server (Gin)
cmd/
  syncdata/main.go           ← Standalone sync job
  setup/main.go              ← One-time BQ setup (idempotent)
internal/
  handlers/                  ← HTTP handlers
  middleware/                ← Firebase JWT auth + email allowlist
  datasync/                  ← BQ → GCS + GitHub sync; Status() for observability
```

## Auth

- `GET` routes are public
- `POST`/`PUT`/`PATCH`/`DELETE` require a valid Firebase ID token
- Allowed writers controlled by `ALLOWED_EMAILS` env var
- Firebase project: `collection-showcase-auth`

## GCP Infrastructure

| Resource | Details |
|----------|---------|
| GCP project | `future-gadget-labs-483502` |
| Cloud Run service | `collection-showcase`, `us-central1` |
| Cloud Run job | `collection-showcase-data-sync`, `us-central1` (daily) |
| GCS bucket | `collection-showcase-data` |
| BigQuery | datasets: `inventory`, `market_data` |
| Firebase | `collection-showcase-auth` |

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `BQ_PROJECT` | `future-gadget-labs-483502` | GCP project |
| `BQ_INVENTORY_DATASET` | `inventory` | BQ dataset |
| `BQ_MARKET_DATASET` | `market_data` | BQ dataset |
| `GCS_DATA_BUCKET` | `collection-showcase-data` | GCS bucket |
| `FIREBASE_PROJECT_ID` | `collection-showcase-auth` | Firebase project |
| `ALLOWED_EMAILS` | _(empty)_ | Comma-separated authorized emails |
| `GITHUB_TOKEN` | _(required for sync)_ | GitHub PAT |
| `GITHUB_OWNER` | _(required for sync)_ | GitHub org/user |
| `GITHUB_REPO` | _(required for sync)_ | `collection-showcase-data` |
| `PORT` | `8080` | API server port |

**Both the Cloud Run service and the Cloud Run job need `GITHUB_TOKEN`, `GITHUB_OWNER`, and `GITHUB_REPO` set.**

## API Routes

`GET` routes are public. Write routes require `Authorization: Bearer <firebase-id-token>`.

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/health` | — | Health check |
| GET | `/info` | — | Version + masked env vars (check if `GITHUB_TOKEN` is set) |
| GET | `/sync/status` | — | Last sync: `{last_sync_at, last_sync_error, in_progress}` |
| POST | `/sync` | required | Trigger BQ → GCS + GitHub sync (async, returns 202) |
| GET | `/products` | — | List products |
| GET | `/products/:id` | — | Get product |
| POST | `/products` | required | Create product |
| POST | `/products/bulk` | required | Bulk create |
| PUT | `/products/:id` | required | Update product |
| PATCH | `/products/bulk` | required | Bulk update |
| DELETE | `/products/:id` | required | Delete product |
| GET | `/transactions` | — | List transactions |
| GET | `/transactions/:id` | — | Get transaction |
| POST | `/transactions` | required | Log transaction |
| POST | `/transactions/bulk` | required | Bulk log |
| PUT | `/transactions/:id` | required | Update transaction |
| PATCH | `/transactions/bulk` | required | Bulk update |
| DELETE | `/transactions/:id` | required | Delete transaction |
| GET | `/collection` | — | Current inventory |
| GET | `/collection/:product_id` | — | Inventory for one product |
| GET | `/price-history` | — | List price snapshots |
| POST | `/price-history` | required | Insert snapshot |
| DELETE | `/price-history/:record_id` | required | Delete snapshot |

## Running Locally

```bash
cp .env.example .env
# Fill in GITHUB_TOKEN, ALLOWED_EMAILS, etc.
source .env
go run .
```

Run sync job locally:
```bash
source .env
go run ./cmd/syncdata/main.go
```

Run BQ setup (idempotent):
```bash
source .env
go run ./cmd/setup/main.go
```

## Diagnosing Sync Issues

Check if `GITHUB_TOKEN` is set on the live service:
```bash
curl https://collection-showcase-957536135168.us-central1.run.app/info
# GITHUB_TOKEN shows masked value (e.g. "g***0") if set, empty string if not
```

Check last sync result (tracks syncs triggered via `POST /sync`):
```bash
curl https://collection-showcase-957536135168.us-central1.run.app/sync/status
# {"last_sync_at":"...","last_sync_error":null,"in_progress":false}
```

Check Cloud Run logs:
```bash
gcloud logging read 'resource.type="cloud_run_revision" resource.labels.service_name="collection-showcase"' \
  --limit=50 --project=future-gadget-labs-483502
# Filter for "datasync:" lines to see sync progress and errors
```

## Deployment

GitHub Actions pushes to Artifact Registry on merge to `main`. Deploy manually after:

```bash
gcloud run deploy collection-showcase \
  --image=us-central1-docker.pkg.dev/future-gadget-labs-483502/tcg-collection/collection-showcase:latest \
  --region=us-central1 --project=future-gadget-labs-483502

gcloud run jobs update collection-showcase-data-sync \
  --image=us-central1-docker.pkg.dev/future-gadget-labs-483502/tcg-collection/collection-showcase:latest \
  --region=us-central1 --project=future-gadget-labs-483502
```
