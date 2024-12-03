#!/usr/bin/env bash

set -e
set -o pipefail

URI="https://us-central1-wmt-cf-prod.cloudfunctions.net/prod-recommendationprocessor-cosmos-provision-func"
HEADERS='Content-Type: application/json User-Agent: Google-Cloud-Scheduler'

gcloud scheduler jobs update http rye_cosmos_throughput_provision_increase_scheduler \
  --location="us-central1" \
  --time-zone="America/Chicago" \
  --schedule="0 10 * * *" \
  --uri="$URI" \
  --http-method=POST \
  --headers="$HEADERS" \
  --oauth-service-account-email="$SERVICE_ACCOUNT" \
  --message-body="$(cat <<EOF
{
  "throughput": "70000",
  "cosmosDbName": "rye",
  "cosmosContainerName": "rye-recommendations"
}
EOF
)"