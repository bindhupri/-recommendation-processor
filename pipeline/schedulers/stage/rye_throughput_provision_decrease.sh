#!/usr/bin/env bash

set -e
set -o pipefail

URI="https://us-central1-wmt-cf-dev.cloudfunctions.net/stage-recommendationprocessor-cosmos-provision-func"
HEADERS='Content-Type: application/json User-Agent: Google-Cloud-Scheduler'

gcloud scheduler jobs update http rye_throughput_provision_decrease \
  --location="us-central1" \
  --time-zone="America/Chicago" \
  --schedule="0 12 * * *" \
  --uri="$URI" \
  --http-method=POST \
  --headers="$HEADERS" \
  --oauth-service-account-email="$SERVICE_ACCOUNT" \
  --message-body="$(cat <<EOF
{
  "throughput": "10000",
  "cosmosDbName": "rye",
  "cosmosContainerName": "rye-recommendations"
}
EOF
)"