#!/usr/bin/env bash

set -e
set -o pipefail

URI="https://us-central1-wmt-cf-dev.cloudfunctions.net/dev-recommendationprocessorcosmosprovision-func"
HEADERS='Content-Type: application/json User-Agent: Google-Cloud-Scheduler'

gcloud scheduler jobs update http Cosmos_throughput_provision_decrease \
  --location="us-central1" \
  --time-zone="America/Chicago" \
  --schedule="0 1 * * *" \
  --uri="$URI" \
  --http-method=POST \
  --headers="$HEADERS" \
  --oauth-service-account-email="$SERVICE_ACCOUNT" \
  --message-body="$(cat <<EOF
{
  "throughput": "11000",
  "cosmosDbName": "recommendations",
  "cosmosContainerName": "product-recommendations"
}
EOF
)"