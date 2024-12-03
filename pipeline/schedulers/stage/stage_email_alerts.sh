#!/usr/bin/env bash

set -e
set -o pipefail

URI="https://us-central1-wmt-cf-dev.cloudfunctions.net/dev-recommendationprocessor-func"
HEADERS='Content-Type: application/json User-Agent: Google-Cloud-Scheduler isRecoFilePresent:true projectId:prod-savings-personalization'

gcloud scheduler jobs update http stage_email_alert \
  --location="us-central1" \
  --time-zone="America/Chicago" \
  --schedule="0 10 * * *" \
  --uri="$URI" \
  --http-method=POST \
  --headers="$HEADERS" \
  --oauth-service-account-email="$SERVICE_ACCOUNT" \
  --message-body="$(cat <<EOF
{
	"mailTo": [
		"alert-offer-bank-n-p@email.wal-mart.com"
	],
        "module": "savings",
        "bucketName": "prod-savings-output"
}
EOF
)"