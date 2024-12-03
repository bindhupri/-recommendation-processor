#!/usr/bin/env bash

set -e
set -o pipefail

URI="https://workflowexecutions.googleapis.com/v1/projects/dev-sams-data-generator/locations/us-central1/workflows/change_feed_retry/executions"
HEADERS='Content-Type: application/json User-Agent: Google-Cloud-Scheduler'

gcloud scheduler jobs update http change_feed_retry_scheduler \
  --location="us-central1" \
  --time-zone="America/Chicago" \
  --schedule="0 9-15 * * *" \
  --uri="$URI" \
  --http-method=POST \
  --headers="$HEADERS" \
  --oauth-service-account-email="$SERVICE_ACCOUNT" \
  --message-body="$(cat <<EOF
{"argument":"{\n  \"input_file\": \"gs://change_feed_failures/pending/**\",\n  \"location\": \"us-central1\",\n  \"output_file\": \"gs://processed_recommendations/savings/cold-start/2024-02-06\",\n  \"project_id\": \"dev-sams-data-generator\",\n  \"serviceAccount\": \"svc-deploy-mgmt@dev-sams-data-generator.iam.gserviceaccount.com\",\n  \"temp_location\": \"gs://datageneratorbigquerytemp\"\n}","callLogLevel":"CALL_LOG_LEVEL_UNSPECIFIED"}
EOF
)"