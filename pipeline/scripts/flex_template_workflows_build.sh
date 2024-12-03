#!/usr/bin/env bash

set -e
set -o pipefail
set -u

echo "#######Building Dataflow Flex Template Docker image with all the dependencies installed inside"

gcloud dataflow flex-template build "$FLEX_TEMPLATE_FILE_LOCATION" \
 --image-gcr-path "$ARTIFACTORY_PATH_VERSION" \
 --sdk-language "$SDK_LANGUAGE" \
 --flex-template-base-image "$FLEX_TEMPLATE_BASE_IMAGE" \
 --metadata-file "$METADATA_FILE" \
 --gcs-log-dir="$GCS_BUILD_LOG_DIR" \
 --subnetwork="$VPC_SUB_NETWORK" \
 --worker-region="$LOCATION" \
 --jar "$JAR" \
 --staging-location="$STAGING_LOCATION" \
 --temp-location="$GCS_TEMP_LOCATION" \
 --disable-public-ips \
 --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="$FLEX_TEMPLATE_JAVA_MAIN_CLASS"


echo "####### Updating Workflows in environment ${ENVIRONMENT}"

WORKFLOWS=(recommendation-processor-flextemplate cosmos_failures_retry publish_message_topic)
WORKFLOW_YML=(./pipeline/workflows/dev/ ./pipeline/workflows/stage/ ./pipeline/workflows/prod/)

for WORKFLOW in "${WORKFLOWS[@]}"; do
  WORKFLOW_YML_PATH="${WORKFLOW_YML[${ENVIRONMENT}]}${WORKFLOW}.yml"
  gcloud workflows deploy "${WORKFLOW}" --source="${WORKFLOW_YML_PATH}" --set-env-vars "TEMP_LOCATION=${GCS_TEMP_LOCATION},SERVICE_ACCOUNT=${SERVICE_ACCOUNT},OUTPUT_FILE_PATH=${OUTPUT_FILE_PATH},COSMOS_BATCH_SIZE=50000"
done


