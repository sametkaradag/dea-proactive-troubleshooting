#!/bin/bash
URL=$(gcloud functions describe troubleshoot-dataform --gen2 --region=us-central1 --format="value(active_version.serviceConfig.uri)")
# Fallback if active_version is not set or complex
if [ -z "$URL" ]; then
    URL="https://troubleshoot-dataform-3zlreps2vq-uc.a.run.app"
fi

echo "Invoking $URL..."

# CloudEvent compliant headers
# The data must be base64 encoded because the function expects Pub/Sub message format in the payload 
# if it is triggered by Pub/Sub, but here we are simulating the Pub/Sub message structure if we use the same code.
# WAIT, our code `troubleshoot_dataform(cloud_event)` expects a CloudEvent object.
# The data property of the cloud_event object typically contains the JSON payload.
# If we send a HTTP request with Ce-Type, etc., functions-framework wraps it.

# Payload inner data (the LogEntry)
JOB_ID="1771520590-3d43767e-e27c-4bd4-ad96-83772597ff25"
REPO_ID="pipeline-optimization-column-pruning"
PROJECT_ID="bq-dataworkeragent-test"
LOCATION="us-central1"
WORKSPACE_ID="default"

LOG_ENTRY=$(cat <<EOF
{
  "jsonPayload": {
    "terminalState": "FAILED",
    "workflowInvocationId": "${JOB_ID}",
    "message": "Direct Invoke Test"
  },
  "resource": {
    "type": "dataform.googleapis.com/Repository",
    "labels": {
      "project_id": "${PROJECT_ID}",
      "location": "${LOCATION}",
      "repository_id": "${REPO_ID}",
      "workspace_id": "${WORKSPACE_ID}",
      "resource_container": "954904738192"
    }
  },
  "logName": "projects/${PROJECT_ID}/logs/dataform.googleapis.com%2Fworkflow_invocation_completion"
}
EOF
)

# Base64 encode the log entry, because Pub/Sub messages are base64 encoded in the `data` field
ENCODED_DATA=$(echo "$LOG_ENTRY" | base64)

# CloudEvent Body for Structured Content Mode (application/cloudevents+json)
# OR Binary Mode. Let's use Binary Mode with JSON body representing the Pub/Sub message.
# The Pub/Sub message format has a `message` field with `data`, `attributes`, etc.

# Actually, when triggered by Pub/Sub, the Cloud Event Data IS the Pub/Sub message.
# So we need to send a JSON body that LOOKS like a Pub/Sub message.

BODY=$(cat <<EOF
{
  "message": {
    "data": "${ENCODED_DATA}",
    "attributes": {}
  },
  "subscription": "projects/${PROJECT_ID}/subscriptions/test-sub"
}
EOF
)

curl -X POST "$URL" \
    -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
    -H "Content-Type: application/json" \
    -H "Ce-Id: 1234567890" \
    -H "Ce-Specversion: 1.0" \
    -H "Ce-Type: google.cloud.pubsub.topic.v1.messagePublished" \
    -H "Ce-Source: //pubsub.googleapis.com/projects/${PROJECT_ID}/topics/dataform-failures" \
    -d "$BODY"
