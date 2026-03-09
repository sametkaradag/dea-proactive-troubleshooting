#!/bin/bash
PROJECT_ID="bq-dataworkeragent-test"
TOPIC_NAME="dataform-failures"

# Real Job ID and Repo ID found in logs (though original job succeeded, we simulate failure)
JOB_ID="1771520590-3d43767e-e27c-4bd4-ad96-83772597ff25"
REPO_ID="pipeline-optimization-column-pruning"
# JOB_ID="job-$(date +%s)" # Old random ID
# REPO_ID="prisa-test" # Old repo

# JSON Payload representing a Dataform failure log
# Updated to match dataform.googleapis.com/Repository and terminalState=FAILED
PAYLOAD=$(cat <<EOF
{
  "insertId": "test-insert-id-${JOB_ID}",
  "jsonPayload": {
    "terminalState": "FAILED",
    "workflowInvocationId": "${JOB_ID}",
    "message": "Simulated Dataform Error: Assertion failed in table definitions/stats.sql"
  },
  "resource": {
    "type": "dataform.googleapis.com/Repository",
    "labels": {
      "project_id": "${PROJECT_ID}",
      "location": "us-central1",
      "repository_id": "${REPO_ID}",
      "resource_container": "954904738192"
    }
  },
  "logName": "projects/${PROJECT_ID}/logs/dataform.googleapis.com%2Fworkflow_invocation_completion",
  "timestamp": "${TIMESTAMP}"
}
EOF
)

echo "Publishing mock failure to $TOPIC_NAME..."
gcloud pubsub topics publish "$TOPIC_NAME" --project="$PROJECT_ID" --message="$PAYLOAD"

echo "Mock failure published. Check Cloud Function logs for 'job-${JOB_ID}'."
