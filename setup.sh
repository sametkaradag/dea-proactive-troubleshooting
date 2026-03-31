#!/bin/bash
set -e

# Parse the arguments and declare them as variables
OLDIFS=${IFS}
for arg in "$*"; do
  if [ -z "${arg}" ]; then
    continue;
  fi
  if [ "${arg}" = "--" ]; then
    break;
  fi
  IFS='='
  read -ra pair <<< "${arg}"
  declare "${pair[0]#'--'}=${pair[1]}"
  IFS=${OLDIFS}
done
IFS=${OLDIFS}

PROJECT_ID=${project:-$(gcloud config get core/project)}
REGION=${region:-$(gcloud config get compute/region)}
SERVICE_ACCOUNT=${sa:-$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")} # Default compute SA for this project
TOPIC_NAME=${topic:-"dataform-failures"}
SINK_NAME=${sink:-"dataform-failure-sink"}
FUNCTION_NAME=${function:-"troubleshoot-dataform"}
USER_EMAIL=${email?"Please provide the notification email through the --email argument"} # Change this to the target recipient email
ALLOWED_DATAFORM_REPOSITORIES=${repos:-""} # Comma separated list of repo names (e.g. "my-repo,other-repo"), leave empty for all

echo "Enabling APIs..."
gcloud services enable \
    dataform.googleapis.com \
    pubsub.googleapis.com \
    logging.googleapis.com \
    cloudfunctions.googleapis.com \
    run.googleapis.com \
    eventarc.googleapis.com \
    geminidataanalytics.googleapis.com \
    --project="$PROJECT_ID"

echo "Creating Pub/Sub Topic..."
if ! gcloud pubsub topics describe "$TOPIC_NAME" --project="$PROJECT_ID" &>/dev/null; then
    gcloud pubsub topics create "$TOPIC_NAME" --project="$PROJECT_ID"
else
    echo "Topic $TOPIC_NAME already exists."
fi

echo "Deploying Cloud Function (Gen 2)..."
# We need to deploy from the directory containing main.py
gcloud functions deploy "$FUNCTION_NAME" \
    --gen2 \
    --runtime=python311 \
    --region="$REGION" \
    --source=./src \
    --entry-point=troubleshoot_dataform \
    --trigger-topic="$TOPIC_NAME" \
    --project="$PROJECT_ID" \
    --service-account="$SERVICE_ACCOUNT" \
    --set-env-vars USER_EMAIL="$USER_EMAILS" \
    --timeout=300s # DEA might take a bit to generate RCA

echo "Creating or Updating Log Sink..."
# Filter for Dataform errors. 
# Real Dataform logs use resource.type="dataform.googleapis.com/Repository"
# and jsonPayload.terminalState="FAILED" for completion logs.
FILTER='resource.type="dataform.googleapis.com/Repository" AND jsonPayload.terminalState="FAILED"'

if [ -n "$ALLOWED_DATAFORM_REPOSITORIES" ]; then
    IFS=',' read -ra REPOS <<< "$ALLOWED_DATAFORM_REPOSITORIES"
    REPO_FILTER=""
    for repo in "${REPOS[@]}"; do
        repo=$(echo "$repo" | awk '{$1=$1};1') # trim whitespace
        if [ -n "$repo" ]; then
            if [ -n "$REPO_FILTER" ]; then
                REPO_FILTER="$REPO_FILTER OR "
            fi
            REPO_FILTER="${REPO_FILTER}resource.labels.repository_id=\"$repo\""
        fi
    done
    if [ -n "$REPO_FILTER" ]; then
        FILTER="$FILTER AND ($REPO_FILTER)"
    fi
    echo "Using filtered repositories: $FILTER"
fi

if ! gcloud logging sinks describe "$SINK_NAME" --project="$PROJECT_ID" &>/dev/null; then
    gcloud logging sinks create "$SINK_NAME" \
        "pubsub.googleapis.com/projects/$PROJECT_ID/topics/$TOPIC_NAME" \
        --log-filter="$FILTER" \
        --project="$PROJECT_ID"
    
    # Grant Pub/Sub Publisher role to the Sink's writer identity
    WRITER_IDENTITY=$(gcloud logging sinks describe "$SINK_NAME" --project="$PROJECT_ID" --format="value(writerIdentity)")
    gcloud pubsub topics add-iam-policy-binding "$TOPIC_NAME" \
        --project="$PROJECT_ID" \
        --member="$WRITER_IDENTITY" \
        --role="roles/pubsub.publisher"
    echo "Sink $SINK_NAME created."
else
    echo "Updating existing Sink $SINK_NAME..."
    gcloud logging sinks update "$SINK_NAME" \
        "pubsub.googleapis.com/projects/$PROJECT_ID/topics/$TOPIC_NAME" \
        --log-filter="$FILTER" \
        --project="$PROJECT_ID"
fi

echo "Creating Notifications Pub/Sub Topic..."
NOTIFICATION_TOPIC="dea-rca-topic"
if ! gcloud pubsub topics describe "$NOTIFICATION_TOPIC" --project="$PROJECT_ID" &>/dev/null; then
    gcloud pubsub topics create "$NOTIFICATION_TOPIC" --project="$PROJECT_ID"
fi
if ! gcloud pubsub subscriptions describe "${NOTIFICATION_TOPIC}-sub" --project="$PROJECT_ID" &>/dev/null; then
    gcloud pubsub subscriptions create "${NOTIFICATION_TOPIC}-sub" --topic="$NOTIFICATION_TOPIC" --project="$PROJECT_ID"
fi

echo "Creating Cloud Logging Sink for Pub/Sub..."
RCA_SINK_NAME="dea-rca-pubsub-sink"
RCA_FILTER='resource.type="cloud_run_revision" AND textPayload:"DATAFORM_RCA_GENERATED"'
if ! gcloud logging sinks describe "$RCA_SINK_NAME" --project="$PROJECT_ID" &>/dev/null; then
    gcloud logging sinks create "$RCA_SINK_NAME" \
        "pubsub.googleapis.com/projects/$PROJECT_ID/topics/$NOTIFICATION_TOPIC" \
        --log-filter="$RCA_FILTER" \
        --project="$PROJECT_ID"
    
    # Grant Pub/Sub Publisher role to the Sink's writer identity
    WRITER_IDENTITY=$(gcloud logging sinks describe "$RCA_SINK_NAME" --project="$PROJECT_ID" --format="value(writerIdentity)")
    gcloud pubsub topics add-iam-policy-binding "$NOTIFICATION_TOPIC" \
        --project="$PROJECT_ID" \
        --member="$WRITER_IDENTITY" \
        --role="roles/pubsub.publisher"
else
    echo "Sink $RCA_SINK_NAME already exists."
fi

echo "Cleaning up old/broken Alert Policies..."
OLD_POLICIES=$(gcloud monitoring policies list --filter='displayName="Dataform RCA Generated Alert"' --project="$PROJECT_ID" --format="value(name)")
for policy in $OLD_POLICIES; do
    echo "Deleting $policy..."
    gcloud monitoring policies delete "$policy" --project="$PROJECT_ID" --quiet || true
done

echo "Creating Pub/Sub Notification Channel..."
CHANNEL_NAME=$(gcloud beta monitoring channels create \
    --display-name="Dataform RCA PubSub Channel" \
    --type=pubsub \
    --channel-labels=topic="projects/$PROJECT_ID/topics/$NOTIFICATION_TOPIC" \
    --project="$PROJECT_ID" \
    --format="value(name)")

echo "Created Pub/Sub Notification Channel: $CHANNEL_NAME"

echo "Creating Email Notification Channels..."
EMAIL_CHANNEL_NAMES=()
IFS=',' read -ra EMAIL_LIST <<< "$USER_EMAILS"
for email in "${EMAIL_LIST[@]}"; do
    email=$(echo "$email" | awk '{$1=$1};1') # trim whitespace
    if [ -n "$email" ]; then
        CH=$(gcloud beta monitoring channels create \
            --display-name="Dataform RCA Email Channel ($email)" \
            --type=email \
            --channel-labels=email_address="$email" \
            --project="$PROJECT_ID" \
            --format="value(name)")
        echo "Created Email Notification Channel for $email: $CH"
        EMAIL_CHANNEL_NAMES+=("$CH")
    fi
done

echo "Creating or Updating Log-Based Metric..."
gcloud logging metrics create dataform_rca_metric \
    --description="Dataform RCA Generated" \
    --log-filter='resource.type="cloud_run_revision" AND textPayload:"DATAFORM_RCA_GENERATED"' \
    --project="$PROJECT_ID" 2>/dev/null || \
gcloud logging metrics update dataform_rca_metric \
    --description="Dataform RCA Generated" \
    --log-filter='resource.type="cloud_run_revision" AND textPayload:"DATAFORM_RCA_GENERATED"' \
    --project="$PROJECT_ID"

echo "Creating Alert Policy..."
cat <<EOF > policy.yaml
displayName: "Dataform RCA Generated Alert"
combiner: OR
conditions:
  - displayName: "Log match for DATAFORM_RCA_GENERATED"
    conditionThreshold:
      filter: 'metric.type="logging.googleapis.com/user/dataform_rca_metric" AND resource.type="cloud_run_revision"'
      comparison: COMPARISON_GT
      thresholdValue: 0
      duration: "0s"
      aggregations:
        - alignmentPeriod: "60s"
          crossSeriesReducer: REDUCE_SUM
          perSeriesAligner: ALIGN_DELTA
notificationChannels:
  - $CHANNEL_NAME
$(for ch in "${EMAIL_CHANNEL_NAMES[@]}"; do echo "  - $ch"; done)
documentation:
  content: "A Root Cause Analysis has been generated for a failed Dataform Job. Please check the Cloud Function Logs to view the generated HTML email containing the RCA details and Workspace Link."
  mimeType: text/markdown
EOF

gcloud monitoring policies create --policy-from-file=policy.yaml --project="$PROJECT_ID" || \
echo "Failed to create Alert Policy automatically."

rm -f policy.yaml

echo "Setup Complete!"
