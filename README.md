# Data Engineering Agent Proactive Troubleshooting

This project contains an automated proactive troubleshooting pipeline for Google Cloud Dataform using the **Data Engineering Agent (DEA)**.

When a Dataform or BQ Pipeline job fails, a Cloud Logging Sink captures the failure event and triggers a Pub/Sub topic. A Gen 2 Cloud Function listens to this topic, extracts the failure context, and automatically invokes the Data Engineering Agent. 

The agent analyzes the failure, provides a Root Cause Analysis (RCA), and automatically prepares a draft workspace (e.g., `user_email-agent-draft`) with proposed fixes. Finally, an email report containing the RCA and a direct link to the draft workspace is dispatched to the user.

## Architecture

1. **Cloud Logging Sink**: Filters for Dataform repository failure logs and pushes them to Pub/Sub.
2. **Pub/Sub Topic**: `dataform-failures` acts as the trigger.
3. **Cloud Function**: `troubleshoot-dataform` (Python 3.11) processes the log, interacts with the Dataform v1beta1 API to prepare a draft workspace, and streams the prompt to the Gemini Data Analytics API (DEA).
4. **Cloud Monitoring**: Dynamically updates an Alert Policy's documentation to send a beautifully formatted email with the RCA to the user.

## Files

- `setup.sh`: An automated deployment script that enables necessary APIs, creates Pub/Sub topics, deploys the Cloud Function, configures Logging Sinks, and sets up the Monitoring Alert Policies.
- `src/main.py`: The core Python Cloud Function logic.
- `src/requirements.txt`: Python package dependencies.
- `test_direct_invoke.sh` / `test_script.py` / `trigger_mock_failure.sh`: Scripts for local testing and mock invocations.

## Deployment

The entire architecture can be deployed using the provided `setup.sh` script.

### Installation

Run the setup script to provision the infrastructure:

```bash
chmod +x setup.sh
./setup.sh --emails=<notification-email@domain>
```

By default it will monitor all Dataform pipeline executions in the default project
configured in your local gcloud environment.

### Configuration

You can alter the behavior of the monitoring with the following parameters to the setup script.

- `--emails=<emails>` a comma separate list of email addresses to notify.
- `--project=<project>` the GCP project to monitor.
- `--region=<region>` the region where the Cloud Function will be deployed.
- `--sa=<service-account>` the service account of the Cloud Function.
- `--repos=<dataform-repos>` a comma separate list of Dataform repository names to monitor.
- `--topic=<pubsub-topic>` the name of the Pub/Sub topic.
- `--sink=<sink-name>` the name of the Cloud Logging sink.
- `--function=<function-name>` the name of the Cloud Function.

## How It Works

1. **Failure Occurs**: A Dataform pipeline fails (terminal state `FAILED`).
2. **Event Triggered**: If the repository matches `ALLOWED_DATAFORM_REPOSITORIES` (or if it's left empty), the Cloud Logging Sink routes the event to the Pub/Sub topic.
3. **Agent Invocation**: The Cloud Function receives the event and asks the Data Engineering Agent for an RCA.
4. **Draft Workspace**: The function creates a draft Dataform workspace specific to the user (e.g. `sametkaradag_google_com-agent-draft`).
5. **Notification**: The Cloud Function triggers a log metric that fires a Cloud Monitoring Alert via email containing the RCA and a link to the draft workspace where the DEA agent's potential fixes reside.
