import base64
import json
import logging
import os
import re
import uuid
import time
import requests
import google.auth
import functions_framework
from google.cloud import logging as cloud_logging
from google.cloud import dataform_v1beta1
from google.auth.transport.requests import Request

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_access_token():
    credentials, project = google.auth.default()
    credentials.refresh(Request())
    return credentials.token

def is_bq_pipeline(token, project_id, location, repo_id):
    """Check if the repo has label bigquery-workflow."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    parent = f"projects/{project_id}/locations/{location}/repositories/{repo_id}"
    get_url = f"https://dataform.googleapis.com/v1beta1/{parent}"
    response = requests.get(get_url, headers=headers)
    if response.status_code == 200:
        body = response.json()
        if "labels" in body and "bigquery-workflow" in body["labels"]:
            return True
    return False

def prepare_draft_workspace(token, project_id, location, repo_id, workspace_id):
    """Creates a draft workspace if it doesn't exist."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    parent = f"projects/{project_id}/locations/{location}/repositories/{repo_id}"
    workspace_name = f"{parent}/workspaces/{workspace_id}"

    # Check if exists
    get_url = f"https://dataform.googleapis.com/v1beta1/{workspace_name}"
    response = requests.get(get_url, headers=headers)

    if response.status_code == 200:
        logger.info(f"Workspace {workspace_id} already exists.")
        return True

    if response.status_code == 404:
        logger.info(f"Workspace {workspace_id} not found. Creating...")
        create_url = f"https://dataform.googleapis.com/v1beta1/{parent}/workspaces?workspaceId={workspace_id}"
        create_response = requests.post(create_url, headers=headers, json={})
        if create_response.status_code == 200:
            logger.info(f"Workspace {workspace_id} created successfully.")
            return True
        else:
            logger.error(f"Failed to create workspace {workspace_id}: {create_response.text}")
            return False

    logger.error(f"Failed to check workspace {workspace_id}: {response.text}")
    return False

def get_invocation_error_details(project_id, location, repo_id, job_id):
    """Uses the Dataform API to retrieve the workflow invocation details and failed action errors.
    
    Returns a structured string with the invocation state, failure reason,
    and per-action error messages for all failed tasks.
    Falls back to Cloud Logging if the Dataform API call fails.
    """
    error_parts = []
    
    try:
        client = dataform_v1beta1.DataformClient()
        invocation_name = (
            f"projects/{project_id}/locations/{location}/repositories/{repo_id}"
            f"/workflowInvocations/{job_id}"
        )
        
        # 1. Get the workflow invocation to retrieve its overall state and failure reason
        try:
            invocation = client.get_workflow_invocation(
                request=dataform_v1beta1.GetWorkflowInvocationRequest(name=invocation_name)
            )
            state_name = dataform_v1beta1.WorkflowInvocation.State(invocation.state).name
            error_parts.append(f"Workflow Invocation State: {state_name}")
            
            # The invocation object may contain a `failure_reason` field
            if hasattr(invocation, 'failure_reason') and invocation.failure_reason:
                error_parts.append(f"Invocation Failure Reason: {invocation.failure_reason}")
        except Exception as inv_err:
            logger.warning(f"Could not fetch workflow invocation details: {inv_err}")
        
        # 2. Query all actions in this invocation and collect FAILED ones
        try:
            actions_pager = client.query_workflow_invocation_actions(
                request=dataform_v1beta1.QueryWorkflowInvocationActionsRequest(
                    name=invocation_name
                )
            )
            
            failed_actions = []
            for action in actions_pager:
                # Check if the action state is FAILED (enum value 3)
                if action.state == dataform_v1beta1.WorkflowInvocationAction.State.FAILED:
                    target = action.target
                    action_name = f"{target.database}.{target.schema}.{target.name}" if target else "Unknown"
                    error_msg = action.failure_reason if hasattr(action, 'failure_reason') and action.failure_reason else "No error message provided."
                    
                    # Also capture the BigQuery action details if available
                    bq_action = ""
                    if hasattr(action, 'bigquery_action') and action.bigquery_action:
                        if hasattr(action.bigquery_action, 'sql_script') and action.bigquery_action.sql_script:
                            bq_action = f"\n  SQL: {action.bigquery_action.sql_script[:500]}"  # Truncate long SQL
                    
                    failed_actions.append(
                        f"FAILED Task: {action_name}\n"
                        f"  Error: {error_msg}{bq_action}"
                    )
            
            if failed_actions:
                error_parts.append(f"\nFailed Actions ({len(failed_actions)} total):")
                error_parts.extend(failed_actions)
            else:
                error_parts.append("No individual action failures found via Dataform API.")
                
        except Exception as action_err:
            logger.warning(f"Could not query workflow invocation actions: {action_err}")
    
    except Exception as api_err:
        logger.warning(f"Dataform API call failed, falling back to Cloud Logging: {api_err}")
    
    # 3. Fallback / supplement: also query Cloud Logging for additional context
    try:
        logging_client = cloud_logging.Client(project=project_id)
        filter_str = (
            f'resource.type="dataform.googleapis.com/Repository" '
            f'AND labels.workflow_invocation_id="{job_id}" '
            f'AND (jsonPayload.state="FAILED" OR severity="ERROR")'
        )
        entries = logging_client.list_entries(
            filter_=filter_str,
            order_by=cloud_logging.DESCENDING,
            max_results=20
        )
        
        log_failures = []
        for entry in entries:
            if isinstance(entry.json_payload, dict):
                action_id = entry.json_payload.get("actionId", {}).get("name", "")
                err_msg = entry.json_payload.get("errorMessage", "")
                state = entry.json_payload.get("state", "")
                failure_reason = entry.json_payload.get("failureReason", "")
                
                detail = ""
                if action_id and (err_msg or failure_reason):
                    detail = f"Log Action '{action_id}': {err_msg or failure_reason}"
                elif err_msg:
                    detail = f"Log Error: {err_msg}"
                elif failure_reason:
                    detail = f"Log Failure Reason: {failure_reason}"
                elif state == "FAILED":
                    detail = f"Log Entry (FAILED): {json.dumps(entry.json_payload, default=str)[:300]}"
                
                if detail and detail not in log_failures:
                    log_failures.append(detail)
            elif entry.text_payload:
                log_failures.append(f"Log Text: {entry.text_payload[:300]}")
        
        if log_failures:
            error_parts.append("\nAdditional details from Cloud Logging:")
            error_parts.extend(log_failures)
    except Exception as log_err:
        logger.warning(f"Cloud Logging query failed: {log_err}")
    
    return "\n".join(error_parts) if error_parts else ""


def call_dea_agent(token, project_id, location, repo_id, workspace_id, job_id, user_email, original_workspace_id="default", error_details=""):
    """Calls the Data Engineering Agent (DEA) to analyze the failure and apply fixes to the draft workspace."""
    
    url = f"https://geminidataanalytics.googleapis.com/v1/a2a/projects/{project_id}/locations/us/agents/dataengineeringagent/v1/message:stream"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Construct a detailed prompt with error context and fix instructions
    prompt = (
        f"Dataform job {job_id} failed in repository {repo_id} (original workspace: {original_workspace_id}). "
        f"Please analyze the failure, provide a Root Cause Analysis (RCA), and apply the necessary fixes "
        f"directly to the draft workspace {workspace_id}."
    )
    if error_details:
        prompt += f"\n\nHere are the error details, failed tasks, and error messages from the last invocation:\n{error_details}"
    
    # payload matches the user provided template
    payload = {
        "request": {
            "message_id": str(uuid.uuid4()),
            "role": "ROLE_USER",
            "content": {
                "text": prompt
            }
        },
        "metadata": {
            "https://geminidataanalytics.googleapis.com/ext/dea/v1/conversationtoken": "",
            "https://geminidataanalytics.googleapis.com/ext/dea/v1/gcpresource": {
                "gcpResourceId": f"projects/{project_id}/locations/{location}/repositories/{repo_id}/workspaces/{workspace_id}"
            }
        },
        "tenant": f"projects/{project_id}/locations/{location}/agents/dataengineeringagent"
    }

    logger.info(f"Calling DEA API at {url} with prompt: {prompt}")
    
    try:
        response = requests.post(url, headers=headers, json=payload, stream=True)
        response.raise_for_status()
        
        # Accumulate the entire response text
        full_text = ""
        for line in response.iter_lines():
            if line:
                full_text += line.decode('utf-8') + "\n"
        
        try:
            # Parse the accumulated text as JSON array
            data = json.loads(full_text)
            
            # Helper to extract text from dynamic structures
            def extract_text(node):
                if isinstance(node, list):
                    return "\n\n".join(filter(None, (extract_text(item) for item in node)))
                elif isinstance(node, dict):
                    if node.get("role") == "ROLE_USER":
                        return "" # Ignore user prompts
                    if "text" in node and isinstance(node["text"], str):
                        return node["text"]
                    
                    # Traverse all values deeply
                    return "\n".join(filter(None, (extract_text(v) for v in node.values())))
                return ""
            
            extracted_chunks = extract_text(data)
            full_response = extracted_chunks.strip()
            
        except json.JSONDecodeError:
            logger.error("Failed to parse DEA response as JSON, falling back to raw text.")
            full_response = full_text # Fallback to raw text

        if not full_response:
             full_response = "No content received from DEA."
             
        logger.info(f"DEA Analysis Complete. Length: {len(full_response)}")
        return full_response

    except Exception as e:
        logger.error(f"Error calling DEA: {e}")
        return f"Failed to get analysis from DEA. Error: {e}"

def generate_html_email(project_id, location, repo_id, workspace_id, job_id, rca_text, workspace_url, pipeline_name):
    """Generates a beautifully styled HTML email with GCP logo, RCA, and Workspace button."""
    
    # Replace newlines with <br> for HTML rendering of the RCA text
    rca_html = rca_text.replace('\n', '<br>')
    
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: 'Roboto', Arial, sans-serif; color: #3c4043; line-height: 1.5; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #dadce0; border-radius: 8px; }}
            .header {{ display: flex; align-items: center; border-bottom: 1px solid #dadce0; padding-bottom: 15px; margin-bottom: 20px; }}
            .logo {{ width: 40px; height: 40px; margin-right: 15px; }}
            .title {{ font-size: 20px; font-weight: 500; color: #1a73e8; }}
            .details {{ background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
            .details p {{ margin: 5px 0; font-size: 14px; }}
            .rca-section {{ margin-bottom: 20px; }}
            .rca-title {{ font-weight: 500; font-size: 16px; margin-bottom: 10px; }}
            .rca-box {{ background-color: #fce8e6; padding: 15px; border-left: 4px solid #d93025; border-radius: 4px; font-family: monospace; font-size: 13px; white-space: pre-wrap; overflow-x: auto; }}
            .remark {{ font-size: 12px; color: #5f6368; margin-top: 5px; font-style: italic; }}
            .button-container {{ text-align: center; margin-top: 30px; }}
            .button {{ background-color: #1a73e8; color: white !important; padding: 10px 24px; text-decoration: none; border-radius: 4px; font-weight: 500; font-size: 14px; display: inline-block; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <!-- GCP Logo -->
                <img class="logo" src="https://cloud.google.com/_static/cloud/images/social-icon-google-cloud-1200-630.png" alt="Google Cloud Logo">
                <div class="title">Data Engineering Agent RCA Report</div>
            </div>
            
            <div class="details">
                <p><strong>Project:</strong> {project_id}</p>
                <p><strong>Repository:</strong> {repo_id}</p>
                <p><strong>Workspace:</strong> {workspace_id}</p>
                <p><strong>Job/Invocation ID:</strong> {job_id}</p>
            </div>
            
            <div class="rca-section">
                <div class="rca-title">Root Cause Analysis</div>
                <div class="remark">Please carefully copy the RCA text below if you need to use it in your terminal or documentation.</div>
                <div class="rca-box">
                    {rca_html}
                </div>
            </div>
            
            <div class="button-container">
                <a href="{workspace_url}" class="button" target="_blank">Open {pipeline_name}</a>
            </div>
        </div>
    </body>
    </html>
    """
    return html

def send_email(project_id, recipient, subject, html_body, rca_text, workspace_url, pipeline_name):
    """
    Dynamically injects the RCA into the Cloud Monitoring Alert Policy documentation field.
    Then triggers the log metric so the email includes the updated RCA!
    """
    logger.info("Updating Alert Policy documentation...")
    policy_updated = False
    policy_name = None
    try:
        from google.cloud import monitoring_v3
        client = monitoring_v3.AlertPolicyServiceClient()
        parent = f"projects/{project_id}"
        
        request = monitoring_v3.ListAlertPoliciesRequest(
            name=parent,
            filter='displayName="Dataform RCA Generated Alert"'
        )
        
        policy_to_update = None
        for policy in client.list_alert_policies(request=request):
            policy_to_update = policy
            policy_name = policy.name
            logger.info(f"Found alert policy: {policy.name}")
            break
            
        if policy_to_update:
            docs = f"**{subject}**\n\nThe Data Engineering Agent generated the following Root Cause Analysis:\n\n{rca_text}\n\n---\n\n**[🚀 Click here to open the {pipeline_name}]({workspace_url})**"
            
            # Cloud Monitoring documentation.content has a hard limit of
            # 8,192 Unicode characters / 10,240 UTF-8 bytes.
            # Sanitize: remove null bytes and other control characters that
            # could cause validation errors.
            docs = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', docs)
            
            MAX_DOC_CHARS = 8192
            if len(docs) > MAX_DOC_CHARS:
                # Reserve space for the truncation notice + workspace link
                suffix = f"\n\n... *(truncated — full RCA available in Cloud Function logs)*\n\n---\n\n**[🚀 Click here to open the {pipeline_name}]({workspace_url})**"
                # Truncate the RCA portion, keeping the suffix intact
                available = MAX_DOC_CHARS - len(suffix)
                docs = docs[:available] + suffix
                logger.info(f"Truncated documentation content from {len(docs)} to {MAX_DOC_CHARS} chars (limit: 8192)")
            
            policy_to_update.documentation.content = docs
            
            update_mask = {"paths": ["documentation.content"]}
            update_req = monitoring_v3.UpdateAlertPolicyRequest(
                alert_policy=policy_to_update,
                update_mask=update_mask
            )
            updated_policy = client.update_alert_policy(request=update_req)
            logger.info(f"Successfully injected RCA into Alert Policy documentation! Policy: {updated_policy.name}")
            policy_updated = True
        else:
            logger.warning("Could not find the target Alert Policy 'Dataform RCA Generated Alert' to inject text into!")
            
    except Exception as e:
        logger.error(f"Failed to inject dynamic documentation: {e}", exc_info=True)
        
    logger.info(f"HTML Body:\n{html_body}")
    
    if policy_updated:
        # Wait for the Alert Policy update to propagate before triggering the alert
        # Cloud Monitoring can take up to 30-60s to propagate documentation changes
        logger.info("Waiting 30 seconds for Alert Policy update to propagate...")
        time.sleep(30)
        
        # Verify the update actually took effect
        try:
            from google.cloud import monitoring_v3
            verify_client = monitoring_v3.AlertPolicyServiceClient()
            verify_req = monitoring_v3.GetAlertPolicyRequest(name=policy_name)
            verified_policy = verify_client.get_alert_policy(request=verify_req)
            current_docs = verified_policy.documentation.content if verified_policy.documentation else ""
            
            if subject in current_docs:
                logger.info(f"Verified: Alert Policy documentation contains RCA (length: {len(current_docs)} chars)")
            else:
                logger.warning(f"Alert Policy documentation may not have propagated yet. Current content starts with: {current_docs[:200]}...")
                # Wait an additional 30 seconds and try once more
                logger.info("Waiting additional 30 seconds for propagation...")
                time.sleep(30)
        except Exception as verify_err:
            logger.warning(f"Could not verify alert policy update: {verify_err}")
    else:
        logger.warning("Skipping propagation wait since policy update was not successful. Triggering alert anyway.")
    
    logger.warning("DATAFORM_RCA_GENERATED", extra={"json_fields": {"rca_subject": subject}})

@functions_framework.cloud_event
def troubleshoot_dataform(cloud_event):
    """Entry point for Cloud Function triggered by Pub/Sub."""
    try:
        data = base64.b64decode(cloud_event.data["message"]["data"]).decode()
        log_entry = json.loads(data)
        
        logger.info(f"Received log entry: {json.dumps(log_entry, indent=2)}")

        # Extract relevant info from the log entry
        resource = log_entry.get("resource", {})
        labels = resource.get("labels", {})
        
        project_id = labels.get("project_id") or log_entry.get("resourceLabels", {}).get("project_id")
        location = labels.get("location") or log_entry.get("resourceLabels", {}).get("location")
        repo_id = labels.get("repository_id")
        
        job_id = "unknown_job"
        if "workflow_invocation_id" in labels:
            job_id = labels["workflow_invocation_id"]
        elif "resource" in log_entry and log_entry["resource"].get("type") == "dataform_workflow_invocation":
             job_id = labels.get("workflow_invocation_id")

        if job_id == "unknown_job":
             payload = log_entry.get("jsonPayload", {})
             if "workflowInvocationId" in payload:
                 job_id = payload["workflowInvocationId"]

        if not project_id: 
            log_name = log_entry.get("logName", "")
            parts = log_name.split("/")
            if len(parts) > 1 and parts[0] == "projects":
                project_id = parts[1]

        logger.info(f"Extracted Context: Project={project_id}, Location={location}, Repo={repo_id}, Job={job_id}")

        if not (project_id and location and repo_id):
            logger.warning("Missing essential context (Project/Location/Repo). Skipping DEA call.")
            return

        user_email_raw = os.environ.get("USER_EMAIL", "")
        if not user_email_raw:
            logger.warning("USER_EMAIL environment variable not set. Falling back to default.")
            user_email_raw = "you@example.com" # Default placeholder

        token = get_access_token()
        bq_pipeline = is_bq_pipeline(token, project_id, location, repo_id)

        # Support comma-separated emails; use the first one for workspace naming
        user_emails = [e.strip() for e in user_email_raw.split(",") if e.strip()]
        user_email = user_emails[0] if user_emails else "you@example.com"
        original_workspace_id = labels.get("workspace_id", "default")
        
        # Convert email to draft workspace ID format: sametkaradag_google_com-agent-draft
        safe_email = re.sub(r'[@\.]', '_', user_email)
        draft_workspace_id = f"{safe_email}-agent-draft"
        
        # Prepare the draft workspace for the DEA to apply fixes
        logger.info(f"Preparing draft workspace: {draft_workspace_id}")
        prepare_draft_workspace(token, project_id, location, repo_id, draft_workspace_id)
        
        # Retrieve detailed error information using Dataform API + Cloud Logging
        logger.info(f"Retrieving invocation error details for job {job_id}...")
        error_details = get_invocation_error_details(project_id, location, repo_id, job_id)
        
        if error_details:
            logger.info(f"Retrieved error details ({len(error_details)} chars): {error_details[:500]}...")
        else:
            # Final fallback: use whatever the triggering log entry contained
            logger.warning("No error details retrieved from Dataform API or Cloud Logging. Using trigger payload.")
            error_details = log_entry.get("textPayload", "")
            if not error_details and "jsonPayload" in log_entry:
                error_details = json.dumps(log_entry["jsonPayload"], indent=2)

        analysis = call_dea_agent(
            token, project_id, location, repo_id, draft_workspace_id, job_id, user_email,
            original_workspace_id=original_workspace_id, error_details=error_details
        )
       
 
        if bq_pipeline:
            workspace_url = f"https://console.cloud.google.com/bigquery?ws=!1m6!1m5!19m4!1m3!1s{project_id}!2s{location}!3s{repo_id}"
            pipeline_name = "BigQuery Pipeline"
        else:
            workspace_url = f"https://console.cloud.google.com/bigquery/dataform/locations/{location}/repositories/{repo_id}/workspaces/{draft_workspace_id}?project={project_id}"
            pipeline_name = "Dataform Workspace"

        html_email = generate_html_email(project_id, location, repo_id, draft_workspace_id, job_id, analysis, workspace_url, pipeline_name)
        
        subject = f"Dataform Job Failure RCA: {job_id}"
        send_email(project_id, user_email, subject, html_email, analysis, workspace_url, pipeline_name)

    except Exception as e:
        logger.error(f"Error processing processing event: {e}")
        raise

