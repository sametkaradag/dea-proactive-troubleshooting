import base64
import json
import logging
import os
import uuid
import re
import requests
import google.auth
import functions_framework
from google.auth.transport.requests import Request

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_access_token():
    credentials, project = google.auth.default()
    credentials.refresh(Request())
    return credentials.token

def prepare_draft_workspace(project_id, location, repo_id, workspace_id):
    """Creates a draft workspace if it doesn't exist."""
    token = get_access_token()
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

def call_dea_agent(project_id, location, repo_id, workspace_id, job_id, user_email, original_workspace_id):
    """Calls the Data Engineering Agent (DEA) to analyze the failure."""
    
    url = f"https://geminidataanalytics.googleapis.com/v1/a2a/projects/{project_id}/locations/{location}/agents/dataengineeringagent/v1/message:stream"
    
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Construct the prompt
    prompt = (
        f"Dataform job {job_id} failed in repository {repo_id} (original workspace: {original_workspace_id}). "
        f"Please analyze the failure, provide a Root Cause Analysis (RCA), and apply the necessary fixes "
        f"directly to the draft workspace {workspace_id}."
    )
    
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

def generate_html_email(project_id, location, repo_id, workspace_id, job_id, rca_text):
    """Generates a beautifully styled HTML email with GCP logo, RCA, and Workspace button."""
    workspace_url = f"https://console.cloud.google.com/bigquery/dataform/locations/{location}/repositories/{repo_id}/workspaces/{workspace_id}?project={project_id}"
    
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
                <a href="{workspace_url}" class="button" target="_blank">Open Dataform Workspace</a>
            </div>
        </div>
    </body>
    </html>
    """
    return html

def send_email(project_id, recipient, subject, html_body, rca_text, workspace_url):
    """
    Dynamically injects the RCA into the Cloud Monitoring Alert Policy documentation field.
    Then triggers the log metric so the email includes the updated RCA!
    """
    logger.info("Updating Alert Policy documentation...")
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
            break
            
        if policy_to_update:
            docs = f"**{subject}**\n\nThe Data Engineering Agent generated the following Root Cause Analysis:\n\n{rca_text}\n\n---\n\n**[Click here to open the Dataform Workspace]({workspace_url})**"
            policy_to_update.documentation.content = docs
            
            update_mask = {"paths": ["documentation.content"]}
            update_req = monitoring_v3.UpdateAlertPolicyRequest(
                alert_policy=policy_to_update,
                update_mask=update_mask
            )
            client.update_alert_policy(request=update_req)
            logger.info("Successfully dynamically injected RCA into Alert Policy documentation!")
        else:
            logger.warning("Could not find the target Alert Policy to inject text into!")
            
    except Exception as e:
        logger.error(f"Failed to inject dynamic documentation: {e}")
        
    logger.info(f"HTML Body:\n{html_body}")
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

        user_email = os.environ.get("USER_EMAIL", "sametkaradag@google.com")
        
        # Convert email to draft workspace ID format: sametkaradag_google_com-agent-draft
        safe_email = re.sub(r'[@\.]', '_', user_email)
        draft_workspace_id = f"{safe_email}-agent-draft"
        
        # Prepare the draft workspace
        prepare_draft_workspace(project_id, location, repo_id, draft_workspace_id)
        
        original_workspace_id = labels.get("workspace_id", "default")
        
        analysis = call_dea_agent(project_id, location, repo_id, draft_workspace_id, job_id, user_email, original_workspace_id)
        html_email = generate_html_email(project_id, location, repo_id, draft_workspace_id, job_id, analysis)
        
        workspace_url = f"https://console.cloud.google.com/bigquery/dataform/locations/{location}/repositories/{repo_id}/workspaces/{draft_workspace_id}?project={project_id}"
        subject = f"Dataform Job Failure RCA: {job_id}"
        send_email(project_id, user_email, subject, html_email, analysis, workspace_url)

    except Exception as e:
        logger.error(f"Error processing processing event: {e}")
        raise
