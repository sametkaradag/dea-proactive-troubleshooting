import requests
import uuid
import subprocess

token = subprocess.check_output(["gcloud", "auth", "print-access-token"]).decode().strip()
project_id = "bq-dataworkeragent-test"
location = "us-central1"
repo_id = "pipeline-optimization-column-pruning"
job_id = "1771520590-3d43767e-e27c-4bd4-ad96-83772597ff25"

url = f"https://geminidataanalytics.googleapis.com/v1/a2a/projects/{project_id}/locations/{location}/agents/dataengineeringagent/v1/message:stream"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

prompt = f"Dataform job {job_id} failed in repository {repo_id}. Please analyze the failure and provide a Root Cause Analysis (RCA)."

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
            "gcpResourceId": f"projects/{project_id}/locations/{location}/repositories/{repo_id}"
        }
    },
    "tenant": f"projects/{project_id}/locations/{location}/agents/dataengineeringagent"
}

response = requests.post(url, headers=headers, json=payload, stream=True)
print(f"Status Code: {response.status_code}")
for line in response.iter_lines():
    if line:
        print(line.decode('utf-8'))

