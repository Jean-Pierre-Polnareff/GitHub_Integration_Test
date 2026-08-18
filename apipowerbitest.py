import msal
import requests
import time
import json

# ── CONFIG ──────────────────────────────────────────────
TENANT_ID     = "_kj_41j2_i1c2n09241inkl1421241"
CLIENT_ID     = "ioqienkalvnx_12ljblbjf_14281h"
CLIENT_SECRET = "sjannuvn398204u51mnda89013h530580hds0n"

AUTHORITY     = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE         = ["https://analysis.windows.net/powerbi/api/.default"]
BASE_URL      = "https://api.powerbi.com/v1.0/myorg"
# ────────────────────────────────────────────────────────


def get_access_token():
    """Authenticate and get access token using Client Credentials (Service Principal)."""
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=SCOPE)

    if "access_token" in result:
        print("✅ Token acquired successfully")
        return result["access_token"]
    else:
        raise Exception(f"❌ Token error: {result.get('error_description')}")


def get_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

#token = get_access_token()
#print(token)
token = "9310hr5oi23hroiwndsnlf_nzmlnfc0ihr092j3r1adfposf9j2-3"
def list_workspaces(token):
    """List all workspaces the service principal has access to."""
    url = f"{BASE_URL}/groups"
    res = requests.get(url, headers=get_headers(token))
    print(res.content)
    res.raise_for_status()
    workspaces = res.json().get("value", [])
    print(f"\n📁 Workspaces ({len(workspaces)} found):")
    for ws in workspaces:
        print(f"  - {ws['name']}  | ID: {ws['id']}")
    return workspaces

workspaces = list_workspaces(token)

def list_reports(token,workspace_id):
    """List all reports the service principal has access to."""
    url = f"{BASE_URL}/groups"
    url = f"{BASE_URL}/groups/{workspace_id}/reports"
    res = requests.get(url, headers=get_headers(token))
    print(res.content)
    res.raise_for_status()
    reports = res.json().get("value", [])
    print(f"\n📁 reports ({len(reports)} found):")
    for r in reports:
        print(f"  - {r['name']}  | ID: {r['id']}")
    return reports

reports = list_reports(token,"225bd050-c167-45d5-af87-6616424e8143")

def list_pages(token,workspace_id,report_id):
    """List all pages the service principal has access to."""
    url = f"{BASE_URL}/groups"
    url = f"{BASE_URL}/groups/{workspace_id}/reports/{report_id}/pages"
    res = requests.get(url, headers=get_headers(token))
    print(res.content)
    res.raise_for_status()
    pages = res.json().get("value", [])
    print(f"\n📁 pages ({len(pages)} found):")
    for p in pages:
        print(f"  - {p['name']}  | DisplayName: {p['displayName']}")
    return pages

pages = list_pages(token,"225bd050-c167-45d5-af87-6616424e8143","81ded363-b89f-4d94-b79a-5f7e7461ff46")


def export_report_pages(token, workspace_id, report_id, file_format="PNG"):
    """Export all pages of a report to PDF, PNG, or PPTX."""

    # Step 1: Get all pages
    url = f"{BASE_URL}/groups/{workspace_id}/reports/{report_id}/pages"
    res = requests.get(url, headers=get_headers(token))
    res.raise_for_status()
    pages = res.json().get("value", [])
    print(f"\n📁 Pages found ({len(pages)}):")
    for p in pages:
        print(f"  - {p['displayName']}")

    # Step 2: Trigger export
    export_url = f"{BASE_URL}/groups/{workspace_id}/reports/{report_id}/ExportTo"
    body = {
        "format": file_format,
        "powerBIReportConfiguration": {
            "pages": [{"pageName": p["name"]} for p in pages]
        }
    }
    res = requests.post(export_url, headers=get_headers(token), json=body)
    print(res.content)
    res.raise_for_status()
    export_id = res.json().get("id")
    print(f"\n⏳ Export triggered. Export ID: {export_id}")

    # Step 3: Poll until done
    status_url = f"{BASE_URL}/groups/{workspace_id}/reports/{report_id}/exports/{export_id}"
    while True:
        res = requests.get(status_url, headers=get_headers(token))
        res.raise_for_status()
        status = res.json().get("status")
        print(f"  Status: {status}")
        if status == "Succeeded":
            break
        elif status == "Failed":
            raise Exception("Export failed.")
        time.sleep(3)

    # Step 4: Download the file
    download_url = f"{status_url}/file"
    res = requests.get(download_url, headers=get_headers(token))
    res.raise_for_status()
    filename = f"report_export.{file_format.lower()}"
    with open(filename, "wb") as f:
        f.write(res.content)
    print(f"\n✅ Export saved as: {filename}")
    return filename

export_report_pages(token,"225bd050-c167-45d5-af87-6616424e8143","81ded363-b89f-4d94-b79a-5f7e7461ff46")