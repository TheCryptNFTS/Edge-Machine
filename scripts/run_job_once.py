import os
import sys
import requests

PM_API_BASE = os.getenv("PM_API_BASE")
PM_ADMIN_TOKEN = os.getenv("PM_ADMIN_TOKEN")
JOB_NAME = os.getenv("JOB_NAME")

if not PM_API_BASE:
    print("Missing PM_API_BASE")
    sys.exit(1)
if not PM_ADMIN_TOKEN:
    print("Missing PM_ADMIN_TOKEN")
    sys.exit(1)
if not JOB_NAME:
    print("Missing JOB_NAME")
    sys.exit(1)

url = f"{PM_API_BASE.rstrip('/')}/v1/admin/jobs/run"
params = {"job_name": JOB_NAME}
headers = {"x-admin-token": PM_ADMIN_TOKEN}

print(f"Running job: {JOB_NAME}")
r = requests.post(url, params=params, headers=headers, timeout=30)

print("Status:", r.status_code)
print(r.text)

if r.status_code != 200:
    sys.exit(1)