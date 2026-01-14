import os
import time
import requests
import sys

API = os.getenv("PM_API_BASE", "http://localhost:8000").rstrip("/")
TOKEN = os.getenv("PM_ADMIN_TOKEN")
JOB = sys.argv[1]

def wait_for_health():
    for _ in range(15):
        try:
            r = requests.get(f"{API}/health", timeout=5)
            if r.ok:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False

if not wait_for_health():
    raise SystemExit("API not healthy")

r = requests.post(
    f"{API}/v1/admin/jobs/run",
    params={"job_name": JOB},
    headers={"x-admin-token": TOKEN},
    timeout=30,
)

print(r.status_code, r.text)
r.raise_for_status()