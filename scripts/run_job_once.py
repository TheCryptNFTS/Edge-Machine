import os
import time
import requests

API = os.environ.get("PM_API_BASE", "http://localhost:8000").rstrip("/")
TOKEN = os.environ["PM_ADMIN_TOKEN"]
JOB = os.environ["JOB_NAME"]

def wait_for_health() -> bool:
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
    raise SystemExit("API not healthy after 30s")

url = f"{API}/v1/admin/jobs/run"
params = {"job_name": JOB}
headers = {"x-admin-token": TOKEN}

last_err = None
for attempt in range(4):
    try:
        r = requests.post(url, params=params, headers=headers, timeout=30)
        print(f"Status: {r.status_code} | {r.text}")
        r.raise_for_status()
        raise SystemExit(0)
    except Exception as e:
        last_err = e
        time.sleep(1.5 * (attempt + 1))

raise SystemExit(f"Job failed after retries: {last_err}")