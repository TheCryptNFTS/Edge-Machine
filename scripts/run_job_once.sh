#!/usr/bin/env bash
set -euo pipefail

: "${PM_API_BASE:?Need PM_API_BASE}"
: "${PM_ADMIN_TOKEN:?Need PM_ADMIN_TOKEN}"
: "${JOB_NAME:?Need JOB_NAME}"

curl -sS -X POST \
  -H "x-admin-token: ${PM_ADMIN_TOKEN}" \
  "${PM_API_BASE}/v1/admin/jobs/run?job_name=${JOB_NAME}"
echo