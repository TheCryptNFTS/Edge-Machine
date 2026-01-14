from __future__ import annotations

import os
import time
import uuid
import secrets
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from typing import Optional, List

import requests
from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from pm.ensemble import compute_machine_p

# ======================================================
# CONFIG
# ======================================================

DB_PATH = os.getenv("PM_DB_PATH", "./pm.db")
ADMIN_TOKEN = os.getenv("PM_ADMIN_TOKEN", "edge-machine-admin-2026")

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

DISCOVER_LIMIT = 50
DISCOVER_PAGES = 5
DISCOVER_PAGE_SIZE = 100
DISCOVER_DETAIL_MAX = 25

HYDRATE_MAX = 50
PRICE_UPDATE_MAX = 50
FORECAST_MAX = 200

JOB_TIME_BUDGET_SECS = 12

# ======================================================
# APP
# ======================================================

app = FastAPI(title="Edge Machine API", version="1.5.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======================================================
# DATABASE
# ======================================================

def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with closing(get_conn()) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            created_at TEXT NOT NULL,
            slug TEXT,
            gamma_market_id TEXT,
            yes_token_id TEXT,
            latest_pm_p REAL,
            latest_machine_p REAL
        )
        """)
        conn.commit()

init_db()

# ======================================================
# MODELS
# ======================================================

class EventOut(BaseModel):
    id: str
    title: str
    slug: Optional[str]
    gamma_market_id: Optional[str]
    yes_token_id: Optional[str]
    latest_pm_p: Optional[float]
    latest_machine_p: Optional[float]

# ======================================================
# AUTH
# ======================================================

def check_admin(x_admin_token: Optional[str]):
    if not x_admin_token or not secrets.compare_digest(x_admin_token, ADMIN_TOKEN):
        raise HTTPException(status_code=401, detail="Invalid admin token")

# ======================================================
# UTILS
# ======================================================

def clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))

# ======================================================
# POLYMARKET HELPERS
# ======================================================

def gamma_markets(limit: int, offset: int) -> list[dict]:
    r = requests.get(
        f"{GAMMA_BASE}/markets",
        params={"limit": limit, "offset": offset, "active": "true"},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    # gamma sometimes returns list OR dict
    if isinstance(data, list):
        return data
    return data.get("markets") or data.get("data") or []

def gamma_detail(mid: str) -> Optional[dict]:
    try:
        r = requests.get(f"{GAMMA_BASE}/markets/{mid}", timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

def extract_yes_token_id(m: dict) -> Optional[str]:
    tokens = m.get("tokens") or m.get("outcomes") or []
    if isinstance(tokens, list):
        for t in tokens:
            label = (t.get("label") or t.get("outcome") or "").strip().lower()
            if label == "yes":
                for k in ("token_id", "tokenId", "id", "clobTokenId"):
                    if t.get(k):
                        return str(t[k])

    # fallback ONLY if it is a list of length 2
    ids = m.get("clobTokenIds")
    if isinstance(ids, list) and len(ids) == 2:
        # ordering NOT guaranteed; but better than nothing
        return str(ids[0])

    return None

def clob_midpoint(token_id: str) -> Optional[float]:
    try:
        r = requests.get(
            f"{CLOB_BASE}/midpoint",
            params={"token_id": token_id},
            timeout=10,
        )
        r.raise_for_status()
        mp = r.json().get("midpoint")
        if mp is None:
            return None
        return float(mp)
    except Exception:
        return None

# ======================================================
# ROUTES
# ======================================================

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/v1/events", response_model=List[EventOut])
def list_events(limit: int = 50):
    with closing(get_conn()) as conn:
        rows = conn.execute(
            "SELECT * FROM events ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [EventOut(**dict(r)) for r in rows]

@app.post("/v1/admin/jobs/run")
def run_job(
    job_name: str = Query(...),
    x_admin_token: Optional[str] = Header(None),
):
    check_admin(x_admin_token)

    if job_name not in {"discover_markets", "hydrate_tokens", "update_prices", "forecast_machine"}:
        raise HTTPException(400, "Unknown job")

    t0 = time.time()

    # ==================================================
    # DISCOVER MARKETS
    # ==================================================
    if job_name == "discover_markets":
        discovered = 0
        tokened = 0
        detail_calls = 0

        with closing(get_conn()) as conn:
            for page in range(DISCOVER_PAGES):
                if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                    break

                markets = gamma_markets(DISCOVER_PAGE_SIZE, page * DISCOVER_PAGE_SIZE)

                for m in markets:
                    if discovered >= DISCOVER_LIMIT:
                        break

                    title = m.get("question") or m.get("title")
                    mid = m.get("id")
                    if not title or not mid:
                        continue

                    yes_tid = extract_yes_token_id(m)

                    if not yes_tid and detail_calls < DISCOVER_DETAIL_MAX:
                        d = gamma_detail(str(mid))
                        detail_calls += 1
                        if d:
                            yes_tid = extract_yes_token_id(d)

                    conn.execute(
                        """
                        INSERT OR IGNORE INTO events
                        (id, title, created_at, slug, gamma_market_id, yes_token_id)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(uuid.uuid4()),
                            title.strip(),
                            datetime.now(timezone.utc).isoformat(),
                            m.get("slug"),
                            str(mid),
                            yes_tid,
                        ),
                    )

                    discovered += 1
                    if yes_tid:
                        tokened += 1

            conn.commit()

        return {
            "ok": True,
            "job": job_name,
            "discovered": discovered,
            "tokened": tokened,
            "detail_calls": detail_calls,
            "time_secs": round(time.time() - t0, 3),
        }

    # ==================================================
    # HYDRATE TOKENS
    # ==================================================
    if job_name == "hydrate_tokens":
        attempted = 0
        hydrated = 0

        with closing(get_conn()) as conn:
            rows = conn.execute(
                """
                SELECT id, gamma_market_id
                FROM events
                WHERE yes_token_id IS NULL
                LIMIT ?
                """,
                (HYDRATE_MAX,),
            ).fetchall()

            for r in rows:
                if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                    break

                attempted += 1
                d = gamma_detail(r["gamma_market_id"])
                if not d:
                    continue

                yes_tid = extract_yes_token_id(d)
                if not yes_tid:
                    continue

                conn.execute(
                    "UPDATE events SET yes_token_id=? WHERE id=?",
                    (yes_tid, r["id"]),
                )
                hydrated += 1

            conn.commit()

        return {
            "ok": True,
            "job": job_name,
            "attempted": attempted,
            "hydrated": hydrated,
            "time_secs": round(time.time() - t0, 3),
        }

    # ==================================================
    # UPDATE PRICES (ONLY PRICES)
    # ==================================================
    if job_name == "update_prices":
        snap = 0
        skipped = 0

        with closing(get_conn()) as conn:
            rows = conn.execute(
                """
                SELECT id, yes_token_id
                FROM events
                LIMIT ?
                """,
                (PRICE_UPDATE_MAX,),
            ).fetchall()

            for r in rows:
                if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                    break

                if not r["yes_token_id"]:
                    skipped += 1
                    continue

                p = clob_midpoint(r["yes_token_id"])
                if p is None:
                    continue

                conn.execute(
                    "UPDATE events SET latest_pm_p=? WHERE id=?",
                    (clamp01(p), r["id"]),
                )
                snap += 1

            conn.commit()

        return {
            "ok": True,
            "job": job_name,
            "snapshot_updated": snap,
            "skipped_no_token": skipped,
            "time_secs": round(time.time() - t0, 3),
        }

    # ==================================================
    # FORECAST MACHINE (ONLY MACHINE)
    # ==================================================
    if job_name == "forecast_machine":
        updated = 0
        skipped = 0

        with closing(get_conn()) as conn:
            rows = conn.execute(
                """
                SELECT id, latest_pm_p
                FROM events
                WHERE latest_pm_p IS NOT NULL
                LIMIT ?
                """,
                (FORECAST_MAX,),
            ).fetchall()

            for r in rows:
                if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                    break

                p = r["latest_pm_p"]
                if p is None:
                    skipped += 1
                    continue

                machine_p = compute_machine_p(float(p))
                conn.execute(
                    "UPDATE events SET latest_machine_p=? WHERE id=?",
                    (machine_p, r["id"]),
                )
                updated += 1

            conn.commit()

        return {
            "ok": True,
            "job": job_name,
            "updated": updated,
            "skipped": skipped,
            "time_secs": round(time.time() - t0, 3),
        }