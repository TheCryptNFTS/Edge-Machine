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

# ==========================================================
# CONFIG
# ==========================================================

DATABASE_URL = os.getenv("PM_DATABASE_URL", "sqlite:///./pm.db")
DB_PATH = DATABASE_URL.replace("sqlite:///", "")

ADMIN_TOKEN = os.getenv("PM_ADMIN_TOKEN", os.getenv("ADMIN_TOKEN", "edge-machine-admin-2026"))

GAMMA_BASE = os.getenv("PM_GAMMA_BASE", "https://gamma-api.polymarket.com").rstrip("/")
CLOB_BASE = os.getenv("PM_CLOB_BASE", "https://clob.polymarket.com").rstrip("/")

DISCOVER_LIMIT = int(os.getenv("PM_DISCOVER_LIMIT", "50"))
DISCOVER_PAGES = int(os.getenv("PM_DISCOVER_PAGES", "5"))
DISCOVER_PAGE_SIZE = int(os.getenv("PM_DISCOVER_PAGE_SIZE", "100"))
DISCOVER_DETAIL_MAX = int(os.getenv("PM_DISCOVER_DETAIL_MAX", "25"))
JOB_TIME_BUDGET_SECS = int(os.getenv("PM_JOB_TIME_BUDGET_SECS", "10"))

HYDRATE_MAX = int(os.getenv("PM_HYDRATE_MAX", "50"))
PRICE_UPDATE_MAX = int(os.getenv("PM_PRICE_UPDATE_MAX", "50"))

DISCOVER_KEYWORDS = os.getenv(
    "PM_DISCOVER_KEYWORDS",
    "bitcoin,btc,ethereum,eth,sol,solana,crypto,ai,trump,election,fed"
)
STRICT_KEYWORDS = os.getenv("PM_STRICT_KEYWORDS", "false").lower() == "true"

# ==========================================================
# APP
# ==========================================================

app = FastAPI(title="Edge Machine API", version="1.4.0-stable")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================================
# DATABASE
# ==========================================================

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

# ==========================================================
# MODELS
# ==========================================================

class EventOut(BaseModel):
    id: str
    title: str
    slug: Optional[str]
    gamma_market_id: Optional[str]
    yes_token_id: Optional[str]
    latest_pm_p: Optional[float]
    latest_machine_p: Optional[float]

class EventCreate(BaseModel):
    title: str

# ==========================================================
# AUTH
# ==========================================================

def check_admin(token: Optional[str]):
    if not token or not secrets.compare_digest(token, ADMIN_TOKEN):
        raise HTTPException(status_code=401, detail="Invalid admin token")

# ==========================================================
# UTILS
# ==========================================================

def clamp01(x: float) -> float:
    try:
        return max(0.0, min(1.0, float(x)))
    except Exception:
        return 0.5

def machine_from_pm(p: float) -> float:
    return clamp01(0.9 * p + 0.05)

# ==========================================================
# POLYMARKET – CLOB
# ==========================================================

def clob_midpoint(token_id: str) -> Optional[float]:
    for endpoint in ("midpoint", "price"):
        try:
            r = requests.get(
                f"{CLOB_BASE}/{endpoint}",
                params={"token_id": token_id},
                timeout=8,
            )
            r.raise_for_status()
            data = r.json()
            return float(data.get("midpoint") or data.get("price"))
        except Exception:
            continue
    return None

# ==========================================================
# POLYMARKET – GAMMA
# ==========================================================

def gamma_get_markets(limit: int, offset: int) -> list[dict]:
    r = requests.get(
        f"{GAMMA_BASE}/markets",
        params={"limit": limit, "offset": offset, "active": "true"},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else data.get("markets", [])

def gamma_get_detail(mid: str) -> Optional[dict]:
    for url in (f"{GAMMA_BASE}/markets/{mid}", f"{GAMMA_BASE}/market/{mid}"):
        try:
            r = requests.get(url, timeout=15)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
    return None

def extract_yes_token_id(m: dict) -> Optional[str]:
    for t in m.get("tokens", []) + m.get("outcomes", []):
        if (t.get("label") or t.get("outcome") or "").lower() == "yes":
            for k in ("token_id", "id", "clobTokenId"):
                if t.get(k):
                    return str(t[k])

    ids = m.get("clobTokenIds") or m.get("clob_token_ids")
    if isinstance(ids, list) and len(ids) == 2:
        return str(ids[0])  # risky fallback

    return None

# ==========================================================
# ROUTES
# ==========================================================

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

@app.post("/v1/admin/events", response_model=EventOut)
def admin_create_event(
    payload: EventCreate,
    x_admin_token: Optional[str] = Header(None),
):
    check_admin(x_admin_token)

    eid = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    with closing(get_conn()) as conn:
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (eid, payload.title, now, None, None, None, None, None),
        )
        conn.commit()

    return EventOut(
        id=eid,
        title=payload.title,
        slug=None,
        gamma_market_id=None,
        yes_token_id=None,
        latest_pm_p=None,
        latest_machine_p=None,
    )

@app.post("/v1/admin/jobs/run")
def run_job(
    job_name: str = Query(...),
    x_admin_token: Optional[str] = Header(None),
):
    check_admin(x_admin_token)
    start = time.time()

    # ======================================================
    # DISCOVER
    # ======================================================
    if job_name == "discover_markets":
        markets: list[dict] = []

        for page in range(DISCOVER_PAGES):
            if time.time() - start > JOB_TIME_BUDGET_SECS:
                break
            markets.extend(
                gamma_get_markets(
                    DISCOVER_PAGE_SIZE,
                    page * DISCOVER_PAGE_SIZE,
                )
            )

        keywords = [k.strip().lower() for k in DISCOVER_KEYWORDS.split(",")]

        used = tokened = detail_calls = 0

        with closing(get_conn()) as conn:
            for m in markets[:DISCOVER_LIMIT]:
                title = (m.get("question") or m.get("title") or "").strip()
                if not title:
                    continue

                if STRICT_KEYWORDS and not any(k in title.lower() for k in keywords):
                    continue

                mid = m.get("id")
                slug = m.get("slug")

                yes_tid = extract_yes_token_id(m)

                if not yes_tid and detail_calls < DISCOVER_DETAIL_MAX:
                    detail = gamma_get_detail(str(mid))
                    detail_calls += 1
                    if detail:
                        yes_tid = extract_yes_token_id(detail)

                conn.execute(
                    """
                    INSERT OR IGNORE INTO events
                    (id, title, created_at, slug, gamma_market_id, yes_token_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        title,
                        datetime.now(timezone.utc).isoformat(),
                        slug,
                        str(mid),
                        yes_tid,
                    ),
                )

                used += 1
                if yes_tid:
                    tokened += 1

            conn.commit()

        return {
            "ok": True,
            "job": job_name,
            "discovered": used,
            "tokened": tokened,
            "detail_calls": detail_calls,
            "time_secs": round(time.time() - start, 3),
        }

    # ======================================================
    # HYDRATE
    # ======================================================
    if job_name == "hydrate_tokens":
        attempted = hydrated = 0

        with closing(get_conn()) as conn:
            rows = conn.execute(
                """
                SELECT id, gamma_market_id FROM events
                WHERE yes_token_id IS NULL AND gamma_market_id IS NOT NULL
                LIMIT ?
                """,
                (HYDRATE_MAX,),
            ).fetchall()

            for r in rows:
                attempted += 1
                detail = gamma_get_detail(r["gamma_market_id"])
                if not detail:
                    continue

                yes_tid = extract_yes_token_id(detail)
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
            "time_secs": round(time.time() - start, 3),
        }

    # ======================================================
    # UPDATE PRICES
    # ======================================================
    if job_name == "update_prices":
        snap = fore = skipped = 0

        with closing(get_conn()) as conn:
            rows = conn.execute(
                "SELECT id, yes_token_id FROM events LIMIT ?",
                (PRICE_UPDATE_MAX,),
            ).fetchall()

            for r in rows:
                if not r["yes_token_id"]:
                    skipped += 1
                    continue

                p = clob_midpoint(r["yes_token_id"])
                if p is None:
                    continue

                conn.execute(
                    "UPDATE events SET latest_pm_p=? WHERE id=?",
                    (p, r["id"]),
                )
                snap += 1

            conn.commit()

            rows = conn.execute(
                "SELECT id, latest_pm_p FROM events WHERE latest_pm_p IS NOT NULL",
            ).fetchall()

            for r in rows:
                conn.execute(
                    "UPDATE events SET latest_machine_p=? WHERE id=?",
                    (machine_from_pm(r["latest_pm_p"]), r["id"]),
                )
                fore += 1

            conn.commit()

        return {
            "ok": True,
            "job": job_name,
            "snapshot_updated": snap,
            "forecast_updated": fore,
            "skipped_no_token": skipped,
            "time_secs": round(time.time() - start, 3),
        }

    raise HTTPException(status_code=400, detail="Unknown job")