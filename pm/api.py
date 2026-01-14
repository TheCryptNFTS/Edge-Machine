from __future__ import annotations

import os
import time
import uuid
import json
import sqlite3
import secrets
from contextlib import closing
from datetime import datetime, timezone
from typing import Optional, List, Tuple

import requests
from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from pm.ensemble import compute_machine_p

# =========================
# CONFIG
# =========================

DB_PATH = os.getenv("PM_DB_PATH", "./pm.db")
ADMIN_TOKEN = os.getenv("PM_ADMIN_TOKEN", "edge-machine-admin-2026")

GAMMA_BASE = os.getenv("PM_GAMMA_BASE", "https://gamma-api.polymarket.com").rstrip("/")
CLOB_BASE = os.getenv("PM_CLOB_BASE", "https://clob.polymarket.com").rstrip("/")

DISCOVER_LIMIT = int(os.getenv("PM_DISCOVER_LIMIT", "50"))
DISCOVER_PAGES = int(os.getenv("PM_DISCOVER_PAGES", "3"))
DISCOVER_PAGE_SIZE = int(os.getenv("PM_DISCOVER_PAGE_SIZE", "100"))
DISCOVER_DETAIL_MAX = int(os.getenv("PM_DISCOVER_DETAIL_MAX", "25"))

HYDRATE_MAX = int(os.getenv("PM_HYDRATE_MAX", "100"))
PRICE_UPDATE_MAX = int(os.getenv("PM_PRICE_UPDATE_MAX", "100"))
FORECAST_MAX = int(os.getenv("PM_FORECAST_MAX", "200"))

JOB_TIME_BUDGET_SECS = int(os.getenv("PM_JOB_TIME_BUDGET_SECS", "12"))

# =========================
# APP
# =========================

app = FastAPI(title="Edge Machine API", version="1.6.2")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# DB
# =========================

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
            market_slug TEXT UNIQUE,
            gamma_market_id TEXT,
            yes_token_id TEXT,
            no_token_id TEXT,
            volume24hr REAL,
            latest_pm_p REAL,
            latest_machine_p REAL
        )
        """)
        conn.commit()

init_db()

# =========================
# MODELS
# =========================

class EventOut(BaseModel):
    id: str
    title: str
    market_slug: Optional[str]
    gamma_market_id: Optional[str]
    yes_token_id: Optional[str]
    no_token_id: Optional[str]
    volume24hr: Optional[float]
    latest_pm_p: Optional[float]
    latest_machine_p: Optional[float]

# =========================
# AUTH
# =========================

def check_admin(x_admin_token: Optional[str]):
    if not x_admin_token or not secrets.compare_digest(x_admin_token, ADMIN_TOKEN):
        raise HTTPException(status_code=401, detail="Invalid admin token")

# =========================
# HELPERS
# =========================

def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()

def _clamp01(x: float) -> float:
    try:
        v = float(x)
    except Exception:
        return 0.5
    return max(0.0, min(1.0, v))

def _as_markets(payload) -> list[dict]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("markets") or payload.get("data") or []
    return []

def _parse_clob_ids(x) -> list[str]:
    """
    Gamma sometimes returns clobTokenIds as:
    - list: ["123","456"]
    - string csv: "123,456"
    - JSON string list: '["123","456"]'
    """
    if x is None:
        return []
    if isinstance(x, list):
        return [str(i).strip() for i in x if str(i).strip()]
    if isinstance(x, str):
        s = x.strip()
        # try JSON list
        if s.startswith("[") and s.endswith("]"):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return [str(i).strip() for i in parsed]
            except Exception:
                pass
        # fallback csv
        return [p.strip() for p in s.split(",") if p.strip()]
    return []

def gamma_markets(limit: int, offset: int) -> list[dict]:
    r = requests.get(
        f"{GAMMA_BASE}/markets",
        params={"limit": limit, "offset": offset, "active": "true"},
        timeout=20
    )
    r.raise_for_status()
    return _as_markets(r.json())

def gamma_detail(mid: str) -> Optional[dict]:
    for url in (f"{GAMMA_BASE}/markets/{mid}", f"{GAMMA_BASE}/market/{mid}"):
        try:
            r = requests.get(url, timeout=20)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
    return None

def extract_yes_no_token_ids(m: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Best case: market['tokens'] contains objects with outcome = Yes/No and tokenId/clobTokenId.
    Fallback: clobTokenIds list/str (assume first = YES, second = NO).
    """
    yes_id = None
    no_id = None

    tokens = m.get("tokens") or m.get("outcomes") or []
    if isinstance(tokens, list) and tokens:
        for t in tokens:
            outcome = (t.get("outcome") or t.get("label") or t.get("name") or "").strip().lower()
            tid = t.get("tokenId") or t.get("token_id") or t.get("id") or t.get("clobTokenId")
            if not tid:
                continue
            tid = str(tid)
            if outcome == "yes":
                yes_id = tid
            elif outcome == "no":
                no_id = tid

    if yes_id and no_id:
        return yes_id, no_id

    clob_ids = _parse_clob_ids(m.get("clobTokenIds"))
    if len(clob_ids) == 2:
        # This ordering is typical in Gamma: [YES, NO]
        return clob_ids[0], clob_ids[1]

    return yes_id, no_id

def clob_midpoint(token_id: str) -> Optional[float]:
    try:
        r = requests.get(
            f"{CLOB_BASE}/midpoint",
            params={"token_id": token_id},
            timeout=10
        )
        r.raise_for_status()
        mp = r.json().get("midpoint")
        if mp is None:
            return None
        return float(mp)
    except Exception:
        return None

# =========================
# ROUTES
# =========================

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/v1/events", response_model=List[EventOut])
def list_events(limit: int = Query(default=50, ge=1, le=500)):
    with closing(get_conn()) as conn:
        rows = conn.execute(
            "SELECT * FROM events ORDER BY COALESCE(volume24hr,0) DESC LIMIT ?",
            (limit,)
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

    # -------------------
    # DISCOVER
    # -------------------
    if job_name == "discover_markets":
        discovered = 0
        inserted = 0
        tokened = 0
        detail_calls = 0

        with closing(get_conn()) as conn:
            existing = conn.execute("SELECT market_slug FROM events").fetchall()
            seen = {r["market_slug"] for r in existing}

        offset = 0
        for _ in range(DISCOVER_PAGES):
            if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                break

            markets = gamma_markets(DISCOVER_PAGE_SIZE, offset)
            if not markets:
                break

            with closing(get_conn()) as conn:
                for m in markets:
                    if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                        break
                    if discovered >= DISCOVER_LIMIT:
                        break

                    title = (m.get("question") or m.get("title") or "").strip()
                    slug = m.get("slug")
                    mid = m.get("id") or m.get("marketId") or m.get("market_id")
                    if not title or not slug or mid is None:
                        continue
                    if slug in seen:
                        continue

                    # Strict binary filter when field exists
                    ot = (m.get("outcomeType") or "").upper()
                    if ot and ot != "BINARY":
                        continue

                    vol = float(m.get("volume24hr") or m.get("volume24hrClob") or m.get("volume") or 0.0)

                    yes_id, no_id = extract_yes_no_token_ids(m)
                    if (not yes_id or not no_id) and detail_calls < DISCOVER_DETAIL_MAX:
                        d = gamma_detail(str(mid))
                        detail_calls += 1
                        if d:
                            y2, n2 = extract_yes_no_token_ids(d)
                            yes_id = yes_id or y2
                            no_id = no_id or n2

                    conn.execute("""
                        INSERT OR IGNORE INTO events
                        (id, title, created_at, market_slug, gamma_market_id, yes_token_id, no_token_id, volume24hr, latest_pm_p, latest_machine_p)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
                    """, (
                        str(uuid.uuid4()),
                        title,
                        _utcnow(),
                        slug,
                        str(mid),
                        yes_id,
                        no_id,
                        vol,
                    ))
                    inserted += 1
                    discovered += 1
                    seen.add(slug)
                    if yes_id:
                        tokened += 1

                conn.commit()

            offset += DISCOVER_PAGE_SIZE

        return {
            "ok": True,
            "job": job_name,
            "discovered": discovered,
            "inserted": inserted,
            "tokened": tokened,
            "detail_calls": detail_calls,
            "time_secs": round(time.time() - t0, 3),
        }

    # -------------------
    # HYDRATE
    # -------------------
    if job_name == "hydrate_tokens":
        attempted = 0
        hydrated = 0

        with closing(get_conn()) as conn:
            rows = conn.execute("""
                SELECT id, gamma_market_id
                FROM events
                WHERE yes_token_id IS NULL OR no_token_id IS NULL
                ORDER BY COALESCE(volume24hr,0) DESC
                LIMIT ?
            """, (HYDRATE_MAX,)).fetchall()

            for r in rows:
                if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                    break

                attempted += 1
                d = gamma_detail(str(r["gamma_market_id"]))
                if not d:
                    continue

                yes_id, no_id = extract_yes_no_token_ids(d)
                if yes_id and no_id:
                    conn.execute(
                        "UPDATE events SET yes_token_id=?, no_token_id=? WHERE id=?",
                        (yes_id, no_id, r["id"])
                    )
                    hydrated += 1

            conn.commit()

        return {"ok": True, "job": job_name, "attempted": attempted, "hydrated": hydrated}

    # -------------------
    # UPDATE PRICES
    # -------------------
    if job_name == "update_prices":
        updated = 0
        skipped_no_token = 0

        with closing(get_conn()) as conn:
            rows = conn.execute("""
                SELECT id, yes_token_id
                FROM events
                ORDER BY COALESCE(volume24hr,0) DESC
                LIMIT ?
            """, (PRICE_UPDATE_MAX,)).fetchall()

            for r in rows:
                if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                    break

                if not r["yes_token_id"]:
                    skipped_no_token += 1
                    continue

                p = clob_midpoint(r["yes_token_id"])
                if p is None:
                    continue

                conn.execute(
                    "UPDATE events SET latest_pm_p=? WHERE id=?",
                    (_clamp01(p), r["id"])
                )
                updated += 1

            conn.commit()

        return {"ok": True, "job": job_name, "updated": updated, "skipped_no_token": skipped_no_token}

    # -------------------
    # FORECAST MACHINE
    # -------------------
    if job_name == "forecast_machine":
        updated = 0

        with closing(get_conn()) as conn:
            rows = conn.execute("""
                SELECT id, latest_pm_p, volume24hr, title
                FROM events
                WHERE latest_pm_p IS NOT NULL
                ORDER BY COALESCE(volume24hr,0) DESC
                LIMIT ?
            """, (FORECAST_MAX,)).fetchall()

            for r in rows:
                if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                    break

                p_pm = float(r["latest_pm_p"])
                vol = float(r["volume24hr"] or 0.0)
                title = r["title"] or ""
                p_m = compute_machine_p(p_pm, vol, title)

                conn.execute(
                    "UPDATE events SET latest_machine_p=? WHERE id=?",
                    (p_m, r["id"])
                )
                updated += 1

            conn.commit()

        return {"ok": True, "job": job_name, "updated": updated}

    raise HTTPException(400, "Unknown job")