from __future__ import annotations

import os
import time
import uuid
import json
import sqlite3
from datetime import datetime, timezone
from typing import Optional, List
from contextlib import closing

import requests
from fastapi import FastAPI, Header, HTTPException, Query
from pydantic import BaseModel

from pm.ensemble import compute_machine_p

app = FastAPI(title="Probability Auditor", version="1.0.0")

# --------------------------
# DB path helper
# --------------------------
def _db_path() -> str:
    p = os.getenv("PM_DB_PATH")
    if p:
        return p
    url = os.getenv("PM_DATABASE_URL", "sqlite:///./auditor.db")
    if url.startswith("sqlite:////"):
        return url.replace("sqlite:////", "/")
    if url.startswith("sqlite:///"):
        return url.replace("sqlite:///", "")
    return "./auditor.db"

DB_PATH = _db_path()
ADMIN_TOKEN = os.getenv("PM_ADMIN_TOKEN", "change-me")

GAMMA = os.getenv("PM_GAMMA_BASE", "https://gamma-api.polymarket.com").rstrip("/")
CLOB = os.getenv("PM_CLOB_BASE", "https://clob.polymarket.com").rstrip("/")

DISCOVER_LIMIT = int(os.getenv("PM_DISCOVER_LIMIT", "50"))
DISCOVER_PAGES = int(os.getenv("PM_DISCOVER_PAGES", "3"))
DISCOVER_PAGE_SIZE = int(os.getenv("PM_DISCOVER_PAGE_SIZE", "100"))
DISCOVER_DETAIL_MAX = int(os.getenv("PM_DISCOVER_DETAIL_MAX", "15"))

HYDRATE_MAX = int(os.getenv("PM_HYDRATE_MAX", "50"))
PRICE_UPDATE_MAX = int(os.getenv("PM_PRICE_UPDATE_MAX", "50"))
FORECAST_MAX = int(os.getenv("PM_FORECAST_MAX", "200"))

JOB_TIME_BUDGET_SECS = int(os.getenv("PM_JOB_TIME_BUDGET_SECS", "12"))

# --------------------------
# DB init
# --------------------------
def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c

def init():
    with closing(db()) as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS events (
          id TEXT PRIMARY KEY,
          title TEXT,
          market_slug TEXT UNIQUE,
          gamma_market_id TEXT,
          yes_token_id TEXT,
          no_token_id TEXT,
          volume24hr REAL,
          latest_pm_p REAL,
          latest_machine_p REAL,
          created_at TEXT
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS resolutions (
          event_id TEXT PRIMARY KEY,
          resolved_at TEXT,
          outcome INTEGER,
          crowd_p REAL,
          machine_p REAL,
          crowd_brier REAL,
          machine_brier REAL
        )
        """)
        c.commit()

init()

# --------------------------
# Models
# --------------------------
class Event(BaseModel):
    id: str
    title: str
    market_slug: Optional[str] = None
    gamma_market_id: Optional[str] = None
    volume24hr: Optional[float] = None
    latest_pm_p: Optional[float] = None
    latest_machine_p: Optional[float] = None

class ResolveReq(BaseModel):
    outcome: int  # 1 or 0

# --------------------------
# Auth
# --------------------------
def auth(x_admin_token: Optional[str]):
    if x_admin_token != ADMIN_TOKEN:
        raise HTTPException(401, "Bad token")

# --------------------------
# Helpers
# --------------------------
def clamp01(x: float) -> float:
    try:
        v = float(x)
    except Exception:
        return 0.5
    if v < 0.0: return 0.0
    if v > 1.0: return 1.0
    return v

def _json_markets(payload) -> list[dict]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("markets") or payload.get("data") or []
    return []

def extract_yes_no_from_tokens(m: dict) -> tuple[Optional[str], Optional[str]]:
    tokens = m.get("tokens") or m.get("outcomes") or []
    yes_id = None
    no_id = None

    if isinstance(tokens, list):
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
    return yes_id, no_id

def gamma_list_markets(limit: int, offset: int) -> list[dict]:
    r = requests.get(
        f"{GAMMA}/markets",
        params={"active": "true", "closed": "false", "limit": limit, "offset": offset},
        timeout=20,
    )
    r.raise_for_status()
    return _json_markets(r.json())

def gamma_detail_by_id(mid: str) -> Optional[dict]:
    for url in (f"{GAMMA}/markets/{mid}", f"{GAMMA}/market/{mid}"):
        try:
            rr = requests.get(url, timeout=20)
            if rr.status_code == 200:
                return rr.json()
        except Exception:
            pass
    return None

def clob_midpoint(token_id: str) -> Optional[float]:
    try:
        rr = requests.get(f"{CLOB}/midpoint", params={"token_id": token_id}, timeout=10)
        rr.raise_for_status()
        mp = rr.json().get("midpoint")
        return float(mp) if mp is not None else None
    except Exception:
        return None

# --------------------------
# Routes
# --------------------------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/v1/events", response_model=List[Event])
def events(limit: int = Query(default=50, ge=1, le=500)):
    with closing(db()) as c:
        rows = c.execute("""
          SELECT id, title, market_slug, gamma_market_id, volume24hr, latest_pm_p, latest_machine_p
          FROM events
          ORDER BY COALESCE(volume24hr,0) DESC
          LIMIT ?
        """, (limit,)).fetchall()
    return [Event(**dict(r)) for r in rows]

@app.get("/v1/scoreboard/brier")
def scoreboard_brier():
    with closing(db()) as c:
        row = c.execute("""
          SELECT COUNT(*) AS n,
                 AVG(crowd_brier) AS crowd_brier_mean,
                 AVG(machine_brier) AS machine_brier_mean
          FROM resolutions
        """).fetchone()
        if not row or row["n"] == 0:
            return {"ok": True, "resolved_events": 0, "note": "No resolved events yet."}

        crowd = float(row["crowd_brier_mean"])
        mach = float(row["machine_brier_mean"])
        return {
            "ok": True,
            "resolved_events": int(row["n"]),
            "crowd_brier_mean": crowd,
            "machine_brier_mean": mach,
            "improvement_percent": round((crowd - mach) / crowd * 100.0, 2),
        }

@app.post("/v1/admin/resolve/{event_id}")
def resolve_event(event_id: str, payload: ResolveReq, x_admin_token: Optional[str] = Header(None)):
    auth(x_admin_token)
    y = 1 if int(payload.outcome) == 1 else 0

    with closing(db()) as c:
        e = c.execute("SELECT latest_pm_p, latest_machine_p FROM events WHERE id=?", (event_id,)).fetchone()
        if not e:
            raise HTTPException(404, "Event not found")
        if e["latest_pm_p"] is None or e["latest_machine_p"] is None:
            raise HTTPException(400, "Need latest_pm_p and latest_machine_p before resolving")

        crowd_p = float(e["latest_pm_p"])
        mach_p = float(e["latest_machine_p"])
        crowd_b = (crowd_p - y) ** 2
        mach_b = (mach_p - y) ** 2

        c.execute("""
          INSERT OR REPLACE INTO resolutions
          (event_id, resolved_at, outcome, crowd_p, machine_p, crowd_brier, machine_brier)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id,
            datetime.now(timezone.utc).isoformat(),
            y, crowd_p, mach_p, crowd_b, mach_b
        ))
        c.commit()

    return {"ok": True, "event_id": event_id, "outcome": y}

@app.post("/v1/admin/jobs/run")
def run(job_name: str = Query(...), x_admin_token: Optional[str] = Header(None)):
    auth(x_admin_token)
    t0 = time.time()

    # ---------------- discover_markets ----------------
    if job_name == "discover_markets":
        added = 0
        tokened = 0
        detail_calls = 0

        with closing(db()) as c:
            existing = c.execute("SELECT market_slug FROM events").fetchall()
            seen = {r["market_slug"] for r in existing if r["market_slug"]}

        offset = 0
        for _ in range(DISCOVER_PAGES):
            if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                break

            markets = gamma_list_markets(DISCOVER_PAGE_SIZE, offset)
            if not markets:
                break

            with closing(db()) as c:
                for m in markets:
                    if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                        break
                    if added >= DISCOVER_LIMIT:
                        break

                    title = (m.get("question") or m.get("title") or "").strip()
                    slug = m.get("slug")
                    mid = m.get("id") or m.get("marketId") or m.get("market_id")
                    if not title or not slug or mid is None:
                        continue

                    ot = (m.get("outcomeType") or m.get("outcome_type") or "").upper()
                    if ot and ot != "BINARY":
                        continue

                    if slug in seen:
                        continue

                    vol = float(m.get("volume24hr") or m.get("volume24hrClob") or m.get("volume") or 0.0)

                    yes_id, no_id = extract_yes_no_from_tokens(m)

                    if (yes_id is None or no_id is None) and detail_calls < DISCOVER_DETAIL_MAX:
                        d = gamma_detail_by_id(str(mid))
                        detail_calls += 1
                        if d:
                            y2, n2 = extract_yes_no_from_tokens(d)
                            yes_id = yes_id or y2
                            no_id = no_id or n2

                    c.execute("""
                      INSERT OR IGNORE INTO events
                      (id, title, market_slug, gamma_market_id, yes_token_id, no_token_id, volume24hr, latest_pm_p, latest_machine_p, created_at)
                      VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)
                    """, (
                        str(uuid.uuid4()),
                        title,
                        slug,
                        str(mid),
                        yes_id,
                        no_id,
                        vol,
                        datetime.now(timezone.utc).isoformat(),
                    ))
                    seen.add(slug)
                    added += 1
                    if yes_id:
                        tokened += 1
                c.commit()

            offset += DISCOVER_PAGE_SIZE

        return {"ok": True, "job": job_name, "added": added, "tokened": tokened, "detail_calls": detail_calls}

    # ---------------- hydrate_tokens ----------------
    if job_name == "hydrate_tokens":
        attempted = 0
        hydrated = 0

        with closing(db()) as c:
            rows = c.execute("""
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
                d = gamma_detail_by_id(str(r["gamma_market_id"]))
                if not d:
                    continue
                yes_id, no_id = extract_yes_no_from_tokens(d)
                if yes_id and no_id:
                    c.execute("UPDATE events SET yes_token_id=?, no_token_id=? WHERE id=?", (yes_id, no_id, r["id"]))
                    hydrated += 1
            c.commit()

        return {"ok": True, "job": job_name, "attempted": attempted, "hydrated": hydrated}

    # ---------------- update_prices ----------------
    if job_name == "update_prices":
        updated = 0
        skipped = 0

        with closing(db()) as c:
            rows = c.execute("""
              SELECT id, yes_token_id
              FROM events
              ORDER BY COALESCE(volume24hr,0) DESC
              LIMIT ?
            """, (PRICE_UPDATE_MAX,)).fetchall()

            for r in rows:
                if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                    break
                if not r["yes_token_id"]:
                    skipped += 1
                    continue
                p = clob_midpoint(r["yes_token_id"])
                if p is None:
                    continue
                c.execute("UPDATE events SET latest_pm_p=? WHERE id=?", (clamp01(p), r["id"]))
                updated += 1
            c.commit()

        return {"ok": True, "job": job_name, "updated": updated, "skipped_no_token": skipped}

    # ---------------- forecast_machine ----------------
    if job_name == "forecast_machine":
        updated = 0
        skipped = 0

        with closing(db()) as c:
            rows = c.execute("""
              SELECT id, latest_pm_p, volume24hr
              FROM events
              WHERE latest_pm_p IS NOT NULL
              ORDER BY COALESCE(volume24hr,0) DESC
              LIMIT ?
            """, (FORECAST_MAX,)).fetchall()

            for r in rows:
                if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                    break
                if r["latest_pm_p"] is None:
                    skipped += 1
                    continue
                mp = compute_machine_p(float(r["latest_pm_p"]), float(r["volume24hr"] or 0.0))
                c.execute("UPDATE events SET latest_machine_p=? WHERE id=?", (mp, r["id"]))
                updated += 1
            c.commit()

        return {"ok": True, "job": job_name, "updated": updated, "skipped_no_pm": skipped}

    # ---------------- resolve_markets ----------------
    if job_name == "resolve_markets":
        resolved = 0
        scanned = 0

        with closing(db()) as c:
            rows = c.execute("""
                SELECT e.id, e.gamma_market_id
                FROM events e
                LEFT JOIN resolutions r ON r.event_id = e.id
                WHERE r.event_id IS NULL
                  AND e.gamma_market_id IS NOT NULL
                LIMIT 200
            """).fetchall()

            for r in rows:
                if time.time() - t0 > 10:
                    break

                scanned += 1

                try:
                    resp = requests.get(f"{GAMMA}/markets/{r['gamma_market_id']}", timeout=15)
                    if not resp.ok:
                        continue
                    m = resp.json()
                except Exception:
                    continue

                closed = m.get("closed") or m.get("isClosed") or m.get("resolved") or m.get("isResolved")
                outcome = (m.get("outcome") or "").strip()

                if not closed:
                    continue
                if outcome not in ("Yes", "No"):
                    continue

                y = 1 if outcome == "Yes" else 0

                e = c.execute("SELECT latest_pm_p, latest_machine_p FROM events WHERE id=?", (r["id"],)).fetchone()
                if not e or e["latest_pm_p"] is None or e["latest_machine_p"] is None:
                    continue

                crowd_p = float(e["latest_pm_p"])
                mach_p = float(e["latest_machine_p"])
                crowd_b = (crowd_p - y) ** 2
                mach_b = (mach_p - y) ** 2

                c.execute("""
                    INSERT OR REPLACE INTO resolutions
                    (event_id, resolved_at, outcome, crowd_p, machine_p, crowd_brier, machine_brier)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    r["id"],
                    datetime.now(timezone.utc).isoformat(),
                    y, crowd_p, mach_p, crowd_b, mach_b
                ))

                resolved += 1

            c.commit()

        return {"ok": True, "job": "resolve_markets", "scanned": scanned, "resolved": resolved}

    raise HTTPException(400, "Unknown job")