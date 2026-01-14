import os
import time
import sqlite3
import secrets
import requests
from datetime import datetime, timezone
from contextlib import closing
from fastapi import FastAPI, HTTPException, Header, Query

from pm.ensemble import compute_machine_p

DB_PATH = os.getenv("PM_DB_PATH", "./auditor.db")
ADMIN_TOKEN = os.getenv("PM_ADMIN_TOKEN", "edge-machine-admin-2026")
GAMMA_BASE = "https://gamma-api.polymarket.com"

app = FastAPI(title="Edge Machine API", version="1.6.3")

def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with closing(get_conn()) as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            title TEXT,
            gamma_market_id TEXT UNIQUE,
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

init_db()

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/v1/events")
def list_events(limit: int = 50):
    with closing(get_conn()) as c:
        rows = c.execute(
            "SELECT id, title, gamma_market_id, latest_pm_p, latest_machine_p, created_at FROM events ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]

def auth(token: str | None):
    if not token or not secrets.compare_digest(token, ADMIN_TOKEN):
        raise HTTPException(401, "Bad admin token")

def _gamma_markets_payload_to_list(payload):
    # Gamma sometimes returns: {"markets":[...]} OR {"data":[...]} OR just [...]
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("markets") or payload.get("data") or payload.get("results") or []
    return []

@app.post("/v1/admin/jobs/run")
def run_job(
    job_name: str = Query(...),
    x_admin_token: str | None = Header(None)
):
    auth(x_admin_token)
    t0 = time.time()

    if job_name == "discover_markets":
        inserted = 0

        # pull one page of active markets (small + safe)
        resp = requests.get(
            f"{GAMMA_BASE}/markets",
            params={"active": "true", "closed": "false", "limit": 50, "offset": 0},
            timeout=15
        )
        resp.raise_for_status()

        markets = _gamma_markets_payload_to_list(resp.json())

        with closing(get_conn()) as c:
            for m in markets:
                if time.time() - t0 > 10:
                    break

                mid = m.get("id")
                title = (m.get("question") or m.get("title") or "").strip()
                if mid is None or not title:
                    continue

                mid_s = str(mid)

                c.execute("""
                    INSERT OR IGNORE INTO events
                    (id, title, gamma_market_id, created_at)
                    VALUES (?, ?, ?, ?)
                """, (
                    mid_s,
                    title,
                    mid_s,
                    datetime.now(timezone.utc).isoformat()
                ))
                inserted += 1

            c.commit()

        return {"ok": True, "job": job_name, "inserted": inserted}

    if job_name == "forecast_machine":
        updated = 0
        with closing(get_conn()) as c:
            rows = c.execute("""
                SELECT id, latest_pm_p FROM events
                WHERE latest_pm_p IS NOT NULL
                LIMIT 200
            """).fetchall()

            for r in rows:
                if time.time() - t0 > 10:
                    break
                mp = compute_machine_p(r["latest_pm_p"])
                c.execute("UPDATE events SET latest_machine_p=? WHERE id=?", (mp, r["id"]))
                updated += 1

            c.commit()

        return {"ok": True, "job": job_name, "updated": updated}

    if job_name == "resolve_markets":
        resolved = 0

        with closing(get_conn()) as c:
            rows = c.execute("""
                SELECT e.id, e.gamma_market_id, e.latest_pm_p, e.latest_machine_p
                FROM events e
                LEFT JOIN resolutions r ON r.event_id = e.id
                WHERE r.event_id IS NULL
                LIMIT 200
            """).fetchall()

            for r in rows:
                if time.time() - t0 > 10:
                    break

                try:
                    resp = requests.get(f"{GAMMA_BASE}/markets/{r['gamma_market_id']}", timeout=10)
                    if not resp.ok:
                        continue

                    m = resp.json()
                    if not m.get("closed"):
                        continue

                    outcome = (m.get("outcome") or "").strip()
                    if outcome not in ("Yes", "No"):
                        continue

                    y = 1 if outcome == "Yes" else 0
                    cp = float(r["latest_pm_p"] or 0.5)
                    mp = float(r["latest_machine_p"] or 0.5)

                    c.execute("""
                        INSERT OR REPLACE INTO resolutions
                        (event_id, resolved_at, outcome, crowd_p, machine_p, crowd_brier, machine_brier)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        r["id"],
                        datetime.now(timezone.utc).isoformat(),
                        y,
                        cp,
                        mp,
                        (cp - y) ** 2,
                        (mp - y) ** 2
                    ))
                    resolved += 1
                except Exception:
                    continue

            c.commit()

        return {"ok": True, "job": job_name, "resolved": resolved}

    raise HTTPException(400, "Unknown job")