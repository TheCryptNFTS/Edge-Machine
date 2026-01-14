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

app = FastAPI(title="Edge Machine API", version="1.6.2")

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
            latest_machine_p REAL
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
            "SELECT * FROM events LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]

def auth(token: str | None):
    if not token or not secrets.compare_digest(token, ADMIN_TOKEN):
        raise HTTPException(401, "Bad admin token")

@app.post("/v1/admin/jobs/run")
def run_job(
    job_name: str = Query(...),
    x_admin_token: str | None = Header(None)
):
    auth(x_admin_token)
    t0 = time.time()

    # =========================
    # DISCOVER MARKETS
    # =========================
    if job_name == "discover_markets":
        inserted = 0
        resp = requests.get(f"{GAMMA_BASE}/markets", timeout=10)
        if not resp.ok:
            return {"ok": False}

        markets = resp.json().get("markets", [])[:50]

        with closing(get_conn()) as c:
            for m in markets:
                try:
                    c.execute("""
                        INSERT OR IGNORE INTO events
                        (id, title, gamma_market_id)
                        VALUES (?, ?, ?)
                    """, (
                        m["id"],
                        m.get("question", "unknown"),
                        m["id"]
                    ))
                    inserted += 1
                except Exception:
                    continue
            c.commit()

        return {"ok": True, "job": job_name, "inserted": inserted}

    # =========================
    # FORECAST MACHINE
    # =========================
    if job_name == "forecast_machine":
        updated = 0
        with closing(get_conn()) as c:
            rows = c.execute("""
                SELECT id, latest_pm_p FROM events
                WHERE latest_pm_p IS NOT NULL
            """).fetchall()

            for r in rows:
                mp = compute_machine_p(r["latest_pm_p"])
                c.execute("""
                    UPDATE events SET latest_machine_p=?
                    WHERE id=?
                """, (mp, r["id"]))
                updated += 1

            c.commit()

        return {"ok": True, "job": job_name, "updated": updated}

    # =========================
    # RESOLVE MARKETS (GROUND TRUTH)
    # =========================
    if job_name == "resolve_markets":
        resolved = 0

        with closing(get_conn()) as c:
            rows = c.execute("""
                SELECT e.id, e.gamma_market_id,
                       e.latest_pm_p, e.latest_machine_p
                FROM events e
                LEFT JOIN resolutions r ON r.event_id = e.id
                WHERE r.event_id IS NULL
                LIMIT 200
            """).fetchall()

            for r in rows:
                if time.time() - t0 > 10:
                    break

                try:
                    resp = requests.get(
                        f"{GAMMA_BASE}/markets/{r['gamma_market_id']}",
                        timeout=10
                    )
                    if not resp.ok:
                        continue

                    m = resp.json()
                    if not m.get("closed"):
                        continue

                    outcome = m.get("outcome")
                    if outcome not in ("Yes", "No"):
                        continue

                    y = 1 if outcome == "Yes" else 0
                    cp = float(r["latest_pm_p"] or 0.5)
                    mp = float(r["latest_machine_p"] or 0.5)

                    c.execute("""
                        INSERT OR REPLACE INTO resolutions
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