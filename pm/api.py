from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import sqlite3
import requests
import os
from datetime import datetime, timezone
from typing import Optional, List

DB_PATH = os.getenv("DB_PATH", "edge_machine.db")
POLYMARKET_GAMMA = "https://gamma-api.polymarket.com"

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "edge-machine-admin-2026")

app = FastAPI(
    title="Edge Machine API",
    version="1.6.4"
)

# -----------------------
# DB helpers
# -----------------------

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def now_utc():
    return datetime.now(timezone.utc).isoformat()


# -----------------------
# Models
# -----------------------

class EventOut(BaseModel):
    id: str
    title: str
    gamma_market_id: str
    latest_pm_p: Optional[float]
    latest_machine_p: Optional[float]
    created_at: str


# -----------------------
# Health
# -----------------------

@app.get("/health")
def health():
    return {"ok": True, "time": now_utc()}


# -----------------------
# Events
# -----------------------

@app.get("/v1/events", response_model=List[EventOut])
def list_events(limit: int = 50):
    db = get_db()
    rows = db.execute(
        """
        SELECT id, title, gamma_market_id,
               latest_pm_p, latest_machine_p, created_at
        FROM events
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


# -----------------------
# Admin job runner
# -----------------------

@app.post("/v1/admin/jobs/run")
def run_job(
    job_name: str,
    x_admin_token: Optional[str] = Header(default=None),
):
    if x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    if job_name == "discover_markets":
        return discover_markets()
    if job_name == "hydrate_tokens":
        return hydrate_tokens()
    if job_name == "update_prices":
        return update_prices()
    if job_name == "forecast_machine":
        return forecast_machine()

    raise HTTPException(status_code=400, detail="Unknown job")


# -----------------------
# Jobs
# -----------------------

def discover_markets(limit: int = 50):
    r = requests.get(f"{POLYMARKET_GAMMA}/markets?limit={limit}")
    r.raise_for_status()
    markets = r.json()

    db = get_db()
    inserted = 0

    for m in markets:
        db.execute(
            """
            INSERT OR IGNORE INTO events
            (id, title, gamma_market_id, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                str(m["id"]),
                m["question"],
                str(m["id"]),
                now_utc(),
            ),
        )
        inserted += 1

    db.commit()
    return {"ok": True, "job": "discover_markets", "inserted": inserted}


def hydrate_tokens():
    db = get_db()
    rows = db.execute(
        "SELECT id, gamma_market_id FROM events WHERE yes_token_id IS NULL"
    ).fetchall()

    attempted = 0
    hydrated = 0

    for r in rows:
        attempted += 1
        market_id = r["gamma_market_id"]

        resp = requests.get(f"{POLYMARKET_GAMMA}/markets/{market_id}")
        if resp.status_code != 200:
            continue

        data = resp.json()
        yes_token = next(
            (t["id"] for t in data.get("tokens", []) if t["outcome"] == "Yes"),
            None,
        )

        if yes_token:
            db.execute(
                "UPDATE events SET yes_token_id=? WHERE id=?",
                (yes_token, r["id"]),
            )
            hydrated += 1

    db.commit()
    return {
        "ok": True,
        "job": "hydrate_tokens",
        "attempted": attempted,
        "hydrated": hydrated,
    }


def update_prices():
    db = get_db()
    rows = db.execute(
        "SELECT id, yes_token_id FROM events WHERE yes_token_id IS NOT NULL"
    ).fetchall()

    updated = 0

    for r in rows:
        token_id = r["yes_token_id"]

        resp = requests.get(f"{POLYMARKET_GAMMA}/token/{token_id}")
        if resp.status_code != 200:
            continue

        data = resp.json()
        price = data.get("price")

        if price is not None:
            db.execute(
                "UPDATE events SET latest_pm_p=? WHERE id=?",
                (float(price), r["id"]),
            )
            updated += 1

    db.commit()
    return {"ok": True, "job": "update_prices", "updated": updated}


def forecast_machine():
    """
    Very simple baseline:
    machine_p = crowd_p (for now)
    This exists so the pipeline is complete.
    You can swap this later with real models.
    """
    db = get_db()
    rows = db.execute(
        "SELECT id, latest_pm_p FROM events WHERE latest_pm_p IS NOT NULL"
    ).fetchall()

    updated = 0

    for r in rows:
        db.execute(
            "UPDATE events SET latest_machine_p=? WHERE id=?",
            (r["latest_pm_p"], r["id"]),
        )
        updated += 1

    db.commit()
    return {"ok": True, "job": "forecast_machine", "updated": updated}