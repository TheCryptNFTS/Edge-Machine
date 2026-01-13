from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timezone
import os
import sqlite3
import uuid

# =========================
# CONFIG
# =========================

DATABASE_URL = os.getenv("PM_DATABASE_URL", "sqlite:///./pm.db")

# ✅ Accept either env var name (fixes your mismatch)
ADMIN_TOKEN = (
    os.getenv("ADMIN_TOKEN")
    or os.getenv("PM_ADMIN_TOKEN")
    or "change-me"
)

DB_PATH = DATABASE_URL.replace("sqlite:///", "")

# =========================
# APP
# =========================

app = FastAPI(title="Edge Machine API", version="0.2.0")

# ✅ CORS for Vercel frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # lock down later
    allow_credentials=False,  # safer with allow_origins="*"
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# DB HELPERS
# =========================

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            created_at TEXT NOT NULL,
            resolve_at_utc TEXT,
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
    latest_pm_p: Optional[float] = None
    latest_machine_p: Optional[float] = None

class EventCreate(BaseModel):
    title: str
    resolve_at_utc: Optional[str] = None  # ISO string, optional

# =========================
# AUTH
# =========================

def check_admin(x_admin_token: Optional[str]):
    # Swagger sends "x-admin-token" and FastAPI passes it into x_admin_token
    if not x_admin_token or x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid admin token")

# =========================
# ROUTES
# =========================

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/v1/events", response_model=List[EventOut])
def list_events(limit: int = 50):
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM events ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()

    return [
        EventOut(
            id=row["id"],
            title=row["title"],
            latest_pm_p=row["latest_pm_p"],
            latest_machine_p=row["latest_machine_p"],
        )
        for row in rows
    ]

@app.post("/v1/admin/events")
def create_event(payload: EventCreate, x_admin_token: Optional[str] = Header(None)):
    check_admin(x_admin_token)

    event_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    with db() as conn:
        conn.execute(
            """
            INSERT INTO events (id, title, created_at, resolve_at_utc, latest_pm_p, latest_machine_p)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                payload.title.strip(),
                now,
                payload.resolve_at_utc,
                None,
                None,
            ),
        )
        conn.commit()

    return {"ok": True, "id": event_id}

@app.post("/v1/admin/events/{event_id}/update")
def update_event_probs(
    event_id: str,
    pm_p: Optional[float] = None,
    machine_p: Optional[float] = None,
    x_admin_token: Optional[str] = Header(None),
):
    check_admin(x_admin_token)

    with db() as conn:
        row = conn.execute("SELECT id FROM events WHERE id = ?", (event_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Event not found")

        conn.execute(
            """
            UPDATE events
            SET latest_pm_p = COALESCE(?, latest_pm_p),
                latest_machine_p = COALESCE(?, latest_machine_p)
            WHERE id = ?
            """,
            (pm_p, machine_p, event_id),
        )
        conn.commit()

    return {"ok": True}

@app.post("/v1/admin/jobs/run")
def run_job(job_name: str, x_admin_token: Optional[str] = Header(None)):
    # Stub for cron jobs right now; later you’ll wire real snapshot/forecast/resolve
    check_admin(x_admin_token)

    if job_name not in {"snapshot_pm", "forecast_machine", "resolve"}:
        raise HTTPException(status_code=400, detail="Unknown job")

    return {"ok": True, "job": job_name}