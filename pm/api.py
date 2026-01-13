from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timezone
import os
import sqlite3
import uuid
import requests
import time

# =========================
# CONFIG
# =========================

DATABASE_URL = os.getenv("PM_DATABASE_URL", "sqlite:///./pm.db")
DB_PATH = DATABASE_URL.replace("sqlite:///", "")

ADMIN_TOKEN = (
    os.getenv("ADMIN_TOKEN")
    or os.getenv("PM_ADMIN_TOKEN")
    or "change-me"
)

GAMMA_BASE = os.getenv("PM_GAMMA_BASE", "https://gamma-api.polymarket.com").rstrip("/")
CLOB_BASE = os.getenv("PM_CLOB_BASE", "https://clob.polymarket.com").rstrip("/")

# Auto-discovery scope (edit in Railway variables later)
DISCOVER_LIMIT = int(os.getenv("PM_DISCOVER_LIMIT", "50"))
DISCOVER_KEYWORDS = os.getenv(
    "PM_DISCOVER_KEYWORDS",
    "bitcoin,btc,ethereum,eth,sol,solana,crypto,election,trump,fed,inflation"
)

# =========================
# APP
# =========================

app = FastAPI(title="Edge Machine API", version="0.4.0")

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

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _has_column(conn, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == col for r in rows)

def init_db():
    with db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            created_at TEXT NOT NULL,
            resolve_at_utc TEXT,
            slug TEXT,
            gamma_market_id TEXT,
            yes_token_id TEXT,
            latest_pm_p REAL,
            latest_machine_p REAL
        )
        """)
        # migrations
        for col, ddl in [
            ("slug", "ALTER TABLE events ADD COLUMN slug TEXT"),
            ("gamma_market_id", "ALTER TABLE events ADD COLUMN gamma_market_id TEXT"),
            ("yes_token_id", "ALTER TABLE events ADD COLUMN yes_token_id TEXT"),
            ("latest_pm_p", "ALTER TABLE events ADD COLUMN latest_pm_p REAL"),
            ("latest_machine_p", "ALTER TABLE events ADD COLUMN latest_machine_p REAL"),
        ]:
            if not _has_column(conn, "events", col):
                conn.execute(ddl)
        conn.commit()

init_db()

# =========================
# AUTH
# =========================

def check_admin(x_admin_token: Optional[str]):
    if not x_admin_token or x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid admin token")

# =========================
# MODELS
# =========================

class EventOut(BaseModel):
    id: str
    title: str
    slug: Optional[str] = None
    gamma_market_id: Optional[str] = None
    yes_token_id: Optional[str] = None
    latest_pm_p: Optional[float] = None
    latest_machine_p: Optional[float] = None

# =========================
# HELPERS
# =========================

def clamp01(x: float) -> float:
    try:
        v = float(x)
    except Exception:
        return 0.5
    if v < 0.0: return 0.0
    if v > 1.0: return 1.0
    return v

def clob_midpoint(token_id: str) -> Optional[float]:
    endpoints = [
        (f"{CLOB_BASE}/midpoint", {"token_id": token_id}),
        (f"{CLOB_BASE}/price", {"token_id": token_id}),
    ]
    last_err = None
    for url, params in endpoints:
        for attempt in range(3):
            try:
                r = requests.get(url, params=params, timeout=8)
                r.raise_for_status()
                data = r.json()
                mp = data.get("midpoint") or data.get("price")
                if mp is None and isinstance(data.get("data"), dict):
                    mp = data["data"].get("midpoint") or data["data"].get("price")
                if mp is None:
                    return None
                return float(mp)
            except Exception as e:
                last_err = e
                time.sleep(0.25 * (attempt + 1))
    return None

def gamma_get_markets(limit: int = 100, offset: int = 0, active: bool = True) -> list[dict]:
    params = {"limit": limit, "offset": offset}
    if active:
        params["active"] = "true"
    r = requests.get(f"{GAMMA_BASE}/markets", params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        return data
    return data.get("markets") or data.get("data") or []

def extract_yes_token_id(market: dict) -> Optional[str]:
    # Common shapes: tokens/outcomes arrays
    tokens = market.get("tokens") or market.get("outcomes") or []
    if isinstance(tokens, list):
        for t in tokens:
            outcome = (t.get("outcome") or t.get("label") or t.get("name") or "").lower()
            if outcome == "yes" or " yes" in outcome or outcome.startswith("yes"):
                for k in ("token_id", "tokenId", "tokenID", "id", "clobTokenId"):
                    if t.get(k):
                        return str(t.get(k))
    # Fallback: clob token ids list (binary markets often yes first)
    clob_ids = market.get("clobTokenIds") or market.get("clob_token_ids")
    if isinstance(clob_ids, list) and clob_ids:
        return str(clob_ids[0])
    # Root keys fallback
    for k in ("yesTokenId", "yes_token_id", "yesToken", "yes_token"):
        if market.get(k):
            return str(market.get(k))
    return None

def machine_from_pm_v01(p_pm: float) -> float:
    # Minimal conservative machine for auto mode (we can swap in your full v0.2 later)
    p = 0.9 * p_pm + 0.1 * 0.5
    if p_pm >= 0.94:
        p = min(p, 0.995)
    if p_pm <= 0.06:
        p = max(p, 0.005)
    return clamp01(p)

def upsert_event_from_market(m: dict):
    title = (m.get("question") or m.get("title") or "").strip()
    if not title:
        return
    slug = m.get("slug")
    mid = m.get("id") or m.get("marketId") or m.get("market_id")
    yes_tid = extract_yes_token_id(m)
    if not yes_tid:
        return

    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        # try find by slug first, then by yes_token_id
        row = None
        if slug:
            row = conn.execute("SELECT id FROM events WHERE slug = ?", (slug,)).fetchone()
        if not row:
            row = conn.execute("SELECT id FROM events WHERE yes_token_id = ?", (yes_tid,)).fetchone()

        if row:
            conn.execute(
                """
                UPDATE events
                SET title = ?, gamma_market_id = ?, slug = ?, yes_token_id = ?
                WHERE id = ?
                """,
                (title, str(mid) if mid is not None else None, slug, yes_tid, row["id"])
            )
        else:
            eid = str(uuid.uuid4())
            conn.execute(
                """
                INSERT INTO events (id, title, created_at, resolve_at_utc, slug, gamma_market_id, yes_token_id, latest_pm_p, latest_machine_p)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (eid, title, now, None, slug, str(mid) if mid is not None else None, yes_tid, None, None)
            )
        conn.commit()

# =========================
# ROUTES
# =========================

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/v1/events", response_model=List[EventOut])
def list_events(limit: int = 50):
    with db() as conn:
        rows = conn.execute("SELECT * FROM events ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
    return [
        EventOut(
            id=r["id"],
            title=r["title"],
            slug=r["slug"],
            gamma_market_id=r["gamma_market_id"],
            yes_token_id=r["yes_token_id"],
            latest_pm_p=r["latest_pm_p"],
            latest_machine_p=r["latest_machine_p"],
        )
        for r in rows
    ]

@app.post("/v1/admin/jobs/run")
def run_job(job_name: str, x_admin_token: Optional[str] = Header(None)):
    check_admin(x_admin_token)

    if job_name not in {"discover_markets", "snapshot_pm", "forecast_machine"}:
        raise HTTPException(status_code=400, detail="Unknown job")

    if job_name == "discover_markets":
        keywords = [k.strip().lower() for k in DISCOVER_KEYWORDS.split(",") if k.strip()]
        discovered = 0
        offset = 0

        # Pull a few pages and filter (cheap + safe)
        for _ in range(5):
            markets = gamma_get_markets(limit=100, offset=offset, active=True)
            if not markets:
                break

            # filter by keywords and sort by volume
            filtered = []
            for m in markets:
                title = (m.get("question") or m.get("title") or "").lower()
                if not title:
                    continue
                if any(k in title for k in keywords):
                    vol = 0.0
                    try:
                        vol = float(m.get("volume") or m.get("volume24h") or m.get("volume_24hr") or 0)
                    except Exception:
                        vol = 0.0
                    filtered.append((vol, m))

            filtered.sort(key=lambda x: x[0], reverse=True)
            for _, m in filtered[:DISCOVER_LIMIT]:
                upsert_event_from_market(m)
                discovered += 1

            offset += 100

        return {"ok": True, "job": job_name, "discovered": discovered}

    if job_name == "snapshot_pm":
        updated = 0
        skipped = 0
        with db() as conn:
            rows = conn.execute("SELECT id, yes_token_id FROM events ORDER BY created_at DESC").fetchall()
            for r in rows:
                if not r["yes_token_id"]:
                    skipped += 1
                    continue
                p = clob_midpoint(r["yes_token_id"])
                if p is None:
                    continue
                p = clamp01(p)
                conn.execute("UPDATE events SET latest_pm_p = ? WHERE id = ?", (p, r["id"]))
                updated += 1
            conn.commit()
        return {"ok": True, "job": job_name, "updated": updated, "skipped_no_token": skipped}

    if job_name == "forecast_machine":
        updated = 0
        skipped = 0
        with db() as conn:
            rows = conn.execute("SELECT id, latest_pm_p FROM events ORDER BY created_at DESC").fetchall()
            for r in rows:
                if r["latest_pm_p"] is None:
                    skipped += 1
                    continue
                p_m = machine_from_pm_v01(float(r["latest_pm_p"]))
                conn.execute("UPDATE events SET latest_machine_p = ? WHERE id = ?", (p_m, r["id"]))
                updated += 1
            conn.commit()
        return {"ok": True, "job": job_name, "updated": updated, "skipped_no_pm": skipped}

    raise HTTPException(status_code=400, detail="Unhandled job")