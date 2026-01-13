from __future__ import annotations

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

# Admin token accepts either env var name
ADMIN_TOKEN = (
    os.getenv("ADMIN_TOKEN")
    or os.getenv("PM_ADMIN_TOKEN")
    or "change-me"
)

# Polymarket endpoints
GAMMA_BASE = os.getenv("PM_GAMMA_BASE", "https://gamma-api.polymarket.com").rstrip("/")
CLOB_BASE = os.getenv("PM_CLOB_BASE", "https://clob.polymarket.com").rstrip("/")

# Discovery tuning
DISCOVER_LIMIT = int(os.getenv("PM_DISCOVER_LIMIT", "50"))
DISCOVER_PAGES = int(os.getenv("PM_DISCOVER_PAGES", "5"))  # each page ~100 markets
DISCOVER_KEYWORDS = os.getenv(
    "PM_DISCOVER_KEYWORDS",
    "bitcoin,btc,ethereum,eth,sol,solana,crypto,memecoin,doge,ai,trump,election,fed,inflation,rate"
)

# =========================
# APP
# =========================

app = FastAPI(title="Edge Machine API", version="0.7.0")

# Allow Vercel frontend to fetch freely (lock down later)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
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
            slug TEXT,
            gamma_market_id TEXT,
            yes_token_id TEXT,
            latest_pm_p REAL,
            latest_machine_p REAL
        )
        """)
        # migrations (safe)
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

class EventCreate(BaseModel):
    title: str

# =========================
# AUTH
# =========================

def check_admin(x_admin_token: Optional[str]):
    if not x_admin_token or x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid admin token")

# =========================
# UTILS
# =========================

def clamp01(x: float) -> float:
    try:
        v = float(x)
    except Exception:
        return 0.5
    if v < 0.0: return 0.0
    if v > 1.0: return 1.0
    return v

def fnum(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def machine_from_pm(p_pm: float) -> float:
    """
    Minimal machine until you plug full v0.2 ensemble back in.
    Stable + safe:
    - mostly rides PM
    - tiny shrink to 0.5
    - mild cap/floor at extremes
    """
    p = 0.90 * p_pm + 0.10 * 0.5
    if p_pm >= 0.94:
        p = min(p, 0.995)
    if p_pm <= 0.06:
        p = max(p, 0.005)
    return clamp01(p)

# =========================
# POLYMARKET: CLOB PRICE
# =========================

def clob_midpoint(token_id: str) -> Optional[float]:
    endpoints = [
        (f"{CLOB_BASE}/midpoint", {"token_id": token_id}),
        (f"{CLOB_BASE}/price", {"token_id": token_id}),
    ]
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
            except Exception:
                time.sleep(0.25 * (attempt + 1))
    return None

# =========================
# POLYMARKET: GAMMA MARKETS
# =========================

def gamma_get_markets(limit: int = 100, offset: int = 0) -> list[dict]:
    params = {"limit": limit, "offset": offset, "active": "true"}
    r = requests.get(f"{GAMMA_BASE}/markets", params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        return data
    return data.get("markets") or data.get("data") or []

def extract_yes_token_id(m: dict) -> Optional[str]:
    """
    Best-effort YES token extraction:
    - binary markets usually have tokens/outcomes where label==YES
    - some provide clobTokenIds (yes first)
    """
    tokens = m.get("tokens") or m.get("outcomes") or []
    if isinstance(tokens, list):
        for t in tokens:
            label = (t.get("outcome") or t.get("label") or t.get("name") or "").strip().lower()
            if label == "yes":
                for k in ("token_id", "tokenId", "tokenID", "id", "clobTokenId"):
                    if t.get(k):
                        return str(t.get(k))

    clob_ids = m.get("clobTokenIds") or m.get("clob_token_ids")
    if isinstance(clob_ids, list) and clob_ids:
        return str(clob_ids[0])

    for k in ("yesTokenId", "yes_token_id"):
        if m.get(k):
            return str(m.get(k))

    return None

def upsert_event(title: str, slug: Optional[str], gamma_market_id: Optional[str], yes_token_id: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        row = None
        if slug:
            row = conn.execute("SELECT id FROM events WHERE slug = ?", (slug,)).fetchone()
        if not row:
            row = conn.execute("SELECT id FROM events WHERE yes_token_id = ?", (yes_token_id,)).fetchone()

        if row:
            conn.execute(
                """UPDATE events
                   SET title=?, slug=?, gamma_market_id=?, yes_token_id=?
                   WHERE id=?""",
                (title, slug, gamma_market_id, yes_token_id, row["id"])
            )
        else:
            eid = str(uuid.uuid4())
            conn.execute(
                """INSERT INTO events
                   (id, title, created_at, slug, gamma_market_id, yes_token_id, latest_pm_p, latest_machine_p)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (eid, title, now, slug, gamma_market_id, yes_token_id, None, None)
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

@app.post("/v1/admin/events", response_model=EventOut)
def admin_create_event(payload: EventCreate, x_admin_token: Optional[str] = Header(None)):
    check_admin(x_admin_token)
    eid = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        conn.execute(
            """INSERT INTO events
               (id, title, created_at, slug, gamma_market_id, yes_token_id, latest_pm_p, latest_machine_p)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (eid, payload.title.strip(), now, None, None, None, None, None)
        )
        conn.commit()
    return EventOut(id=eid, title=payload.title.strip())

@app.post("/v1/admin/jobs/run")
def admin_run_job(job_name: str, x_admin_token: Optional[str] = Header(None)):
    check_admin(x_admin_token)

    if job_name not in {"discover_markets", "snapshot_pm", "forecast_machine", "update_prices"}:
        raise HTTPException(status_code=400, detail="Unknown job")

    # -------------------------
    # 1) DISCOVER REAL POLYMARKET MARKETS
    # -------------------------
    if job_name == "discover_markets":
        keywords = [k.strip().lower() for k in DISCOVER_KEYWORDS.split(",") if k.strip()]
        candidates: list[tuple[float, dict]] = []
        offset = 0

        for _ in range(DISCOVER_PAGES):
            markets = gamma_get_markets(limit=100, offset=offset)
            if not markets:
                break

            for m in markets:
                title = (m.get("question") or m.get("title") or "").strip()
                if not title:
                    continue
                tl = title.lower()
                if not any(k in tl for k in keywords):
                    continue

                # Score by volume/liquidity
                vol = fnum(m.get("volume") or m.get("volume24h") or m.get("volume_24hr") or 0)
                liq = fnum(m.get("liquidity") or 0)
                score = vol + (0.1 * liq)

                candidates.append((score, m))

            offset += 100

        candidates.sort(key=lambda x: x[0], reverse=True)

        used = 0
        for _, m in candidates[:DISCOVER_LIMIT]:
            title = (m.get("question") or m.get("title") or "").strip()
            slug = m.get("slug")
            mid = m.get("id") or m.get("marketId") or m.get("market_id")
            yes_tid = extract_yes_token_id(m)
            if not yes_tid:
                continue  # skip markets where we can't trade YES easily
            upsert_event(
                title=title,
                slug=slug,
                gamma_market_id=str(mid) if mid else None,
                yes_token_id=yes_tid
            )
            used += 1

        return {"ok": True, "job": job_name, "discovered": used}

    # -------------------------
    # 2) SNAPSHOT POLYMARKET PRICES
    # -------------------------
    if job_name == "snapshot_pm":
        updated = 0
        with db() as conn:
            rows = conn.execute("SELECT id, yes_token_id FROM events WHERE yes_token_id IS NOT NULL").fetchall()
            for r in rows:
                p = clob_midpoint(r["yes_token_id"])
                if p is None:
                    continue
                p = clamp01(p)
                conn.execute("UPDATE events SET latest_pm_p = ? WHERE id = ?", (p, r["id"]))
                updated += 1
            conn.commit()
        return {"ok": True, "job": job_name, "updated": updated}

    # -------------------------
    # 3) FORECAST MACHINE FROM PM
    # -------------------------
    if job_name == "forecast_machine":
        updated = 0
        with db() as conn:
            rows = conn.execute("SELECT id, latest_pm_p FROM events WHERE latest_pm_p IS NOT NULL").fetchall()
            for r in rows:
                p_m = machine_from_pm(float(r["latest_pm_p"]))
                conn.execute("UPDATE events SET latest_machine_p = ? WHERE id = ?", (p_m, r["id"]))
                updated += 1
            conn.commit()
        return {"ok": True, "job": job_name, "updated": updated}

    # -------------------------
    # 4) UPDATE PRICES (SNAPSHOT + FORECAST)
    # -------------------------
    if job_name == "update_prices":
        snap = 0
        with db() as conn:
            rows = conn.execute("SELECT id, yes_token_id FROM events WHERE yes_token_id IS NOT NULL").fetchall()
            for r in rows:
                p = clob_midpoint(r["yes_token_id"])
                if p is None:
                    continue
                p = clamp01(p)
                conn.execute("UPDATE events SET latest_pm_p = ? WHERE id = ?", (p, r["id"]))
                snap += 1
            conn.commit()

        fore = 0
        with db() as conn:
            rows = conn.execute("SELECT id, latest_pm_p FROM events WHERE latest_pm_p IS NOT NULL").fetchall()
            for r in rows:
                p_m = machine_from_pm(float(r["latest_pm_p"]))
                conn.execute("UPDATE events SET latest_machine_p = ? WHERE id = ?", (p_m, r["id"]))
                fore += 1
            conn.commit()

        return {"ok": True, "job": job_name, "snapshot_updated": snap, "forecast_updated": fore}

    raise HTTPException(status_code=400, detail="Unhandled job")