from __future__ import annotations

import os
import time
import uuid
import sqlite3
import secrets
from contextlib import closing
from datetime import datetime, timezone
from typing import Optional, List

import requests
from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


# =========================
# CONFIG
# =========================

DATABASE_URL = os.getenv("PM_DATABASE_URL", "sqlite:///./pm.db")
DB_PATH = DATABASE_URL.replace("sqlite:///", "")

ADMIN_TOKEN = os.getenv("PM_ADMIN_TOKEN", os.getenv("ADMIN_TOKEN", "change-me"))

GAMMA_BASE = os.getenv("PM_GAMMA_BASE", "https://gamma-api.polymarket.com").rstrip("/")
CLOB_BASE = os.getenv("PM_CLOB_BASE", "https://clob.polymarket.com").rstrip("/")

DISCOVER_LIMIT = int(os.getenv("PM_DISCOVER_LIMIT", "50"))
DISCOVER_PAGES = int(os.getenv("PM_DISCOVER_PAGES", "3"))
DISCOVER_PAGE_SIZE = int(os.getenv("PM_DISCOVER_PAGE_SIZE", "100"))

DISCOVER_KEYWORDS = os.getenv(
    "PM_DISCOVER_KEYWORDS",
    "bitcoin,btc,ethereum,eth,sol,solana,crypto,memecoin,doge,ai,trump,election,fed,inflation,rate"
)

STRICT_KEYWORDS = os.getenv("PM_STRICT_KEYWORDS", "true").lower() == "true"

JOB_TIME_BUDGET_SECS = int(os.getenv("PM_JOB_TIME_BUDGET_SECS", "12"))
DISCOVER_DETAIL_MAX = int(os.getenv("PM_DISCOVER_DETAIL_MAX", "25"))
HYDRATE_MAX = int(os.getenv("PM_HYDRATE_MAX", "50"))
PRICE_UPDATE_MAX = int(os.getenv("PM_PRICE_UPDATE_MAX", "50"))


# =========================
# APP
# =========================

app = FastAPI(title="Edge Machine API", version="1.4.0-binary-only")

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

def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def _has_column(conn, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == col for r in rows)

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
            no_token_id TEXT,
            latest_pm_p REAL,
            latest_machine_p REAL
        )
        """)
        # add columns if older DB
        for col, ddl in [
            ("slug", "ALTER TABLE events ADD COLUMN slug TEXT"),
            ("gamma_market_id", "ALTER TABLE events ADD COLUMN gamma_market_id TEXT"),
            ("yes_token_id", "ALTER TABLE events ADD COLUMN yes_token_id TEXT"),
            ("no_token_id", "ALTER TABLE events ADD COLUMN no_token_id TEXT"),
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
    no_token_id: Optional[str] = None
    latest_pm_p: Optional[float] = None
    latest_machine_p: Optional[float] = None

class JobResult(BaseModel):
    ok: bool
    job: str


# =========================
# AUTH
# =========================

def check_admin(x_admin_token: Optional[str]):
    if not x_admin_token or not secrets.compare_digest(x_admin_token, ADMIN_TOKEN):
        raise HTTPException(status_code=401, detail="Invalid admin token")


# =========================
# UTILS
# =========================

def clamp01(x: float) -> float:
    try:
        v = float(x)
    except Exception:
        return 0.5
    if v < 0.0:
        return 0.0
    if v > 1.0:
        return 1.0
    return v

def fnum(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def machine_from_pm(p_pm: float) -> float:
    # placeholder conservative machine
    p = 0.90 * p_pm + 0.10 * 0.5
    if p_pm >= 0.94:
        p = min(p, 0.995)
    if p_pm <= 0.06:
        p = max(p, 0.005)
    return clamp01(p)


# =========================
# GAMMA
# =========================

def gamma_get_markets(limit: int = 100, offset: int = 0) -> list[dict]:
    params = {"limit": limit, "offset": offset, "active": "true"}
    r = requests.get(f"{GAMMA_BASE}/markets", params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        return data
    return data.get("markets") or data.get("data") or []

def gamma_get_detail(mid: str) -> Optional[dict]:
    for url in (f"{GAMMA_BASE}/markets/{mid}", f"{GAMMA_BASE}/market/{mid}"):
        try:
            r = requests.get(url, timeout=15)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
    return None


def _binary_yes_no_tokens(payload: dict) -> tuple[Optional[str], Optional[str]]:
    """
    Return (YES, NO) token IDs if the market is clearly binary YES/NO.
    Supports multiple Gamma shapes.
    """
    tokens = payload.get("tokens") or payload.get("outcomes") or []
    if isinstance(tokens, list) and tokens:
        yes = None
        no = None
        for t in tokens:
            label = (t.get("outcome") or t.get("label") or t.get("name") or "").strip().lower()
            tid = t.get("token_id") or t.get("tokenId") or t.get("tokenID") or t.get("id") or t.get("clobTokenId")
            if not tid:
                continue
            if label == "yes":
                yes = str(tid)
            elif label == "no":
                no = str(tid)
        if yes and no:
            return yes, no

    # Some payloads have outcomes + clobTokenIds arrays
    outcomes = payload.get("outcomes")
    clob_ids = payload.get("clobTokenIds") or payload.get("clob_token_ids")
    if isinstance(outcomes, list) and isinstance(clob_ids, list) and len(outcomes) == len(clob_ids):
        yes = None
        no = None
        for o, tid in zip(outcomes, clob_ids):
            lab = str(o).strip().lower()
            if lab == "yes":
                yes = str(tid)
            elif lab == "no":
                no = str(tid)
        if yes and no:
            return yes, no

    return None, None


def _looks_binary_yesno(payload: dict) -> bool:
    yes, no = _binary_yes_no_tokens(payload)
    return bool(yes and no)


# =========================
# CLOB
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
# DB OPS
# =========================

def upsert_event(conn, title: str, slug: Optional[str], gamma_market_id: str, yes_token_id: Optional[str], no_token_id: Optional[str]) -> None:
    now = datetime.now(timezone.utc).isoformat()

    row = None
    if slug:
        row = conn.execute("SELECT id FROM events WHERE slug = ?", (slug,)).fetchone()
    if not row:
        row = conn.execute("SELECT id FROM events WHERE gamma_market_id = ?", (gamma_market_id,)).fetchone()

    if row:
        conn.execute(
            "UPDATE events SET title=?, slug=?, gamma_market_id=?, yes_token_id=?, no_token_id=? WHERE id=?",
            (title, slug, gamma_market_id, yes_token_id, no_token_id, row["id"]),
        )
    else:
        eid = str(uuid.uuid4())
        conn.execute(
            """INSERT INTO events
               (id, title, created_at, slug, gamma_market_id, yes_token_id, no_token_id, latest_pm_p, latest_machine_p)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (eid, title, now, slug, gamma_market_id, yes_token_id, no_token_id, None, None),
        )


# =========================
# ROUTES
# =========================

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
    return [
        EventOut(
            id=r["id"],
            title=r["title"],
            slug=r["slug"],
            gamma_market_id=r["gamma_market_id"],
            yes_token_id=r["yes_token_id"],
            no_token_id=r["no_token_id"],
            latest_pm_p=r["latest_pm_p"],
            latest_machine_p=r["latest_machine_p"],
        )
        for r in rows
    ]


@app.post("/v1/admin/jobs/run")
def admin_run_job(job_name: str = Query(...), x_admin_token: Optional[str] = Header(None)):
    check_admin(x_admin_token)

    if job_name not in {"discover_markets", "hydrate_tokens", "update_prices"}:
        raise HTTPException(status_code=400, detail="Unknown job")

    t0 = time.time()

    # -------------------------
    # DISCOVER: only binary YES/NO
    # -------------------------
    if job_name == "discover_markets":
        keywords = [k.strip().lower() for k in DISCOVER_KEYWORDS.split(",") if k.strip()]

        markets: list[dict] = []
        for page in range(DISCOVER_PAGES):
            if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                break
            offset = page * DISCOVER_PAGE_SIZE
            markets.extend(gamma_get_markets(limit=DISCOVER_PAGE_SIZE, offset=offset))

        candidates: list[tuple[float, dict]] = []
        for m in markets:
            if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                break

            title = (m.get("question") or m.get("title") or "").strip()
            if not title:
                continue
            tl = title.lower()

            if STRICT_KEYWORDS and keywords and not any(k in tl for k in keywords):
                continue

            # only keep likely binary yes/no
            if not _looks_binary_yesno(m):
                # try a few detail calls (some