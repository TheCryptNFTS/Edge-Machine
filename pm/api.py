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

ADMIN_TOKEN = (
    os.getenv("ADMIN_TOKEN")
    or os.getenv("PM_ADMIN_TOKEN")
    or "change-me"
)

GAMMA_BASE = os.getenv("PM_GAMMA_BASE", "https://gamma-api.polymarket.com").rstrip("/")
CLOB_BASE = os.getenv("PM_CLOB_BASE", "https://clob.polymarket.com").rstrip("/")

DISCOVER_LIMIT = int(os.getenv("PM_DISCOVER_LIMIT", "50"))
DISCOVER_KEYWORDS = os.getenv(
    "PM_DISCOVER_KEYWORDS",
    "bitcoin,btc,ethereum,eth,sol,solana,crypto,memecoin,doge,ai,trump,election,fed,inflation,rate"
)

# ---- timeout-safe knobs ----
JOB_TIME_BUDGET_SECS = int(os.getenv("PM_JOB_TIME_BUDGET_SECS", "8"))
DISCOVER_DETAIL_MAX = int(os.getenv("PM_DISCOVER_DETAIL_MAX", "10"))
HYDRATE_MAX = int(os.getenv("PM_HYDRATE_MAX", "15"))          # token lookups per run
PRICE_UPDATE_MAX = int(os.getenv("PM_PRICE_UPDATE_MAX", "20")) # clob calls per run

# =========================
# APP
# =========================

app = FastAPI(title="Edge Machine API", version="1.2.0-hydrate-fixed")

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
            slug TEXT,
            gamma_market_id TEXT,
            yes_token_id TEXT,
            latest_pm_p REAL,
            latest_machine_p REAL
        )
        """)
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
    p = 0.90 * p_pm + 0.10 * 0.5
    if p_pm >= 0.94:
        p = min(p, 0.995)
    if p_pm <= 0.06:
        p = max(p, 0.005)
    return clamp01(p)

# =========================
# POLYMARKET: CLOB
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
# POLYMARKET: GAMMA
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
    for url in [f"{GAMMA_BASE}/markets/{mid}", f"{GAMMA_BASE}/market/{mid}"]:
        try:
            r = requests.get(url, timeout=15)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
    return None

def extract_yes_token_id(m: dict) -> Optional[str]:
    """
    Robust YES token extraction for Gamma schema variants.

    Handles:
      - tokens/outcomes as list[dict]
      - outcomes as list[str] with clobTokenIds aligned by index
      - root yesTokenId/yes_token_id
      - binary fallback
    """

    # 0) Direct root fields
    for k in ("yesTokenId", "yes_token_id", "yes_token", "yesToken"):
        if m.get(k):
            return str(m.get(k))

    tokens_or_outcomes = m.get("tokens") or m.get("outcomes")

    # 1) tokens/outcomes as list[dict]
    if isinstance(tokens_or_outcomes, list) and tokens_or_outcomes and isinstance(tokens_or_outcomes[0], dict):
        for t in tokens_or_outcomes:
            label = (t.get("outcome") or t.get("label") or t.get("name") or "").strip().lower()
            if label in ("yes", "true"):
                for key in ("token_id", "tokenId", "tokenID", "id", "clobTokenId"):
                    if t.get(key):
                        return str(t.get(key))

        clob_ids = m.get("clobTokenIds") or m.get("clob_token_ids")
        if isinstance(clob_ids, list) and len(clob_ids) == len(tokens_or_outcomes):
            for i, t in enumerate(tokens_or_outcomes):
                label = (t.get("outcome") or t.get("label") or t.get("name") or "").strip().lower()
                if label in ("yes", "true"):
                    return str(clob_ids[i])

        return None

    # 2) outcomes as list[str] + clobTokenIds aligned by index
    if isinstance(tokens_or_outcomes, list) and tokens_or_outcomes and isinstance(tokens_or_outcomes[0], str):
        clob_ids = m.get("clobTokenIds") or m.get("clob_token_ids")
        if isinstance(clob_ids, list) and len(clob_ids) == len(tokens_or_outcomes):
            for i, name in enumerate(tokens_or_outcomes):
                if str(name).strip().lower() in ("yes", "true"):
                    return str(clob_ids[i])

    # 3) last resort: binary fallback
    clob_ids = m.get("clobTokenIds") or m.get("clob_token_ids")
    if isinstance(clob_ids, list) and len(clob_ids) == 2:
        return str(clob_ids[0])

    return None

def is_current(m: dict) -> bool:
    if "active" in m and m.get("active") is False:
        return False
    return True

def upsert_event(title: str, slug: Optional[str], gamma_market_id: Optional[str], yes_token_id: Optional[str]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        row = None
        if slug:
            row = conn.execute("SELECT id FROM events WHERE slug = ?", (slug,)).fetchone()
        if not row and gamma_market_id:
            row = conn.execute("SELECT id FROM events WHERE gamma_market_id = ?", (gamma_market_id,)).fetchone()

        if row:
            if yes_token_id:
                conn.execute(
                    "UPDATE events SET title=?, slug=?, gamma_market_id=?, yes_token_id=? WHERE id=?",
                    (title, slug, gamma_market_id, yes_token_id, row["id"])
                )
            else:
                conn.execute(
                    "UPDATE events SET title=?, slug=?, gamma_market_id=? WHERE id=?",
                    (title, slug, gamma_market_id, row["id"])
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

@app.post("/v1/admin/jobs/run")
def admin_run_job(job_name: str = Query(...), x_admin_token: Optional[str] = Header(None)):
    check_admin(x_admin_token)

    if job_name not in {"discover_markets", "hydrate_tokens", "update_prices"}:
        raise HTTPException(status_code=400, detail="Unknown job")

    # -------------------------
    # DISCOVER MARKETS
    # -------------------------
    if job_name == "discover_markets":
        t0 = time.time()
        keywords = [k.strip().lower() for k in DISCOVER_KEYWORDS.split(",") if k.strip()]
        markets = gamma_get_markets(limit=100, offset=0)

        candidates: list[tuple[float, dict]] = []
        for m in markets:
            if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                break
            title = (m.get("question") or m.get("title") or "").strip()
            if not title:
                continue
            if not is_current(m):
                continue
            tl = title.lower()
            if not any(k in tl for k in keywords):
                continue
            vol = fnum(m.get("volume") or m.get("volume24h") or m.get("volume_24hr") or 0)
            liq = fnum(m.get("liquidity") or 0)
            score = vol + (0.1 * liq)
            candidates.append((score, m))

        candidates.sort(key=lambda x: x[0], reverse=True)

        used = 0
        tokened = 0
        detail_calls = 0

        for _, m in candidates[:DISCOVER_LIMIT]:
            if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                break

            title = (m.get("question") or m.get("title") or "").strip()
            slug = m.get("slug")
            mid = m.get("id") or m.get("marketId") or m.get("market_id")
            gamma_market_id = str(mid) if mid else None
            if not gamma_market_id:
                continue

            yes_tid = extract_yes_token_id(m)
            if not yes_tid and detail_calls < DISCOVER_DETAIL_MAX:
                detail = gamma_get_detail(gamma_market_id)
                detail_calls += 1
                if detail:
                    yes_tid = extract_yes_token_id(detail)

            upsert_event(title=title, slug=slug, gamma_market_id=gamma_market_id, yes_token_id=yes_tid)
            used += 1
            if yes_tid:
                tokened += 1

        return {
            "ok": True,
            "job": job_name,
            "discovered": used,
            "tokened": tokened,
            "detail_calls": detail_calls,
            "time_secs": round(time.time() - t0, 3),
        }

    # -------------------------
    # HYDRATE TOKENS
    # -------------------------
    if job_name == "hydrate_tokens":
        t0 = time.time()
        hydrated = 0
        attempted = 0

        with db() as conn:
            rows = conn.execute(
                """SELECT id, gamma_market_id
                   FROM events
                   WHERE (yes_token_id IS NULL OR yes_token_id = '')
                     AND gamma_market_id IS NOT NULL
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (HYDRATE_MAX,)
            ).fetchall()

            for r in rows:
                if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                    break

                attempted += 1
                mid = r["gamma_market_id"]
                detail = gamma_get_detail(mid)
                if not detail:
                    continue

                yes_tid = extract_yes_token_id(detail)
                if not yes_tid:
                    continue

                conn.execute("UPDATE events SET yes_token_id=? WHERE id=?", (yes_tid, r["id"]))
                hydrated += 1

            conn.commit()

        return {"ok": True, "job": job_name, "attempted": attempted, "hydrated": hydrated, "time_secs": round(time.time() - t0, 3)}

    # -------------------------
    # UPDATE PRICES
    # -------------------------
    if job_name == "update_prices":
        t0 = time.time()
        snap = 0
        fore = 0
        skipped_no_token = 0
        skipped_no_pm = 0

        with db() as conn:
            rows = conn.execute(
                """SELECT id, yes_token_id
                   FROM events
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (PRICE_UPDATE_MAX,)
            ).fetchall()

            for r in rows:
                if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                    break
                if not r["yes_token_id"]:
                    skipped_no_token += 1
                    continue
                p = clob_midpoint(r["yes_token_id"])
                if p is None:
                    continue
                p = clamp01(p)
                conn.execute("UPDATE events SET latest_pm_p=? WHERE id=?", (p, r["id"]))
                snap += 1

            conn.commit()

        with db() as conn:
            rows = conn.execute(
                """SELECT id, latest_pm_p
                   FROM events
                   WHERE latest_pm_p IS NOT NULL
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (PRICE_UPDATE_MAX,)
            ).fetchall()

            for r in rows:
                if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                    break
                p_m = machine_from_pm(float(r["latest_pm_p"]))
                conn.execute("UPDATE events SET latest_machine_p=? WHERE id=?", (p_m, r["id"]))
                fore += 1

            conn.commit()

        return {
            "ok": True,
            "job": job_name,
            "snapshot_updated": snap,
            "forecast_updated": fore,
            "skipped_no_token": skipped_no_token,
            "skipped_no_pm": skipped_no_pm,
            "time_secs": round(time.time() - t0, 3),
        }

    raise HTTPException(status_code=400, detail="Unhandled job")