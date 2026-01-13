from __future__ import annotations

from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timezone
import os
import sqlite3
import uuid
import requests
import time
import secrets

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
DISCOVER_DETAIL_MAX = int(os.getenv("PM_DISCOVER_DETAIL_MAX", "10"))  # detail calls per discover run
HYDRATE_MAX = int(os.getenv("PM_HYDRATE_MAX", "15"))                  # detail calls per hydrate run
PRICE_UPDATE_MAX = int(os.getenv("PM_PRICE_UPDATE_MAX", "20"))        # clob calls per update run

REQUESTS_TIMEOUT_SECS = int(os.getenv("PM_HTTP_TIMEOUT_SECS", "15"))

# =========================
# APP
# =========================

app = FastAPI(title="Edge Machine API", version="1.1.0-hardened")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ok for now; lock down later
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# DB
# =========================

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == col for r in rows)

def init_db() -> None:
    with get_conn() as conn:
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
        # safe migrations
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

def check_admin(x_admin_token: Optional[str]) -> None:
    # timing-safe compare
    if (not x_admin_token) or (not secrets.compare_digest(x_admin_token, ADMIN_TOKEN)):
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

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _try_parse_iso(v) -> Optional[datetime]:
    if not v:
        return None
    try:
        s = str(v).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def machine_from_pm(p_pm: float) -> float:
    """
    Minimal machine until you re-plug v0.2 ensemble.
    Stable & safe.
    """
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
    r = requests.get(f"{GAMMA_BASE}/markets", params=params, timeout=REQUESTS_TIMEOUT_SECS)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        return data
    return data.get("markets") or data.get("data") or []

def gamma_get_detail(mid: str) -> Optional[dict]:
    # Many Gamma deployments support one of these:
    for url in (f"{GAMMA_BASE}/markets/{mid}", f"{GAMMA_BASE}/market/{mid}"):
        try:
            r = requests.get(url, timeout=REQUESTS_TIMEOUT_SECS)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
    return None

def is_current(m: dict) -> bool:
    """
    Best-effort: skip obviously closed/resolved markets.
    Gamma schema varies, so we only apply checks when fields exist.
    """
    # explicit active flag
    if "active" in m and m.get("active") is False:
        return False

    # explicit closed/resolved flags (if present)
    for k in ("closed", "isClosed", "resolved", "isResolved", "archived"):
        if k in m and m.get(k) is True:
            return False

    # end/close time (if present)
    for k in ("end_date", "endDate", "closeTime", "close_time", "closeDate", "close_date", "resolutionTime"):
        dt = _try_parse_iso(m.get(k))
        if dt and dt < now_utc():
            return False

    return True

def extract_yes_token_id(m: dict) -> Optional[str]:
    """
    Safer YES extraction:
    - Never assumes clobTokenIds[0] == YES.
    - Only returns YES when label mapping exists.
    - Supports outcomes list[str] + clobTokenIds alignment.
    - Supports tokens/outcomes list[dict].
    """
    # direct fields (rare)
    for k in ("yesTokenId", "yes_token_id", "yes_token", "yesToken"):
        if m.get(k):
            return str(m.get(k))

    tokens_or_outcomes = m.get("tokens") or m.get("outcomes")
    clob_ids = m.get("clobTokenIds") or m.get("clob_token_ids")

    # case: list[dict]
    if isinstance(tokens_or_outcomes, list) and tokens_or_outcomes and isinstance(tokens_or_outcomes[0], dict):
        # prefer explicit label YES/TRUE
        for t in tokens_or_outcomes:
            label = (t.get("outcome") or t.get("label") or t.get("name") or "").strip().lower()
            if label in ("yes", "true"):
                for key in ("token_id", "tokenId", "tokenID", "id", "clobTokenId"):
                    if t.get(key):
                        return str(t.get(key))

        # if labels exist but IDs are only in clobTokenIds aligned by index
        if isinstance(clob_ids, list) and len(clob_ids) == len(tokens_or_outcomes):
            for i, t in enumerate(tokens_or_outcomes):
                label = (t.get("outcome") or t.get("label") or t.get("name") or "").strip().lower()
                if label in ("yes", "true"):
                    return str(clob_ids[i])

        return None

    # case: outcomes list[str] + clobTokenIds aligned
    if isinstance(tokens_or_outcomes, list) and tokens_or_outcomes and isinstance(tokens_or_outcomes[0], str):
        if isinstance(clob_ids, list) and len(clob_ids) == len(tokens_or_outcomes):
            for i, name in enumerate(tokens_or_outcomes):
                if str(name).strip().lower() in ("yes", "true"):
                    return str(clob_ids[i])

    # no safe mapping found
    return None

# =========================
# DB UPSERT (BATCH-FRIENDLY)
# =========================

def upsert_event_in_conn(
    conn: sqlite3.Connection,
    title: str,
    slug: Optional[str],
    gamma_market_id: Optional[str],
    yes_token_id: Optional[str],
) -> None:
    # Use slug/gamma_market_id to match existing row
    row = None
    if slug:
        row = conn.execute("SELECT id, yes_token_id FROM events WHERE slug = ?", (slug,)).fetchone()
    if not row and gamma_market_id:
        row = conn.execute("SELECT id, yes_token_id FROM events WHERE gamma_market_id = ?", (gamma_market_id,)).fetchone()

    if row:
        existing_yes = row["yes_token_id"]
        # only set yes_token_id if new one exists (never wipe)
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
        created_at = now_utc().isoformat()
        conn.execute(
            """INSERT INTO events
               (id, title, created_at, slug, gamma_market_id, yes_token_id, latest_pm_p, latest_machine_p)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (eid, title, created_at, slug, gamma_market_id, yes_token_id, None, None)
        )

# =========================
# ROUTES
# =========================

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/v1/events", response_model=List[EventOut])
def list_events(limit: int = 50):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM events ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()

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
    created_at = now_utc().isoformat()
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO events
               (id, title, created_at, slug, gamma_market_id, yes_token_id, latest_pm_p, latest_machine_p)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (eid, payload.title.strip(), created_at, None, None, None, None, None)
        )
        conn.commit()
    return EventOut(id=eid, title=payload.title.strip())

@app.post("/v1/admin/jobs/run")
def admin_run_job(job_name: str = Query(...), x_admin_token: Optional[str] = Header(None)):
    check_admin(x_admin_token)

    if job_name not in {"discover_markets", "hydrate_tokens", "update_prices"}:
        raise HTTPException(status_code=400, detail="Unknown job")

    # -------------------------
    # DISCOVER MARKETS (batch DB, bounded runtime)
    # -------------------------
    if job_name == "discover_markets":
        t0 = time.time()
        keywords = [k.strip().lower() for k in DISCOVER_KEYWORDS.split(",") if k.strip()]

        markets = gamma_get_markets(limit=100, offset=0)

        candidates: List[Tuple[float, Dict[str, Any]]] = []
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
        detail_cache: Dict[str, dict] = {}

        with get_conn() as conn:
            conn.execute("BEGIN")
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

                if (not yes_tid) and (detail_calls < DISCOVER_DETAIL_MAX):
                    if gamma_market_id in detail_cache:
                        detail = detail_cache[gamma_market_id]
                    else:
                        detail = gamma_get_detail(gamma_market_id)
                        if detail:
                            detail_cache[gamma_market_id] = detail
                        detail_calls += 1

                    if detail:
                        yes_tid = extract_yes_token_id(detail)

                upsert_event_in_conn(conn, title, slug, gamma_market_id, yes_tid)

                used += 1
                if yes_tid:
                    tokened += 1

            conn.commit()

        return {
            "ok": True,
            "job": job_name,
            "discovered": used,
            "tokened": tokened,
            "detail_calls": detail_calls,
            "time_secs": round(time.time() - t0, 3),
        }

    # -------------------------
    # HYDRATE TOKENS (detail fetch for rows missing yes_token_id)
    # -------------------------
    if job_name == "hydrate_tokens":
        t0 = time.time()
        attempted = 0
        hydrated = 0
        detail_cache: Dict[str, dict] = {}

        with get_conn() as conn:
            rows = conn.execute(
                """SELECT id, gamma_market_id
                   FROM events
                   WHERE (yes_token_id IS NULL OR yes_token_id = '')
                     AND gamma_market_id IS NOT NULL
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (HYDRATE_MAX,)
            ).fetchall()

            conn.execute("BEGIN")
            for r in rows:
                if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                    break

                attempted += 1
                mid = r["gamma_market_id"]

                detail = detail_cache.get(mid)
                if detail is None:
                    detail = gamma_get_detail(mid)
                    if detail:
                        detail_cache[mid] = detail

                if not detail:
                    continue

                yes_tid = extract_yes_token_id(detail)
                if not yes_tid:
                    continue

                conn.execute(
                    "UPDATE events SET yes_token_id=? WHERE id=?",
                    (yes_tid, r["id"])
                )
                hydrated += 1

            conn.commit()

        return {
            "ok": True,
            "job": job_name,
            "attempted": attempted,
            "hydrated": hydrated,
            "time_secs": round(time.time() - t0, 3),
        }

    # -------------------------
    # UPDATE PRICES (bounded by PRICE_UPDATE_MAX and time budget)
    # -------------------------
    if job_name == "update_prices":
        t0 = time.time()
        snap = 0
        fore = 0
        skipped_no_token = 0
        skipped_no_pm = 0

        with get_conn() as conn:
            rows = conn.execute(
                """SELECT id, yes_token_id
                   FROM events
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (PRICE_UPDATE_MAX,)
            ).fetchall()

            conn.execute("BEGIN")
            for r in rows:
                if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                    break

                tid = r["yes_token_id"]
                if not tid:
                    skipped_no_token += 1
                    continue

                p = clob_midpoint(tid)
                if p is None:
                    continue

                p = clamp01(p)
                conn.execute("UPDATE events SET latest_pm_p=? WHERE id=?", (p, r["id"]))
                snap += 1

            conn.commit()

        with get_conn() as conn:
            rows = conn.execute(
                """SELECT id, latest_pm_p
                   FROM events
                   WHERE latest_pm_p IS NOT NULL
                   ORDER BY created_at DESC
                   LIMIT ?""",
                (PRICE_UPDATE_MAX,)
            ).fetchall()

            conn.execute("BEGIN")
            for r in rows:
                if time.time() - t0 > JOB_TIME_BUDGET_SECS:
                    break

                p_pm = float(r["latest_pm_p"])
                p_m = machine_from_pm(p_pm)
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