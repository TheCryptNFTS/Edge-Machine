from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import os
import uuid
from datetime import datetime

app = FastAPI(title="Edge Machine API")

# -------------------------
# In-memory event store
# -------------------------
EVENTS: List[dict] = []


# -------------------------
# Models
# -------------------------
class EventCreate(BaseModel):
    name: str
    source: Optional[str] = "discover_markets"


class EventOut(BaseModel):
    id: str
    name: str
    source: str
    created_at: str


# -------------------------
# Health
# -------------------------
@app.get("/health")
def health():
    return {"ok": True}


# -------------------------
# Public: list events
# -------------------------
@app.get("/v1/events", response_model=List[EventOut])
def list_events():
    return EVENTS


# -------------------------
# Admin: create event
# -------------------------
@app.post("/v1/admin/events", response_model=EventOut)
def create_event(
    event: EventCreate,
    x_admin_token: Optional[str] = Header(None),
):
    required = os.environ.get("PM_ADMIN_TOKEN")
    if required and x_admin_token != required:
        raise HTTPException(status_code=401, detail="Invalid admin token")

    ev = {
        "id": str(uuid.uuid4()),
        "name": event.name,
        "source": event.source,
        "created_at": datetime.utcnow().isoformat() + "Z",
    }

    EVENTS.append(ev)
    return ev


# -------------------------
# Admin: run jobs
# -------------------------
@app.post("/v1/admin/jobs/run")
def run_job(
    job_name: str,
    x_admin_token: Optional[str] = Header(None),
):
    required = os.environ.get("PM_ADMIN_TOKEN")
    if required and x_admin_token != required:
        raise HTTPException(status_code=401, detail="Invalid admin token")

    # -------------------------
    # JOB: discover_markets
    # -------------------------
    if job_name == "discover_markets":
        discovered = []

        # ⚠️ This is where real Polymarket logic would go
        # Right now we FORCE one event so the pipeline works
        test_event = {
            "id": str(uuid.uuid4()),
            "name": "Polymarket Sample Market",
            "source": "discover_markets",
            "created_at": datetime.utcnow().isoformat() + "Z",
        }

        EVENTS.append(test_event)
        discovered.append(test_event)

        return {
            "ok": True,
            "job": job_name,
            "discovered": len(discovered),
        }

    # -------------------------
    # Unknown job
    # -------------------------
    raise HTTPException(status_code=400, detail=f"Unknown job: {job_name}")