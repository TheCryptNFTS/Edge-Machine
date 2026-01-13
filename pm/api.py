from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Edge Machine API")

# Allow browser frontend (Vercel) to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

# Minimal endpoint your frontend needs right now
@app.get("/v1/events")
def v1_events(limit: int = 50):
    return []