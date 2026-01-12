from fastapi import FastAPI

app = FastAPI(title="Edge Machine API")

@app.get("/health")
def health():
    return {"ok": True}