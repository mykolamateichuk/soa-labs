from fastapi import FastAPI

app = FastAPI(title="Analytics Service")

@app.get("/health")
def health():
    return {"status": "ok", "service": "analytics_service"}
