from fastapi import FastAPI
import requests, threading
from contextlib import asynccontextmanager

SERVICE_NAME = "child_profile_service"
SERVICE_HOST = "child_profile_service"
SERVICE_PORT = 8002
CONSUL_HOST = "consul"
CONSUL_PORT = 8500

def register_service():
    requests.put(
        f"http://{CONSUL_HOST}:{CONSUL_PORT}/v1/agent/service/register",
        json={
            "Name": SERVICE_NAME,
            "Address": SERVICE_HOST,
            "Port": SERVICE_PORT,
            "Check": {
                "HTTP": f"http://{SERVICE_HOST}:{SERVICE_PORT}/health",
                "Interval": "10s"
            }
        }
    )

@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=register_service, daemon=True).start()
    yield

app = FastAPI(lifespan=lifespan)

@app.get("/health")
def health():
    return {"status": "ok", "service": "child_profile_service"}

@app.get("/profiles")
def get_profiles():
    return {
        "service": "child_profile_service",
        "profiles": [
            {"child": "John", "age": 7},
            {"child": "Jane", "age": 10}
        ]
    }
