from contextlib import asynccontextmanager

from fastapi import FastAPI
import random, requests
import socket
import os
import threading
import time
import httpx


CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
CONSUL_PORT = int(os.getenv("CONSUL_PORT", 8500))
SERVICE_NAME = "user_service"
SERVICE_PORT = int(os.getenv("SERVICE_PORT", 8001))
CHILD_SERVICE_URL = os.getenv("CHILD_SERVICE_URL", "http://child_profile_service:8002")


def discover_child_service():
    r = requests.get(f"http://{CONSUL_HOST}:{CONSUL_PORT}/v1/health/service/child_profile_service?passing=true")
    services = r.json()
    if not services:
        return None
    instance = random.choice(services)
    address = instance["Service"]["Address"]
    port = instance["Service"]["Port"]
    return f"http://{address}:{port}"


def register_service():
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)

    payload = {
        "Name": SERVICE_NAME,
        "Address": ip_address,
        "Port": SERVICE_PORT,
        "Check": {
            "HTTP": f"http://{ip_address}:{SERVICE_PORT}/health",
            "Interval": "10s"
        }
    }

    while True:
        try:
            requests.put(f"http://{CONSUL_HOST}:{CONSUL_PORT}/v1/agent/service/register", json=payload)
            print(f"Registered {SERVICE_NAME} on {ip_address}:{SERVICE_PORT} to Consul")
            break
        except Exception as e:
            print(f"Consul registration failed: {e}. Retrying...")
            time.sleep(3)

@asynccontextmanager
async def lifespan(_app: FastAPI):
    threading.Thread(target=register_service, daemon=True).start()
    yield

app = FastAPI(lifespan=lifespan)

@app.get("/user")
async def get_user():
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{discover_child_service()}/profiles")
            return {"user": "John Doe", "child_data": response.json()}
    except Exception as e:
        return {"error": str(e)}


@app.get("/health")
async def health():
    return "user_service OK"
