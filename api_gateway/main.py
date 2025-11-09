from fastapi import FastAPI
import httpx
import os
import random

app = FastAPI()

CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
CONSUL_PORT = int(os.getenv("CONSUL_PORT", 8500))

USER_SERVICE_NAME = "user_service"
MEASUREMENT_SERVICE_URL = os.getenv("MEASUREMENT_SERVICE_URL", "http://measurement_service:8003")


async def get_service_instances(service_name: str):
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(
                f"http://{CONSUL_HOST}:{CONSUL_PORT}/v1/health/service/{service_name}?passing=true"
            )
            res.raise_for_status()
            data = res.json()
            instances = []
            for svc in data:
                address = svc["Service"]["Address"]
                port = svc["Service"]["Port"]
                instances.append(f"http://{address}:{port}")
            return instances
    except Exception as e:
        print(f"Error fetching from Consul ({service_name}): {e}")
        return []


@app.get("/api/user")
async def get_user_data():
    instances = await get_service_instances(USER_SERVICE_NAME)
    if not instances:
        return {"error": "No user_service instances found"}

    target = random.choice(instances)
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{target}/user")
            return {"target": target, "response": response.json()}
    except Exception as e:
        return {"error": str(e), "target": target}


@app.post("/api/measure")
async def create_measurement():
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{MEASUREMENT_SERVICE_URL}/measure")
            return {"measurement": response.json()}
    except Exception as e:
        return {"error": str(e)}


@app.get("/health")
async def health():
    return "API Gateway OK"
