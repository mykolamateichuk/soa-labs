from contextlib import asynccontextmanager

from fastapi import FastAPI
import threading
import requests
import pika
import json
import os

app = FastAPI(title="Measurement Service")

SERVICE_NAME = "measurement_service"
SERVICE_HOST = "measurement_service"
SERVICE_PORT = 8003
CONSUL_HOST = "consul"
CONSUL_PORT = 8500
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")

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
    return {"status": "ok", "service": "measurement_service"}

@app.post("/measure")
def measure():
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue='notify_queue', durable=True)

    message = json.dumps({"task": "send_notification", "text": "Measurement complete"})
    channel.basic_publish(exchange='', routing_key='notify_queue', body=message)

    connection.close()

    return {"status": "queued", "message": "Notification task sent to RabbitMQ"}