from contextlib import asynccontextmanager

from fastapi import FastAPI
import pika
import threading
import requests
import time
import json
import os

app = FastAPI(title="Notification Service")

SERVICE_NAME = "notification_service"
SERVICE_HOST = "notification_service"
SERVICE_PORT = 8004
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
    return {"status": "ok", "service": "notification_service"}

def callback(ch, method, properties, body):
    data = json.loads(body)
    print(f"Received task: {data}")
    time.sleep(3)
    print("Notification sent!")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def consume():
    max_retries = 10
    retry_delay_seconds = 5
    connection = None

    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to RabbitMQ (Attempt {attempt + 1}/{max_retries})...")
            connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
            print("Successfully connected to RabbitMQ.")
            break
        except pika.exceptions.AMQPConnectionError as e:
            if attempt < max_retries - 1:
                print(f"Connection failed: {e}. Retrying in {retry_delay_seconds} seconds...")
                time.sleep(retry_delay_seconds)
            else:
                print("Failed to connect to RabbitMQ after multiple retries. Exiting thread.")
                raise

    if connection is None:
        return

    channel = connection.channel()
    channel.queue_declare(queue='notify_queue', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='notify_queue', on_message_callback=callback)
    print("Waiting for a response from RabbitMQ...")
    channel.start_consuming()

threading.Thread(target=consume, daemon=True).start()
