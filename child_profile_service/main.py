from fastapi import FastAPI
from pydantic import BaseModel
import requests, threading
from contextlib import asynccontextmanager
import sqlite3
import pika
import json
import os
import time

SERVICE_NAME = "child_profile_service"
SERVICE_HOST = "child_profile_service"
SERVICE_PORT = 8002
CONSUL_HOST = "consul"
CONSUL_PORT = 8500
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
DB_PATH = "child_profiles.db"

# RabbitMQ queue names for SAGA
SAGA_QUEUE = "profile_update_queue"
SAGA_DLX_NAME = "profile_saga_dlx"
SAGA_DLQ_NAME = "profile_saga_dlq"
MESSAGE_TTL_MS = 30000  # 30 seconds TTL

def init_database():
    """Initialize SQLite database for child profiles"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS child_profiles (
            child_id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER,
            last_height REAL,
            last_updated TEXT
        )
    """)
    
    # Insert sample data if empty
    cursor.execute("SELECT COUNT(*) FROM child_profiles")
    if cursor.fetchone()[0] == 0:
        cursor.execute("""
            INSERT INTO child_profiles (child_id, name, age, last_height, last_updated)
            VALUES (1, 'John', 7, 120.0, datetime('now')),
                   (2, 'Jane', 10, 140.0, datetime('now'))
        """)
    
    conn.commit()
    conn.close()
    print("Child profiles database initialized")

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
    """Application lifecycle: initialize DB and start background threads"""
    init_database()
    threading.Thread(target=register_service, daemon=True).start()
    # Start SAGA consumer background thread
    threading.Thread(target=consume_saga_events, daemon=True).start()
    yield

app = FastAPI(lifespan=lifespan)

def update_profile_from_saga(message_data):
    """
    Update child profile from SAGA message.
    This can throw an error for testing compensation.
    """
    child_id = message_data.get("child_id")
    height = message_data.get("height")
    
    if not child_id or not height:
        raise ValueError("Missing child_id or height in message")
    
    # Simulate error for testing SAGA compensation (can be triggered via query param)
    if message_data.get("force_error"):
        raise Exception("Simulated error for SAGA compensation testing")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Update or insert child profile
        cursor.execute("""
            INSERT OR REPLACE INTO child_profiles (child_id, last_height, last_updated)
            VALUES (?, ?, datetime('now'))
        """, (child_id, height))
        
        conn.commit()
        print(f"Profile updated for child_id={child_id}, height={height}")
        return True
    finally:
        conn.close()

def saga_callback(ch, method, properties, body):
    """SAGA consumer callback - processes profile update events"""
    try:
        data = json.loads(body)
        print(f"SAGA: Received profile update event: {data}")
        
        # Update profile - this can throw error for testing
        update_profile_from_saga(data)
        
        # Acknowledge message on success
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"SAGA: Profile updated successfully for child_id={data.get('child_id')}")
        
    except Exception as e:
        print(f"SAGA: Error processing profile update: {e}")
        # NACK - message will go to DLQ for compensation
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        print(f"SAGA: Message sent to DLQ for compensation")

def consume_saga_events():
    """Background task: consume SAGA events from RabbitMQ"""
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
            channel = connection.channel()
            
            # Declare DLX and DLQ for SAGA
            channel.exchange_declare(exchange=SAGA_DLX_NAME, exchange_type='direct', durable=True)
            channel.queue_declare(queue=SAGA_DLQ_NAME, durable=True)
            channel.queue_bind(exchange=SAGA_DLX_NAME, queue=SAGA_DLQ_NAME, routing_key=SAGA_DLQ_NAME)
            
            # Declare SAGA queue with DLX configuration
            channel.queue_declare(
                queue=SAGA_QUEUE,
                durable=True,
                arguments={
                    'x-dead-letter-exchange': SAGA_DLX_NAME,
                    'x-dead-letter-routing-key': SAGA_DLQ_NAME
                }
            )
            
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=SAGA_QUEUE, on_message_callback=saga_callback)
            print("SAGA consumer connected to RabbitMQ")
            channel.start_consuming()
            break
            
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Failed to connect to RabbitMQ (attempt {attempt + 1}/{max_retries}): {e}. Retrying...")
                time.sleep(retry_delay)
            else:
                print(f"Failed to connect to RabbitMQ after {max_retries} attempts. SAGA consumer stopped.")
                return

@app.get("/health")
def health():
    return {"status": "ok", "service": "child_profile_service"}

@app.get("/profiles")
def get_profiles():
    """Get all child profiles"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM child_profiles")
    profiles = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    return {
        "service": "child_profile_service",
        "profiles": profiles
    }

@app.put("/profiles/{child_id}")
def update_profile(child_id: int, height: float, force_error: bool = False):
    """
    Update child profile (for testing).
    If force_error=True, throws error to test SAGA compensation.
    """
    try:
        if force_error:
            raise Exception("Simulated error for SAGA compensation testing")
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO child_profiles (child_id, last_height, last_updated)
            VALUES (?, ?, datetime('now'))
        """, (child_id, height))
        
        conn.commit()
        conn.close()
        
        return {"status": "updated", "child_id": child_id, "height": height}
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500
