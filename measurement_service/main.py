from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel
import threading
import requests
import pika
import json
import os
import sqlite3
from datetime import datetime
import time

app = FastAPI(title="Measurement Service")

SERVICE_NAME = "measurement_service"
SERVICE_HOST = "measurement_service"
SERVICE_PORT = 8003
CONSUL_HOST = "consul"
CONSUL_PORT = 8500
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
DB_PATH = "measurements.db"

# RabbitMQ queue names and settings for Transactional Outbox
NOTIFY_QUEUE = "notify_queue"
DLX_NAME = "measurement_dlx"
DLQ_NAME = "measurement_dlq"
MESSAGE_TTL_MS = 30000  # 30 seconds TTL for messages

# SAGA queue names for child_profile_service transaction
SAGA_QUEUE = "profile_update_queue"
SAGA_DLX_NAME = "profile_saga_dlx"
SAGA_DLQ_NAME = "profile_saga_dlq"

def init_database():
    """Initialize SQLite database with measurements and outbox tables for transactional outbox pattern"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Main measurements table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            child_id INTEGER NOT NULL,
            height REAL NOT NULL,
            timestamp TEXT NOT NULL,
            status TEXT DEFAULT 'pending'
        )
    """)
    
    # Outbox table for transactional outbox pattern - stores events before publishing to RabbitMQ
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            published INTEGER DEFAULT 0,
            created_at TEXT NOT NULL
        )
    """)
    
    conn.commit()
    conn.close()
    print("Database initialized successfully")

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

def publish_outbox_events():
    """Background task: publishes unpublished events from outbox to RabbitMQ (Transactional Outbox pattern)"""
    max_retries = 10
    retry_delay = 5
    
    # Connect to RabbitMQ with retry logic
    for attempt in range(max_retries):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
            channel = connection.channel()
            # Ensure queue declaration matches consumer (notification_service) including DLX
            channel.queue_declare(
                queue=NOTIFY_QUEUE,
                durable=True,
                arguments={
                    'x-dead-letter-exchange': DLX_NAME,
                    'x-dead-letter-routing-key': DLQ_NAME,
                },
            )
            print("Outbox publisher connected to RabbitMQ")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Failed to connect to RabbitMQ (attempt {attempt + 1}/{max_retries}): {e}. Retrying...")
                time.sleep(retry_delay)
            else:
                print(f"Failed to connect to RabbitMQ after {max_retries} attempts. Outbox publisher stopped.")
                return
    
    # Poll outbox for unpublished events every 5 seconds
    while True:
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            # Fetch unpublished events (published=0)
            cursor.execute("""
                SELECT id, payload FROM outbox 
                WHERE published = 0 
                ORDER BY id ASC
                LIMIT 10
            """)
            
            unpublished_events = cursor.fetchall()
            
            # Publish each event to RabbitMQ
            for event_id, payload_json in unpublished_events:
                try:
                    # Publish to RabbitMQ with persistent delivery
                    channel.basic_publish(
                        exchange='',
                        routing_key='notify_queue',
                        body=payload_json,
                        properties=pika.BasicProperties(
                            delivery_mode=2,  # Make message persistent
                        )
                    )
                    
                    # Mark event as published in outbox
                    cursor.execute("UPDATE outbox SET published = 1 WHERE id = ?", (event_id,))
                    
                    # Update measurement status to 'sent'
                    payload = json.loads(payload_json)
                    measurement_id = payload.get("measurement_id")
                    if measurement_id:
                        cursor.execute("""
                            UPDATE measurements 
                            SET status = 'sent' 
                            WHERE id = ?
                        """, (measurement_id,))
                    
                    conn.commit()
                    print(f"Published outbox event {event_id} to RabbitMQ")
                    
                except Exception as e:
                    print(f"Error publishing event {event_id}: {e}")
                    conn.rollback()  # Rollback on error to maintain consistency
            
            conn.close()
            
        except Exception as e:
            print(f"Error in outbox publisher: {e}")
        
        time.sleep(5)  # Poll interval

def compensate_saga_failure(ch, method, properties, body):
    """
    SAGA compensation: rollback measurement when child_profile_service fails.
    This is called when message arrives in DLQ (timeout or error).
    """
    try:
        data = json.loads(body)
        measurement_id = data.get("measurement_id")
        child_id = data.get("child_id")
        
        print(f"SAGA Compensation: Received failed transaction for measurement_id={measurement_id}")
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Rollback: delete measurement
        cursor.execute("DELETE FROM measurements WHERE id = ?", (measurement_id,))
        
        # Also mark related outbox events as cancelled if any
        cursor.execute("""
            UPDATE outbox 
            SET published = -1 
            WHERE payload LIKE ? AND published = 0
        """, (f'%"measurement_id":{measurement_id}%',))
        
        conn.commit()
        conn.close()
        
        print(f"SAGA Compensation: Rolled back measurement_id={measurement_id} (deleted)")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        print(f"SAGA Compensation: Error during rollback: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def consume_saga_compensation():
    """
    Background task: listen to SAGA DLQ for failed transactions and perform compensation.
    This implements the compensation part of SAGA pattern.
    """
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
            channel = connection.channel()
            
            # Declare DLX and DLQ
            channel.exchange_declare(exchange=SAGA_DLX_NAME, exchange_type='direct', durable=True)
            channel.queue_declare(queue=SAGA_DLQ_NAME, durable=True)
            channel.queue_bind(exchange=SAGA_DLX_NAME, queue=SAGA_DLQ_NAME, routing_key=SAGA_DLQ_NAME)
            
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=SAGA_DLQ_NAME, on_message_callback=compensate_saga_failure)
            print("SAGA Compensation listener connected to RabbitMQ DLQ")
            channel.start_consuming()
            break
            
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Failed to connect to RabbitMQ for compensation (attempt {attempt + 1}/{max_retries}): {e}. Retrying...")
                time.sleep(retry_delay)
            else:
                print(f"Failed to connect to RabbitMQ after {max_retries} attempts. SAGA compensation stopped.")
                return

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle: initialize DB and start background threads"""
    init_database()
    threading.Thread(target=register_service, daemon=True).start()
    # Start outbox publisher background thread (Transactional Outbox)
    threading.Thread(target=publish_outbox_events, daemon=True).start()
    # Start SAGA compensation listener (for rollback on failures)
    threading.Thread(target=consume_saga_compensation, daemon=True).start()
    yield

app = FastAPI(lifespan=lifespan)

class MeasurementRequest(BaseModel):
    child_id: int
    height: float
    force_saga_error: bool = False  # For testing SAGA compensation

@app.get("/health")
def health():
    return {"status": "ok", "service": "measurement_service"}

@app.get("/db/check")
def check_database():
    try:
        db_exists = os.path.exists(DB_PATH)
        db_size = os.path.getsize(DB_PATH) if db_exists else 0
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='measurements'")
        measurements_exists = cursor.fetchone() is not None
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='outbox'")
        outbox_exists = cursor.fetchone() is not None
        
        tables_info = {}
        if measurements_exists:
            cursor.execute("PRAGMA table_info(measurements)")
            tables_info["measurements"] = [{"name": row[1], "type": row[2]} for row in cursor.fetchall()]
        
        if outbox_exists:
            cursor.execute("PRAGMA table_info(outbox)")
            tables_info["outbox"] = [{"name": row[1], "type": row[2]} for row in cursor.fetchall()]
        
        conn.close()
        
        return {
            "status": "ok",
            "database_file": DB_PATH,
            "file_exists": db_exists,
            "file_size_bytes": db_size,
            "tables": {
                "measurements": measurements_exists,
                "outbox": outbox_exists
            },
            "table_structure": tables_info
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/db/data")
def get_database_data():
    """Debug endpoint: view all data from measurements and outbox tables"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row  # Return rows as dictionaries
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM measurements ORDER BY id DESC")
        measurements = [dict(row) for row in cursor.fetchall()]
        
        cursor.execute("SELECT * FROM outbox ORDER BY id DESC")
        outbox_events = []
        for row in cursor.fetchall():
            row_dict = dict(row)
            # Parse JSON payload for easier viewing
            try:
                row_dict["payload_parsed"] = json.loads(row_dict["payload"])
            except:
                pass
            outbox_events.append(row_dict)
        
        conn.close()
        
        return {
            "status": "ok",
            "measurements": {
                "count": len(measurements),
                "data": measurements
            },
            "outbox": {
                "count": len(outbox_events),
                "unpublished_count": sum(1 for e in outbox_events if e["published"] == 0),  # Count unpublished events
                "data": outbox_events
            }
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/measure")
def measure(request: MeasurementRequest):
    """
    Create measurement using Transactional Outbox pattern:
    - Save measurement to DB
    - Save event to outbox (unpublished)
    - Both operations in single transaction for atomicity
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        timestamp = datetime.now().isoformat()
        
        # Step 1: Insert measurement into DB
        cursor.execute("""
            INSERT INTO measurements (child_id, height, timestamp, status)
            VALUES (?, ?, ?, ?)
        """, (request.child_id, request.height, timestamp, 'pending'))
        
        measurement_id = cursor.lastrowid
        
        # Step 2: Insert event into outbox (transactional outbox pattern)
        event_payload = {
            "measurement_id": measurement_id,
            "child_id": request.child_id,
            "height": request.height,
            "timestamp": timestamp,
            "task": "send_notification",
            "text": f"Measurement complete: child {request.child_id}, height {request.height}cm"
        }
        
        cursor.execute("""
            INSERT INTO outbox (event_type, payload, published, created_at)
            VALUES (?, ?, ?, ?)
        """, (
            "measurement_created",
            json.dumps(event_payload),
            0,  # unpublished - will be published by background task
            timestamp
        ))
        
        # Commit transaction - both measurement and outbox event saved atomically
        conn.commit()
        
        # Step 3: Publish SAGA event to child_profile_service (async transaction)
        try:
            saga_connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
            saga_channel = saga_connection.channel()
            
            # Declare SAGA DLX and DLQ
            saga_channel.exchange_declare(exchange=SAGA_DLX_NAME, exchange_type='direct', durable=True)
            saga_channel.queue_declare(queue=SAGA_DLQ_NAME, durable=True)
            saga_channel.queue_bind(exchange=SAGA_DLX_NAME, queue=SAGA_DLQ_NAME, routing_key=SAGA_DLQ_NAME)
            
            # Declare SAGA queue with DLX
            saga_channel.queue_declare(
                queue=SAGA_QUEUE,
                durable=True,
                arguments={
                    'x-dead-letter-exchange': SAGA_DLX_NAME,
                    'x-dead-letter-routing-key': SAGA_DLQ_NAME
                }
            )
            
            # Publish SAGA event with TTL
            saga_payload = {
                "measurement_id": measurement_id,
                "child_id": request.child_id,
                "height": request.height,
                "timestamp": timestamp,
                "force_error": request.force_saga_error  # For testing compensation
            }
            
            saga_channel.basic_publish(
                exchange='',
                routing_key=SAGA_QUEUE,
                body=json.dumps(saga_payload),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Persistent
                    expiration=str(MESSAGE_TTL_MS)  # TTL for compensation
                )
            )
            
            saga_connection.close()
            print(f"SAGA: Published profile update event for measurement_id={measurement_id}")
        except Exception as e:
            print(f"SAGA: Error publishing event: {e}")
            # Continue - compensation will handle if needed
        
        return {
            "status": "created",
            "measurement_id": measurement_id,
            "message": "Measurement saved. Event queued in outbox for publishing. SAGA transaction started."
        }
        
    except Exception as e:
        if conn:
            conn.rollback()  # Rollback on error to maintain data consistency
        return {"status": "error", "message": str(e)}, 500
    finally:
        if conn:
            conn.close()
