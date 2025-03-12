"""
Python backend for consuming processed CFPB complaints from Kafka and storing them in MySQL.
This allows Superset to query the data for visualization.
"""
import sys
import json
import time
import signal
from pathlib import Path
import threading
from datetime import datetime

from kafka import KafkaConsumer
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

# Add parent directory to Python path to import config module
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_OUTPUT_TOPIC,
    KAFKA_GROUP_ID,
    DB_URI,
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)

# Flag to control the consumer loop
running = True

def signal_handler(sig, frame):
    """Handle Ctrl+C to gracefully shut down."""
    global running
    print("Shutting down...")
    running = False

def setup_database():
    """
    Set up the MySQL database connection and ensure the table exists.
    
    Returns:
        engine: SQLAlchemy engine for database operations
    """
    try:
        # Connect using mysql.connector to create database if it doesn't exist
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        
        # Create database if it doesn't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        
        # Switch to the database
        cursor.execute(f"USE {DB_NAME}")
        
        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_complaints (
            id INT AUTO_INCREMENT PRIMARY KEY,
            complaint_id VARCHAR(255) UNIQUE,
            narrative TEXT NOT NULL,
            embedding JSON,
            cluster_id INT NOT NULL,
            resolution VARCHAR(255),
            timely_response BOOLEAN,
            consumer_disputed BOOLEAN,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Create indexes for faster queries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cluster_id ON processed_complaints(cluster_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON processed_complaints(created_at)")
        
        # Create view for dashboard
        cursor.execute("""
        CREATE OR REPLACE VIEW recent_complaints AS
        SELECT 
            cluster_id,
            resolution,
            COUNT(*) AS complaint_count,
            SUM(CASE WHEN timely_response = 1 THEN 1 ELSE 0 END) AS timely_responses,
            SUM(CASE WHEN consumer_disputed = 1 THEN 1 ELSE 0 END) AS disputed_complaints
        FROM processed_complaints
        WHERE created_at > DATE_SUB(NOW(), INTERVAL 1 HOUR)
        GROUP BY cluster_id, resolution
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Create SQLAlchemy engine
        engine = create_engine(DB_URI)
        print(f"Connected to MySQL database: {DB_NAME}")
        return engine
    
    except Error as e:
        print(f"Error setting up database: {e}")
        sys.exit(1)

def store_complaint(engine, complaint):
    """
    Store a complaint in the MySQL database.
    
    Args:
        engine: SQLAlchemy engine
        complaint: Complaint data dictionary
    """
    try:
        # Convert embedding array to JSON
        embedding = complaint.get('embedding', [])
        if isinstance(embedding, list):
            embedding_json = json.dumps(embedding)
        else:
            embedding_json = json.dumps([])
        
        # Create DataFrame for the complaint
        df = pd.DataFrame([{
            'complaint_id': complaint.get('complaint_id', ''),
            'narrative': complaint.get('text', ''),
            'embedding': embedding_json,
            'cluster_id': complaint.get('cluster_id', -1),
            'resolution': complaint.get('resolution', 'Uncategorized'),
            'timely_response': complaint.get('timely_response', False),
            'consumer_disputed': complaint.get('consumer_disputed', False)
        }])
        
        # Insert into MySQL, replace if exists
        df.to_sql('processed_complaints', engine, if_exists='append', index=False)
        print(f"Stored complaint {complaint.get('complaint_id')} in MySQL")
    
    except Exception as e:
        print(f"Error storing complaint: {e}")

def consume_kafka_messages():
    """
    Consume messages from Kafka output topic and store them in MySQL.
    """
    try:
        # Create database engine
        engine = setup_database()
        
        # Create Kafka consumer
        consumer = KafkaConsumer(
            KAFKA_OUTPUT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        print(f"Connected to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"Consuming from topic: {KAFKA_OUTPUT_TOPIC}")
        
        # Process messages
        for message in consumer:
            if not running:
                break
            
            complaint = message.value
            print(f"Received complaint: {complaint.get('complaint_id')}")
            
            # Store in MySQL
            store_complaint(engine, complaint)
        
        # Clean up
        consumer.close()
        print("Kafka consumer closed")
    
    except Exception as e:
        print(f"Error in Kafka consumer: {e}")

def start_consumer_thread():
    """Start Kafka consumer in a separate thread."""
    consumer_thread = threading.Thread(target=consume_kafka_messages)
    consumer_thread.daemon = True
    consumer_thread.start()
    return consumer_thread

def main():
    """Main entry point for the backend application."""
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("Starting CFPB Complaint Backend")
    
    # Start consumer thread
    consumer_thread = start_consumer_thread()
    
    # Keep the main thread alive
    try:
        while running:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Interrupted by user")
    
    # Wait for consumer thread to finish
    consumer_thread.join(timeout=5)
    print("Backend shutdown complete")

if __name__ == "__main__":
    main() 