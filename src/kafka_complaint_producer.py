"""
Kafka Complaint Producer Script

This script simulates real-time complaint submissions by sending data to Kafka
at a controlled rate (configurable, default 10,000 complaints/minute).
It can be run independently for testing the streaming infrastructure.
"""

import os
import json
import time
import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime

from confluent_kafka import Producer
import pandas as pd

# Add parent directory to path for imports
sys.path.append(str(Path(os.path.dirname(os.path.abspath(__file__))).parent))

# Import project modules
from src import config
from src.logging_utils import setup_logging
from src.spark_utils import create_spark_session
from src.data_utils import load_historical_data

# Set up logging
logger = setup_logging(log_level=config.LOG_LEVEL)

def delivery_callback(err, msg):
    """
    Callback function for Kafka message delivery.
    
    Args:
        err: Error object (None if no error)
        msg: Delivered message
    """
    if err:
        logger.error(f"Message delivery failed: {err}")

def setup_producer(bootstrap_servers):
    """
    Set up Kafka producer with optimized settings.
    
    Args:
        bootstrap_servers (str): Kafka bootstrap servers
        
    Returns:
        Producer: Configured Kafka producer
    """
    producer_config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'complaint-producer',
        'linger.ms': 100,  # Wait up to 100ms to batch messages
        'batch.size': 65536,  # 64KB batch size
        'compression.type': 'lz4',  # Use LZ4 compression
        'acks': '1'  # Wait for leader acknowledgment
    }
    
    return Producer(producer_config)

def load_complaint_data(source_path, spark=None):
    """
    Load complaint data from source file.
    
    Args:
        source_path (str): Path to data source
        spark (SparkSession, optional): Spark session, will create if None
        
    Returns:
        list: List of complaint records as dictionaries
    """
    logger.info(f"Loading complaint data from {source_path}")
    
    # Create Spark session if not provided
    close_spark = False
    if spark is None:
        spark = create_spark_session()
        close_spark = True
    
    try:
        # Load data using Spark
        df = load_historical_data(spark, source_path)
        complaints = df.toJSON().collect()
        complaints = [json.loads(c) for c in complaints]
        
        logger.info(f"Loaded {len(complaints)} complaints")
        return complaints
    
    finally:
        if close_spark:
            spark.stop()

def run_producer(
    complaints,
    topic,
    bootstrap_servers,
    complaints_per_minute=10000,
    duration_minutes=5,
    batch_size=100,
    progress_interval=10
):
    """
    Run the Kafka producer simulation.
    
    Args:
        complaints (list): List of complaint records
        topic (str): Kafka topic to produce to
        bootstrap_servers (str): Kafka bootstrap servers
        complaints_per_minute (int): Target complaints per minute
        duration_minutes (int): Duration to run in minutes
        batch_size (int): Number of complaints to send in each batch
        progress_interval (int): Seconds between progress logging
    """
    # Set up Kafka producer
    producer = setup_producer(bootstrap_servers)
    
    # Calculate timing parameters
    total_complaints = complaints_per_minute * duration_minutes
    complaints_per_second = complaints_per_minute / 60
    sleep_time = 1 / complaints_per_second * batch_size
    
    logger.info(f"Starting complaint producer simulation:")
    logger.info(f"  Topic: {topic}")
    logger.info(f"  Rate: {complaints_per_minute} complaints/minute")
    logger.info(f"  Duration: {duration_minutes} minutes")
    logger.info(f"  Batch size: {batch_size}")
    logger.info(f"  Sleep between batches: {sleep_time:.4f} seconds")
    logger.info(f"  Total complaints to send: {total_complaints}")
    
    # Start simulation
    start_time = time.time()
    last_progress_time = start_time
    sent_count = 0
    
    try:
        # If we have fewer complaints than needed, we'll cycle through them
        while sent_count < total_complaints:
            # Prepare batch
            batch_start = sent_count % len(complaints)
            batch_end = min(batch_start + batch_size, len(complaints))
            
            # Handle wrapping around the end of the array
            if batch_end - batch_start < batch_size and sent_count + (batch_end - batch_start) < total_complaints:
                # Get complaints from the end and beginning of the array
                batch = complaints[batch_start:] + complaints[:batch_size - (batch_end - batch_start)]
            else:
                batch = complaints[batch_start:batch_end]
            
            # Send each complaint in batch
            for complaint in batch:
                # Add timestamp for tracking
                complaint['_kafka_timestamp'] = datetime.now().timestamp()
                
                # Convert to JSON and send
                try:
                    producer.produce(
                        topic,
                        value=json.dumps(complaint).encode('utf-8'),
                        callback=delivery_callback
                    )
                except Exception as e:
                    logger.error(f"Error producing message: {e}")
            
            # Update count and poll to handle callbacks
            sent_count += len(batch)
            producer.poll(0)  # Non-blocking poll to handle callbacks
            
            # Sleep to maintain target rate
            time.sleep(sleep_time)
            
            # Log progress periodically
            current_time = time.time()
            if current_time - last_progress_time >= progress_interval:
                elapsed_minutes = (current_time - start_time) / 60
                actual_rate = sent_count / elapsed_minutes if elapsed_minutes > 0 else 0
                logger.info(f"Progress: Sent {sent_count}/{total_complaints} complaints "
                           f"({sent_count/total_complaints*100:.1f}%) "
                           f"at {actual_rate:.1f} complaints/minute")
                last_progress_time = current_time
    
    except KeyboardInterrupt:
        logger.info("Producer interrupted, shutting down...")
    
    finally:
        # Final flush and report
        producer.flush()
        
        total_time = time.time() - start_time
        logger.info(f"Simulation completed: Sent {sent_count} complaints in {total_time:.2f} seconds")
        logger.info(f"Average rate: {sent_count/(total_time/60):.1f} complaints/minute")

def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Kafka Complaint Producer Simulation')
    
    parser.add_argument(
        '--topic',
        type=str,
        default=config.KAFKA_INPUT_TOPIC,
        help='Kafka topic to produce to'
    )
    
    parser.add_argument(
        '--bootstrap-servers',
        type=str,
        default=config.KAFKA_BOOTSTRAP_SERVERS,
        help='Kafka bootstrap servers'
    )
    
    parser.add_argument(
        '--rate',
        type=int,
        default=config.SIMULATION_COMPLAINTS_PER_MINUTE,
        help='Complaints per minute'
    )
    
    parser.add_argument(
        '--duration',
        type=int,
        default=config.SIMULATION_DEFAULT_DURATION_MINUTES,
        help='Duration in minutes'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Batch size for sending complaints'
    )
    
    parser.add_argument(
        '--data-source',
        type=str,
        default=str(config.TEST_DATA_PATH),
        help='Path to complaint data source'
    )
    
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()
    
    # Load complaint data
    complaints = load_complaint_data(args.data_source)
    
    if not complaints:
        logger.error("No complaint data loaded, exiting")
        sys.exit(1)
    
    # Run producer simulation
    run_producer(
        complaints=complaints,
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        complaints_per_minute=args.rate,
        duration_minutes=args.duration,
        batch_size=args.batch_size
    ) 