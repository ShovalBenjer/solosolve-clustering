"""
Mission 5: Streaming Infrastructure Setup

This script implements the Kafka infrastructure setup for the CFPB Complaint Processing System.
It configures Kafka topics, sets up checkpointing mechanisms, and creates a simulation
producer to generate streaming data at the required rate (10,000 complaints per minute).
"""

import os
import json
import time
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import pandas as pd
from pyspark.sql import SparkSession

# Add parent directory to path for imports
sys.path.append(str(Path(os.path.dirname(os.path.abspath(__file__))).parent))

# Import project modules
from src import config
from src.spark_utils import create_spark_session
from src.logging_utils import setup_logging
from src.data_utils import load_historical_data, get_sample_data

# Set up logging
logger = setup_logging(log_level=config.LOG_LEVEL)

def create_kafka_admin_client() -> AdminClient:
    """
    Create a Kafka AdminClient for topic management.
    
    Returns:
        AdminClient: Configured Kafka admin client
    """
    kafka_config = {
        'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS
    }
    return AdminClient(kafka_config)

def create_kafka_topics(admin_client: AdminClient, topics: List[Dict]) -> None:
    """
    Create Kafka topics if they don't exist.
    
    Args:
        admin_client (AdminClient): Kafka admin client
        topics (List[Dict]): List of topic configurations
    """
    logger.info(f"Creating Kafka topics: {', '.join([t['name'] for t in topics])}")
    
    # Get existing topics
    existing_topics = admin_client.list_topics(timeout=10).topics
    
    # Create new topic objects
    new_topics = []
    for topic in topics:
        if topic['name'] not in existing_topics:
            logger.info(f"Creating new topic: {topic['name']}")
            new_topics.append(NewTopic(
                topic['name'],
                num_partitions=topic['partitions'],
                replication_factor=topic['replication'],
                config=topic.get('config', {})
            ))
        else:
            logger.info(f"Topic {topic['name']} already exists")
    
    # Create topics
    if new_topics:
        futures = admin_client.create_topics(new_topics)
        
        # Wait for topic creation to complete
        for topic, future in futures.items():
            try:
                future.result()  # Wait for completion
                logger.info(f"Topic {topic} created successfully")
            except KafkaException as e:
                logger.error(f"Failed to create topic {topic}: {e}")

def configure_kafka_topics() -> None:
    """
    Configure Kafka topics for the complaint processing system.
    """
    admin_client = create_kafka_admin_client()
    
    # Define topics to create
    topics = [
        {
            'name': config.KAFKA_INPUT_TOPIC,
            'partitions': 3,
            'replication': 1,
            'config': {
                'retention.ms': '604800000',  # 7 days retention
                'cleanup.policy': 'delete',
                'compression.type': 'lz4'
            }
        },
        {
            'name': config.KAFKA_OUTPUT_TOPIC,
            'partitions': 3,
            'replication': 1,
            'config': {
                'retention.ms': '604800000',  # 7 days retention
                'cleanup.policy': 'delete',
                'compression.type': 'lz4'
            }
        }
    ]
    
    create_kafka_topics(admin_client, topics)

def configure_checkpoint_location() -> None:
    """
    Configure checkpoint directory for Spark Structured Streaming.
    """
    logger.info(f"Setting up checkpoint location at {config.CHECKPOINT_LOCATION}")
    
    # Create checkpoint directory if it doesn't exist
    os.makedirs(config.CHECKPOINT_LOCATION, exist_ok=True)
    
    # Create subdirectories for different streaming jobs
    os.makedirs(config.CHECKPOINT_LOCATION / "ingestion", exist_ok=True)
    os.makedirs(config.CHECKPOINT_LOCATION / "processing", exist_ok=True)
    os.makedirs(config.CHECKPOINT_LOCATION / "output", exist_ok=True)
    
    logger.info("Checkpoint directories created successfully")

class KafkaComplaintProducer:
    """
    Kafka producer for simulating real-time complaint submissions.
    """
    
    def __init__(self, bootstrap_servers: str, topic: str, batch_size: int = 100):
        """
        Initialize the Kafka producer.
        
        Args:
            bootstrap_servers (str): Kafka bootstrap servers
            topic (str): Target Kafka topic
            batch_size (int): Number of messages to send in each batch
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.batch_size = batch_size
        self.producer = None
        self.running = False
        
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'complaint-producer',
            'linger.ms': 100,
            'batch.size': 65536,
            'compression.type': 'lz4',
            'acks': '1'
        }
        
        self.producer = Producer(self.producer_config)
        logger.info(f"Kafka producer initialized for topic {topic}")
    
    def delivery_callback(self, err, msg):
        """
        Callback function for message delivery.
        
        Args:
            err: Error object (None if no error)
            msg: Delivered message
        """
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            pass  # Successful delivery, no need to log every message
    
    def send_complaint(self, complaint: Dict) -> None:
        """
        Send a single complaint to Kafka.
        
        Args:
            complaint (Dict): Complaint data to send
        """
        try:
            # Add timestamp for tracking
            complaint['_kafka_timestamp'] = datetime.now().timestamp()
            
            # Convert to JSON
            complaint_json = json.dumps(complaint)
            
            # Send to Kafka
            self.producer.produce(
                self.topic,
                value=complaint_json.encode('utf-8'),
                callback=self.delivery_callback
            )
            
        except Exception as e:
            logger.error(f"Error sending complaint to Kafka: {e}")
    
    def send_complaints_batch(self, complaints: List[Dict]) -> None:
        """
        Send a batch of complaints to Kafka.
        
        Args:
            complaints (List[Dict]): List of complaints to send
        """
        for complaint in complaints:
            self.send_complaint(complaint)
            
        # Flush to ensure all messages are sent
        self.producer.flush()
    
    def simulate_complaint_stream(
        self, 
        data_source: str,
        complaints_per_minute: int = 10000,
        duration_minutes: int = 5,
        progress_interval: int = 10
    ) -> None:
        """
        Simulate a continuous stream of complaints at the specified rate.
        
        Args:
            data_source (str): Path to the data source file
            complaints_per_minute (int): Target rate of complaints per minute
            duration_minutes (int): Duration to run the simulation (in minutes)
            progress_interval (int): Interval (in seconds) to log progress
        """
        logger.info(f"Starting complaint stream simulation at {complaints_per_minute} complaints/minute")
        logger.info(f"Simulation will run for {duration_minutes} minutes")
        
        try:
            # Load test data
            spark = create_spark_session()
            df = load_historical_data(spark, data_source)
            complaints = df.toJSON().collect()
            complaints = [json.loads(c) for c in complaints]
            
            # Calculate timing parameters
            total_complaints = complaints_per_minute * duration_minutes
            complaints_per_second = complaints_per_minute / 60
            sleep_time = 1 / complaints_per_second * self.batch_size
            
            logger.info(f"Loaded {len(complaints)} complaints from {data_source}")
            logger.info(f"Sending at rate of {complaints_per_second:.2f} complaints/second")
            logger.info(f"Batch size: {self.batch_size}, Sleep time between batches: {sleep_time:.4f} seconds")
            
            # Start simulation
            self.running = True
            start_time = time.time()
            last_progress_time = start_time
            sent_count = 0
            
            # If we have fewer complaints than needed, we'll cycle through them
            while self.running and sent_count < total_complaints:
                # Prepare batch
                batch_start = sent_count % len(complaints)
                batch_end = min(batch_start + self.batch_size, len(complaints))
                
                # Handle wrapping around the end of the array
                if batch_end - batch_start < self.batch_size and sent_count + (batch_end - batch_start) < total_complaints:
                    # Get complaints from the end and beginning of the array
                    batch = complaints[batch_start:] + complaints[:self.batch_size - (batch_end - batch_start)]
                else:
                    batch = complaints[batch_start:batch_end]
                
                # Send batch
                self.send_complaints_batch(batch)
                
                # Update count and sleep
                sent_count += len(batch)
                
                # Sleep to maintain the target rate
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
            
            # Final flush and report
            self.producer.flush()
            
            total_time = time.time() - start_time
            logger.info(f"Simulation completed: Sent {sent_count} complaints in {total_time:.2f} seconds")
            logger.info(f"Average rate: {sent_count/(total_time/60):.1f} complaints/minute")
            
        except Exception as e:
            logger.error(f"Error in complaint stream simulation: {e}")
            raise
        
        finally:
            spark.stop()
            self.running = False

def monitor_consumer_lag(group_id: str, topic: str, duration: int = 60) -> None:
    """
    Monitor consumer lag for a specific consumer group.
    
    Args:
        group_id (str): Consumer group ID to monitor
        topic (str): Topic to monitor
        duration (int): Duration to monitor in seconds
    """
    logger.info(f"Monitoring consumer lag for group {group_id} on topic {topic}")
    
    # Create admin client to get topic information
    admin_client = create_kafka_admin_client()
    
    # Configure consumer
    consumer_config = {
        'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'auto.offset.reset': 'latest'
    }
    
    try:
        # Create consumer
        consumer = Consumer(consumer_config)
        
        # Subscribe to topic
        consumer.subscribe([topic])
        
        # Monitor for the specified duration
        end_time = time.time() + duration
        while time.time() < end_time:
            # Poll for messages (but don't actually process them)
            msg = consumer.poll(timeout=1.0)
            
            # Get consumer lag
            assignment = consumer.assignment()
            for tp in assignment:
                # Get committed offset
                committed = consumer.committed([tp])[0]
                # Get end offset (latest message)
                end_offsets = consumer.get_watermark_offsets(tp)
                
                if committed and end_offsets:
                    lag = end_offsets[1] - committed.offset
                    logger.info(f"Consumer lag for {group_id} on {tp.topic}:{tp.partition}: {lag} messages")
            
            time.sleep(5)  # Check every 5 seconds
    
    except Exception as e:
        logger.error(f"Error monitoring consumer lag: {e}")
    
    finally:
        # Clean up consumer
        consumer.close()

def run_mission():
    """
    Main function to run Mission 5.
    """
    logger.info("Starting Mission 5: Streaming Infrastructure Setup")
    
    try:
        # Step 1: Configure Kafka topics
        configure_kafka_topics()
        
        # Step 2: Set up checkpoint location
        configure_checkpoint_location()
        
        # Step 3: Create and run simulation producer
        producer = KafkaComplaintProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            topic=config.KAFKA_INPUT_TOPIC,
            batch_size=100
        )
        
        # Run a short simulation to verify setup
        producer.simulate_complaint_stream(
            data_source=config.TEST_DATA_PATH,
            complaints_per_minute=1000,  # Lower rate for testing
            duration_minutes=1,
            progress_interval=5
        )
        
        # Step 4: Check consumer lag (placeholder)
        logger.info("Note: To monitor consumer lag, start a Spark streaming job and then run:")
        logger.info(f"monitor_consumer_lag('{config.KAFKA_GROUP_ID}', '{config.KAFKA_INPUT_TOPIC}')")
        
        logger.info("Mission 5 completed successfully")
        
        return True
    
    except Exception as e:
        logger.error(f"Mission 5 failed: {str(e)}")
        return False

if __name__ == "__main__":
    run_mission() 
