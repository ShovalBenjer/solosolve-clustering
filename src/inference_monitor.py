"""
Inference Monitoring Script

This script provides tools to monitor the real-time inference system,
track performance metrics, and analyze prediction distributions.
"""

import os
import sys
import time
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from confluent_kafka import Consumer, TopicPartition, KafkaException
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, json_tuple, explode, window, count, avg, max, min, expr

# Add parent directory to path for imports
sys.path.append(str(Path(os.path.dirname(os.path.abspath(__file__))).parent))

# Import project modules
from src import config
from src.logging_utils import setup_logging
from src.spark_utils import create_spark_session
from src.schema import complaint_schema

# Set up logging
logger = setup_logging(log_level=config.LOG_LEVEL)

def setup_consumer(topic, group_id=None, reset_offset=True):
    """
    Set up a Kafka consumer for monitoring.
    
    Args:
        topic (str): Kafka topic to consume
        group_id (str, optional): Consumer group ID
        reset_offset (bool): Whether to reset offset to earliest
        
    Returns:
        Consumer: Configured Kafka consumer
    """
    # Generate a unique group ID if not provided
    if group_id is None:
        group_id = f"monitor-{datetime.now().timestamp()}"
    
    # Configure consumer
    consumer_config = {
        'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'auto.offset.reset': 'earliest' if reset_offset else 'latest',
        'enable.auto.commit': 'true',
        'auto.commit.interval.ms': '5000'
    }
    
    # Create consumer
    consumer = Consumer(consumer_config)
    
    # Subscribe to topic
    consumer.subscribe([topic])
    
    return consumer

def collect_output_samples(
    topic=config.KAFKA_OUTPUT_TOPIC,
    limit=10,
    timeout=30,
    compact=False
):
    """
    Collect sample outputs from the Kafka output topic.
    
    Args:
        topic (str): Kafka topic to consume
        limit (int): Maximum number of messages to collect
        timeout (int): Maximum time to wait for messages (seconds)
        compact (bool): Whether to show compact output
        
    Returns:
        list: List of collected messages
    """
    logger.info(f"Collecting samples from {topic}")
    
    # Set up consumer
    consumer = setup_consumer(topic)
    
    # Collect messages
    messages = []
    start_time = time.time()
    
    try:
        while len(messages) < limit and time.time() - start_time < timeout:
            # Poll for messages
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            # Parse message
            try:
                value = json.loads(msg.value().decode('utf-8'))
                
                # Add metadata
                value['_offset'] = msg.offset()
                value['_partition'] = msg.partition()
                value['_topic'] = msg.topic()
                
                # Compact output if requested
                if compact and isinstance(value, dict):
                    # Keep only key fields
                    key_fields = [
                        'Complaint ID', 'Product', 'Issue', 'prediction',
                        'escalate', 'urgency_level', 'priority_routing'
                    ]
                    value = {k: v for k, v in value.items() if k in key_fields}
                
                messages.append(value)
                logger.info(f"Collected sample {len(messages)}/{limit}")
            
            except Exception as e:
                logger.error(f"Error parsing message: {e}")
        
        if len(messages) < limit:
            logger.warning(f"Timeout reached, collected only {len(messages)}/{limit} samples")
        
        return messages
    
    finally:
        consumer.close()

def calculate_processing_latency(results, time_field='processing_timestamp', kafka_field='_kafka_timestamp'):
    """
    Calculate processing latency from Kafka timestamps.
    
    Args:
        results (list): List of result messages
        time_field (str): Field containing processing timestamp
        kafka_field (str): Field containing Kafka timestamp
        
    Returns:
        float: Average processing latency in seconds
    """
    latencies = []
    
    for result in results:
        if isinstance(result, dict) and time_field in result and kafka_field in result:
            # Parse timestamps
            try:
                proc_time = result[time_field]
                kafka_time = result[kafka_field]
                
                # Handle different timestamp formats
                if isinstance(proc_time, str):
                    proc_time = datetime.fromisoformat(proc_time.replace('Z', '+00:00'))
                    proc_time = proc_time.timestamp()
                
                # Calculate latency
                latency = proc_time - kafka_time
                latencies.append(latency)
            
            except Exception as e:
                logger.error(f"Error calculating latency: {e}")
    
    # Return average latency
    if latencies:
        avg_latency = sum(latencies) / len(latencies)
        logger.info(f"Average processing latency: {avg_latency:.2f} seconds")
        return avg_latency
    else:
        logger.warning("No valid timestamps found for latency calculation")
        return None

def monitor_prediction_distribution(topic=config.KAFKA_OUTPUT_TOPIC, duration=60):
    """
    Monitor prediction distribution in real-time.
    
    Args:
        topic (str): Kafka topic to monitor
        duration (int): Monitoring duration in seconds
    """
    logger.info(f"Monitoring prediction distribution from {topic} for {duration} seconds")
    
    # Set up consumer
    consumer = setup_consumer(topic)
    
    # Track predictions
    predictions = []
    start_time = time.time()
    
    try:
        # Monitor for specified duration
        while time.time() - start_time < duration:
            # Poll for messages
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            # Parse message
            try:
                value = json.loads(msg.value().decode('utf-8'))
                
                if isinstance(value, dict) and 'prediction' in value:
                    predictions.append(float(value['prediction']))
            
            except Exception as e:
                logger.error(f"Error parsing message: {e}")
        
        # Report results
        if predictions:
            logger.info(f"Collected {len(predictions)} predictions")
            
            # Calculate statistics
            avg_pred = sum(predictions) / len(predictions)
            min_pred = min(predictions)
            max_pred = max(predictions)
            
            logger.info(f"Prediction statistics: Avg={avg_pred:.4f}, Min={min_pred:.4f}, Max={max_pred:.4f}")
            
            # Print distribution
            bins = {
                'Low (0.0-0.3)': len([p for p in predictions if p < 0.3]),
                'Medium (0.3-0.7)': len([p for p in predictions if 0.3 <= p < 0.7]),
                'High (0.7-1.0)': len([p for p in predictions if p >= 0.7])
            }
            
            for bin_name, count in bins.items():
                percentage = count / len(predictions) * 100
                logger.info(f"{bin_name}: {count} ({percentage:.1f}%)")
            
            return predictions
        else:
            logger.warning("No predictions collected")
            return []
    
    finally:
        consumer.close()

def monitor_throughput(topic=config.KAFKA_OUTPUT_TOPIC, window_size=10, duration=60):
    """
    Monitor system throughput in real-time.
    
    Args:
        topic (str): Kafka topic to monitor
        window_size (int): Window size for throughput calculation (seconds)
        duration (int): Total monitoring duration (seconds)
    """
    logger.info(f"Monitoring throughput from {topic} for {duration} seconds")
    
    # Set up consumer
    consumer = setup_consumer(topic)
    
    # Track message counts per window
    windows = {}
    current_window = int(time.time() / window_size)
    start_time = time.time()
    
    try:
        # Monitor for specified duration
        while time.time() - start_time < duration:
            # Update current window
            now = time.time()
            window_key = int(now / window_size)
            
            if window_key not in windows:
                windows[window_key] = 0
            
            # Poll for messages with timeout to ensure we don't block too long
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            # Count message
            windows[window_key] += 1
        
        # Calculate throughput
        if windows:
            total_messages = sum(windows.values())
            elapsed_time = time.time() - start_time
            overall_rate = total_messages / elapsed_time
            
            logger.info(f"Total messages: {total_messages}")
            logger.info(f"Overall rate: {overall_rate:.2f} messages/second ({overall_rate * 60:.2f} messages/minute)")
            
            # Calculate statistics across windows
            rates = [count / window_size for count in windows.values()]
            
            if rates:
                avg_rate = sum(rates) / len(rates)
                max_rate = max(rates)
                min_rate = min(rates) if rates else 0
                
                logger.info(f"Window rates (msgs/sec): Avg={avg_rate:.2f}, Min={min_rate:.2f}, Max={max_rate:.2f}")
            
            return windows, overall_rate
        else:
            logger.warning("No messages collected")
            return {}, 0
    
    finally:
        consumer.close()

def run_spark_streaming_metrics(topic=config.KAFKA_OUTPUT_TOPIC, duration=60):
    """
    Run a Spark Streaming job to calculate real-time metrics.
    
    Args:
        topic (str): Kafka topic to analyze
        duration (int): Duration to run the analysis (seconds)
    """
    logger.info(f"Starting Spark Streaming metrics for {topic}")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Create streaming dataframe from Kafka
        kafka_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON data
        parsed_stream = kafka_stream \
            .selectExpr("CAST(value AS STRING) as json_data") \
            .select("json_data")
        
        # Extract fields for analysis
        extracted_stream = parsed_stream \
            .selectExpr(
                "json_tuple(json_data, 'prediction', 'escalate', 'urgency_level', 'priority_routing', 'Product') "
                "as (prediction, escalate, urgency_level, priority_routing, product)"
            )
        
        # Calculate metrics
        metrics_stream = extracted_stream \
            .withWatermark("timestamp", "10 seconds") \
            .groupBy(
                window(col("timestamp"), "10 seconds", "5 seconds")
            ) \
            .agg(
                count("*").alias("count"),
                avg("prediction").alias("avg_prediction"),
                count(when(col("escalate") == "Yes", 1)).alias("escalation_count"),
                count(when(col("urgency_level") == "High", 1)).alias("high_urgency_count")
            )
        
        # Start query
        query = metrics_stream \
            .writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", "false") \
            .start()
        
        # Run for specified duration
        logger.info(f"Running Spark metrics for {duration} seconds")
        query.awaitTermination(duration * 1000)
        
        # Stop query
        query.stop()
        
        logger.info("Spark metrics completed")
    
    except Exception as e:
        logger.error(f"Error running Spark metrics: {e}")
    
    finally:
        spark.stop()

def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Inference Monitoring Tools')
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Sample output command
    sample_parser = subparsers.add_parser('samples', help='Collect sample outputs')
    sample_parser.add_argument(
        '--topic',
        type=str,
        default=config.KAFKA_OUTPUT_TOPIC,
        help='Kafka topic to sample'
    )
    sample_parser.add_argument(
        '--limit',
        type=int,
        default=10,
        help='Maximum number of samples to collect'
    )
    sample_parser.add_argument(
        '--timeout',
        type=int,
        default=30,
        help='Maximum time to wait for samples (seconds)'
    )
    sample_parser.add_argument(
        '--compact',
        action='store_true',
        help='Show compact output'
    )
    
    # Latency command
    latency_parser = subparsers.add_parser('latency', help='Calculate processing latency')
    latency_parser.add_argument(
        '--topic',
        type=str,
        default=config.KAFKA_OUTPUT_TOPIC,
        help='Kafka topic to analyze'
    )
    latency_parser.add_argument(
        '--limit',
        type=int,
        default=100,
        help='Maximum number of messages to analyze'
    )
    
    # Distribution command
    distribution_parser = subparsers.add_parser('distribution', help='Monitor prediction distribution')
    distribution_parser.add_argument(
        '--topic',
        type=str,
        default=config.KAFKA_OUTPUT_TOPIC,
        help='Kafka topic to monitor'
    )
    distribution_parser.add_argument(
        '--duration',
        type=int,
        default=60,
        help='Monitoring duration (seconds)'
    )
    
    # Throughput command
    throughput_parser = subparsers.add_parser('throughput', help='Monitor system throughput')
    throughput_parser.add_argument(
        '--topic',
        type=str,
        default=config.KAFKA_OUTPUT_TOPIC,
        help='Kafka topic to monitor'
    )
    throughput_parser.add_argument(
        '--window',
        type=int,
        default=10,
        help='Window size for throughput calculation (seconds)'
    )
    throughput_parser.add_argument(
        '--duration',
        type=int,
        default=60,
        help='Monitoring duration (seconds)'
    )
    
    # Spark metrics command
    spark_parser = subparsers.add_parser('spark-metrics', help='Run Spark Streaming metrics')
    spark_parser.add_argument(
        '--topic',
        type=str,
        default=config.KAFKA_OUTPUT_TOPIC,
        help='Kafka topic to analyze'
    )
    spark_parser.add_argument(
        '--duration',
        type=int,
        default=60,
        help='Duration to run the analysis (seconds)'
    )
    
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()
    
    # Execute the requested command
    if args.command == 'samples':
        samples = collect_output_samples(
            topic=args.topic,
            limit=args.limit,
            timeout=args.timeout,
            compact=args.compact
        )
        print(json.dumps(samples, indent=2))
    
    elif args.command == 'latency':
        samples = collect_output_samples(
            topic=args.topic,
            limit=args.limit,
            timeout=args.limit * 2
        )
        calculate_processing_latency(samples)
    
    elif args.command == 'distribution':
        monitor_prediction_distribution(
            topic=args.topic,
            duration=args.duration
        )
    
    elif args.command == 'throughput':
        monitor_throughput(
            topic=args.topic,
            window_size=args.window,
            duration=args.duration
        )
    
    elif args.command == 'spark-metrics':
        run_spark_streaming_metrics(
            topic=args.topic,
            duration=args.duration
        )
    
    else:
        # If no command or invalid command, print help
        print("Please specify a valid command. Use --help for more information.") 