"""
End-to-End Streaming System Runner

This script runs the entire streaming inference system end-to-end,
including the Kafka producer, real-time inference system, and monitoring tools.
"""

import os
import sys
import time
import argparse
import logging
import multiprocessing
import signal
import subprocess
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(os.path.dirname(os.path.abspath(__file__))).parent))

# Import project modules
from src import config
from src.logging_utils import setup_logging

# Set up logging
logger = setup_logging(log_level=config.LOG_LEVEL)

def run_kafka_producer(rate, duration, batch_size):
    """
    Run the Kafka producer to generate simulated complaint data.
    
    Args:
        rate (int): Complaints per minute
        duration (int): Duration in minutes
        batch_size (int): Batch size for sending complaints
    """
    logger.info(f"Starting Kafka producer (rate={rate}/min, duration={duration} min)")
    
    cmd = [
        sys.executable,
        "src/kafka_complaint_producer.py",
        "--rate", str(rate),
        "--duration", str(duration),
        "--batch-size", str(batch_size)
    ]
    
    return subprocess.Popen(cmd)

def run_inference_system():
    """
    Run the real-time inference system.
    
    Returns:
        subprocess.Popen: Process running the inference system
    """
    logger.info("Starting real-time inference system")
    
    cmd = [
        sys.executable,
        "src/mission_6_realtime_inference.py"
    ]
    
    return subprocess.Popen(cmd)

def run_monitoring(duration):
    """
    Run various monitoring tools.
    
    Args:
        duration (int): Monitoring duration in seconds
    """
    logger.info(f"Starting monitoring tools (duration={duration} sec)")
    
    # Wait a bit for the system to start processing
    time.sleep(10)
    
    # Run throughput monitoring
    logger.info("Running throughput monitoring")
    throughput_cmd = [
        sys.executable,
        "src/inference_monitor.py",
        "throughput",
        "--duration", str(min(duration, 60))
    ]
    subprocess.run(throughput_cmd)
    
    # Run prediction distribution monitoring
    logger.info("Running prediction distribution monitoring")
    distribution_cmd = [
        sys.executable,
        "src/inference_monitor.py",
        "distribution",
        "--duration", str(min(duration, 60))
    ]
    subprocess.run(distribution_cmd)
    
    # Collect sample outputs
    logger.info("Collecting sample outputs")
    samples_cmd = [
        sys.executable,
        "src/inference_monitor.py",
        "samples",
        "--limit", "5",
        "--compact"
    ]
    subprocess.run(samples_cmd)

def run_end_to_end(rate=1000, duration=5, batch_size=100, monitoring=True):
    """
    Run the end-to-end streaming system.
    
    Args:
        rate (int): Complaints per minute
        duration (int): Duration in minutes
        batch_size (int): Batch size for sending complaints
        monitoring (bool): Whether to run monitoring tools
    """
    logger.info(f"Starting end-to-end streaming system (rate={rate}/min, duration={duration} min)")
    
    try:
        # Start inference system
        inference_process = run_inference_system()
        logger.info("Inference system started")
        
        # Wait a bit for the inference system to initialize
        time.sleep(5)
        
        # Start Kafka producer
        producer_process = run_kafka_producer(rate, duration, batch_size)
        logger.info("Kafka producer started")
        
        # Run monitoring tools if requested
        if monitoring:
            monitoring_duration = duration * 60  # Convert to seconds
            run_monitoring(monitoring_duration)
        
        # Wait for producer to complete
        producer_process.wait()
        logger.info("Kafka producer completed")
        
        # Wait a bit for the inference system to finish processing
        logger.info("Waiting for inference system to complete processing...")
        time.sleep(10)
        
        # Terminate inference system
        logger.info("Terminating inference system")
        inference_process.terminate()
        inference_process.wait()
        
        logger.info("End-to-end streaming system run completed successfully")
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down...")
        if 'inference_process' in locals():
            inference_process.terminate()
        if 'producer_process' in locals():
            producer_process.terminate()
    
    except Exception as e:
        logger.error(f"Error running end-to-end streaming system: {str(e)}")
        if 'inference_process' in locals():
            inference_process.terminate()
        if 'producer_process' in locals():
            producer_process.terminate()
        raise

def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='End-to-End Streaming System Runner')
    
    parser.add_argument(
        '--rate',
        type=int,
        default=1000,
        help='Complaints per minute (default: 1000)'
    )
    
    parser.add_argument(
        '--duration',
        type=int,
        default=5,
        help='Duration in minutes (default: 5)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Batch size for sending complaints (default: 100)'
    )
    
    parser.add_argument(
        '--no-monitoring',
        action='store_true',
        help='Disable monitoring tools'
    )
    
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()
    
    # Run end-to-end system
    run_end_to_end(
        rate=args.rate,
        duration=args.duration,
        batch_size=args.batch_size,
        monitoring=not args.no_monitoring
    ) 