"""
Data utilities for CFPB Complaint Processing System

This module provides functions for loading, validating, and preprocessing 
CFPB complaint data.
"""

import os
import logging
import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException

from src import config
from src.schema import get_complaint_schema, get_processed_complaint_schema

# Set up logger
logger = logging.getLogger(__name__)

def load_historical_data(spark, data_path=None, schema=None):
    """
    Load historical complaint data from JSON file
    
    Args:
        spark (SparkSession): Spark session
        data_path (str, optional): Path to historical data file
        schema (StructType, optional): Schema for the data
        
    Returns:
        DataFrame: Loaded data as a Spark DataFrame
    """
    if data_path is None:
        data_path = config.HISTORICAL_DATA_PATH
    
    if schema is None:
        schema = get_complaint_schema()
    
    logger.info(f"Loading historical data from: {data_path}")
    
    try:
        # Load data with schema validation
        df = spark.read.json(str(data_path), schema=schema)
        logger.info(f"Successfully loaded data with {df.count()} records")
        return df
    except AnalysisException as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

def validate_data_quality(df):
    """
    Perform data quality checks on complaint data
    
    Args:
        df (DataFrame): DataFrame containing complaint data
        
    Returns:
        dict: Dictionary with data quality metrics
    """
    logger.info("Performing data quality checks")
    
    # Get total number of records
    total_records = df.count()
    
    # Check for missing values in key fields
    critical_fields = [
        "Date received", 
        "Product", 
        "Company", 
        "Complaint ID",
        "Company response to consumer"
    ]
    
    missing_counts = {}
    for field in critical_fields:
        missing_count = df.filter(F.col(field).isNull()).count()
        missing_pct = (missing_count / total_records) * 100 if total_records > 0 else 0
        missing_counts[field] = {
            "count": missing_count,
            "percentage": missing_pct
        }
    
    # Validate date formats
    date_fields = ["Date received", "Date sent to company"]
    invalid_dates = {}
    
    for field in date_fields:
        # Count records where the date field is not null but cannot be cast to date
        invalid_count = df.filter(
            F.col(field).isNotNull() & 
            F.to_date(F.col(field)).isNull()
        ).count()
        
        invalid_pct = (invalid_count / total_records) * 100 if total_records > 0 else 0
        invalid_dates[field] = {
            "count": invalid_count,
            "percentage": invalid_pct
        }
    
    # Check categorical values
    categorical_fields = {
        "Product": df.select("Product").distinct().count(),
        "State": df.select("State").distinct().count(),
        "Company response to consumer": df.select("Company response to consumer").distinct().collect(),
        "Timely response?": df.select("Timely response?").distinct().collect(),
        "Consumer disputed?": df.select("Consumer disputed?").distinct().collect()
    }
    
    # Compile quality metrics
    quality_metrics = {
        "total_records": total_records,
        "missing_values": missing_counts,
        "invalid_dates": invalid_dates,
        "categorical_values": categorical_fields
    }
    
    logger.info(f"Data quality check completed: {quality_metrics}")
    return quality_metrics

def preprocess_dates(df):
    """
    Preprocess date fields in the DataFrame
    
    Args:
        df (DataFrame): DataFrame containing complaint data
        
    Returns:
        DataFrame: DataFrame with processed date fields
    """
    logger.info("Preprocessing date fields")
    
    # Convert date strings to timestamps
    df_processed = df.withColumn(
        "DateReceivedTimestamp", 
        F.to_timestamp(F.col("Date received"), "MM/dd/yyyy")
    ).withColumn(
        "DateSentToCompanyTimestamp", 
        F.to_timestamp(F.col("Date sent to company"), "MM/dd/yyyy")
    )
    
    # Calculate processing time in days
    df_processed = df_processed.withColumn(
        "ProcessingTimeInDays",
        F.when(
            F.col("DateSentToCompanyTimestamp").isNotNull() & F.col("DateReceivedTimestamp").isNotNull(),
            F.datediff(F.col("DateSentToCompanyTimestamp"), F.col("DateReceivedTimestamp"))
        ).otherwise(None)
    )
    
    return df_processed

def engineer_target_variable(df):
    """
    Engineer the 'SuccessfulResolution' target variable
    
    Args:
        df (DataFrame): DataFrame containing complaint data
        
    Returns:
        DataFrame: DataFrame with engineered target variable
    """
    logger.info("Engineering target variable")
    
    # Define successful resolution based on:
    # 1. Company response was positive
    # 2. Response was timely
    # 3. Consumer did not dispute the resolution
    
    positive_responses = [
        "Closed with explanation", 
        "Closed with monetary relief", 
        "Closed with non-monetary relief"
    ]
    
    df_with_target = df.withColumn(
        "SuccessfulResolution",
        (F.col("Company response to consumer").isin(positive_responses)) &
        (F.col("Timely response?") == "Yes") &
        ((F.col("Consumer disputed?") == "No") | F.col("Consumer disputed?").isNull())
    )
    
    # Count successful vs unsuccessful resolutions
    resolution_counts = df_with_target.groupBy("SuccessfulResolution").count()
    resolution_counts.show()
    
    return df_with_target

def split_train_test_data(df, test_ratio=0.25, seed=42):
    """
    Split data into training and test sets
    
    Args:
        df (DataFrame): DataFrame to split
        test_ratio (float): Ratio of test data (0-1)
        seed (int): Random seed for reproducibility
        
    Returns:
        tuple: (training_data, test_data)
    """
    logger.info(f"Splitting data with test_ratio={test_ratio}")
    
    # Split data
    train_df, test_df = df.randomSplit([1.0 - test_ratio, test_ratio], seed=seed)
    
    logger.info(f"Split complete. Training set: {train_df.count()} records, Test set: {test_df.count()} records")
    
    return train_df, test_df

def save_dataframes(train_df, test_df):
    """
    Save training and test DataFrames to disk
    
    Args:
        train_df (DataFrame): Training DataFrame
        test_df (DataFrame): Test DataFrame
    """
    logger.info("Saving DataFrames to disk")
    
    # Ensure data directory exists
    os.makedirs(config.DATA_DIR, exist_ok=True)
    
    # Save training data
    train_path = config.DATA_DIR / "historical_complaints_75percent.json"
    train_df.write.json(str(train_path), mode="overwrite")
    logger.info(f"Training data saved to {train_path}")
    
    # Save test data
    test_path = config.DATA_DIR / "historical_complaints_25percent.json"
    test_df.write.json(str(test_path), mode="overwrite")
    logger.info(f"Test data saved to {test_path}")

def create_kafka_producer_simulation(df, output_path, batch_size=100):
    """
    Create a simulation script to feed data into Kafka
    
    Args:
        df (DataFrame): DataFrame containing complaint data
        output_path (str): Path to output Python script
        batch_size (int): Number of records per batch
    """
    logger.info(f"Creating Kafka producer simulation script at {output_path}")
    
    # Convert to pandas for easier manipulation in the simulation script
    sample_data = df.limit(1000).toPandas()
    
    # Create the simulation script content
    script_template = '''
"""
CFPB Complaint Kafka Producer Simulation

This script simulates real-time streaming of CFPB complaint data to Kafka.
It reads from the historical dataset and sends data to Kafka at a controlled rate.
"""
import json
import time
import pandas as pd
from kafka import KafkaProducer
import logging
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(os.path.dirname(os.path.abspath(__file__))).parent
sys.path.append(str(project_root))

# Import configuration
from src import config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_producer_simulation')

def create_kafka_producer():
    """Create and return a Kafka producer"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[config.KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info(f"Kafka producer created, connected to {config.KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {str(e)}")
        raise

def load_simulation_data():
    """Load data for simulation"""
    try:
        # Load from the test dataset
        df = pd.read_json(config.TEST_DATA_PATH, lines=True)
        logger.info(f"Loaded {len(df)} records for simulation")
        return df
    except Exception as e:
        logger.error(f"Failed to load simulation data: {str(e)}")
        raise

def send_to_kafka(producer, data, topic, batch_size=BATCH_SIZE, target_rate=10000):
    """
    Send data to Kafka topic at controlled rate
    
    Args:
        producer: Kafka producer
        data: DataFrame with complaint data
        topic: Kafka topic to send to
        batch_size: Number of records per batch
        target_rate: Target number of complaints per minute
    """
    records_sent = 0
    start_time = time.time()
    
    # Calculate sleep time to achieve target rate
    # target_rate records per minute = target_rate/60 records per second
    # For each batch, we need to sleep: batch_size / (target_rate/60) seconds
    sleep_time = batch_size / (target_rate / 60)
    
    # Process data in batches
    for i in range(0, len(data), batch_size):
        batch_start = time.time()
        
        # Get batch
        batch = data.iloc[i:i+batch_size]
        
        # Send each record in batch
        for _, record in batch.iterrows():
            # Convert to dictionary and clean NaN values
            record_dict = record.to_dict()
            record_dict = {k: (v if pd.notna(v) else None) for k, v in record_dict.items()}
            
            # Send to Kafka
            producer.send(topic, value=record_dict)
            records_sent += 1
        
        # Force sending all messages
        producer.flush()
        
        # Calculate time to sleep to maintain target rate
        batch_time = time.time() - batch_start
        if batch_time < sleep_time:
            time.sleep(sleep_time - batch_time)
        
        # Log progress
        elapsed = time.time() - start_time
        current_rate = records_sent / elapsed * 60 if elapsed > 0 else 0
        logger.info(f"Sent {records_sent} records. Current rate: {current_rate:.2f} records/minute")

def main():
    """Main function to run the simulation"""
    logger.info("Starting CFPB complaint Kafka producer simulation")
    
    try:
        # Create Kafka producer
        producer = create_kafka_producer()
        
        # Load simulation data
        data = load_simulation_data()
        
        # Send data to Kafka
        logger.info(f"Sending data to Kafka topic: {config.KAFKA_INPUT_TOPIC}")
        send_to_kafka(producer, data, config.KAFKA_INPUT_TOPIC)
        
        logger.info("Simulation completed successfully")
    except Exception as e:
        logger.error(f"Simulation failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
'''
    
    # Replace placeholder with actual batch size
    script_content = script_template.replace('BATCH_SIZE', str(batch_size))
    
    # Write to file
    with open(output_path, 'w') as f:
        f.write(script_content)
    
    logger.info(f"Kafka producer simulation script created at {output_path}")
    
    # Make the script executable
    os.chmod(output_path, 0o755)

def create_data_summary(df):
    """
    Create a summary of the data
    
    Args:
        df (DataFrame): DataFrame containing complaint data
        
    Returns:
        dict: Dictionary with data summary
    """
    logger.info("Creating data summary")
    
    # Get total number of records
    total_records = df.count()
    
    # Get summary by product
    product_summary = df.groupBy("Product").count().orderBy(F.col("count").desc())
    
    # Get summary by company
    company_summary = df.groupBy("Company").count().orderBy(F.col("count").desc())
    
    # Get summary by state
    state_summary = df.groupBy("State").count().orderBy(F.col("count").desc())
    
    # Get summary by issue
    issue_summary = df.groupBy("Issue").count().orderBy(F.col("count").desc())
    
    # Get summary by company response
    response_summary = df.groupBy("Company response to consumer").count().orderBy(F.col("count").desc())
    
    # Return summaries
    return {
        "total_records": total_records,
        "product_summary": product_summary,
        "company_summary": company_summary,
        "state_summary": state_summary,
        "issue_summary": issue_summary,
        "response_summary": response_summary
    } 