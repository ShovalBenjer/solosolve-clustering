"""
Mission 6: Real-time Inference System

This script implements the real-time inference system for the CFPB Complaint Processing System.
It reads complaint data from Kafka, applies the preprocessing pipeline and trained model
to generate predictions, adds decision logic, and writes results back to Kafka.
"""

import os
import logging
import json
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, monotonically_increasing_id,
    when, lit, current_timestamp, expr
)
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegressionModel

# Add parent directory to path for imports
sys.path.append(str(Path(os.path.dirname(os.path.abspath(__file__))).parent))

# Import project modules
from src import config
from src.spark_utils import create_spark_session
from src.logging_utils import setup_logging
from src.schema import complaint_schema, processed_schema

# Set up logging
logger = setup_logging(log_level=config.LOG_LEVEL)

def load_models():
    """
    Load the preprocessing pipeline and trained model.
    
    Returns:
        tuple: (preprocessing_pipeline, trained_model)
    """
    logger.info("Loading preprocessing pipeline and trained model")
    
    try:
        # Load preprocessing pipeline
        pipeline_path = config.PREPROCESSING_PIPELINE_PATH
        logger.info(f"Loading preprocessing pipeline from {pipeline_path}")
        preprocessing_pipeline = PipelineModel.load(str(pipeline_path))
        
        # Load trained model
        model_path = config.MODEL_PATH
        logger.info(f"Loading trained model from {model_path}")
        model = LogisticRegressionModel.load(str(model_path))
        
        logger.info("Successfully loaded preprocessing pipeline and model")
        return preprocessing_pipeline, model
    
    except Exception as e:
        logger.error(f"Error loading models: {str(e)}")
        raise

def create_kafka_stream(spark: SparkSession):
    """
    Create a streaming DataFrame from Kafka.
    
    Args:
        spark (SparkSession): Active Spark session
        
    Returns:
        DataFrame: Streaming DataFrame from Kafka
    """
    logger.info(f"Creating Kafka stream from topic {config.KAFKA_INPUT_TOPIC}")
    
    try:
        # Read from Kafka
        kafka_stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", config.KAFKA_INPUT_TOPIC) \
            .option("startingOffsets", config.STREAMING_CONFIG["startingOffsets"]) \
            .option("maxOffsetsPerTrigger", config.STREAMING_CONFIG["maxOffsetsPerTrigger"]) \
            .option("failOnDataLoss", config.STREAMING_CONFIG["failOnDataLoss"]) \
            .load()
        
        logger.info("Successfully created Kafka stream")
        return kafka_stream
    
    except Exception as e:
        logger.error(f"Error creating Kafka stream: {str(e)}")
        raise

def parse_kafka_stream(kafka_stream: DataFrame):
    """
    Parse the JSON data from Kafka stream.
    
    Args:
        kafka_stream (DataFrame): Raw Kafka stream
        
    Returns:
        DataFrame: Parsed DataFrame with complaint data
    """
    logger.info("Parsing Kafka stream data")
    
    try:
        # Extract value and parse JSON
        parsed_stream = kafka_stream \
            .selectExpr("CAST(value AS STRING) as json_data") \
            .select(from_json(col("json_data"), complaint_schema).alias("data")) \
            .select("data.*")
        
        # Add unique identifier and processing timestamp
        parsed_stream = parsed_stream \
            .withColumn("row_id", monotonically_increasing_id()) \
            .withColumn("processing_timestamp", current_timestamp())
        
        logger.info("Successfully parsed Kafka stream data")
        return parsed_stream
    
    except Exception as e:
        logger.error(f"Error parsing Kafka stream: {str(e)}")
        raise

def apply_preprocessing(parsed_stream: DataFrame, preprocessing_pipeline: PipelineModel):
    """
    Apply preprocessing pipeline to streaming data.
    
    Args:
        parsed_stream (DataFrame): Parsed streaming data
        preprocessing_pipeline (PipelineModel): Preprocessing pipeline
        
    Returns:
        DataFrame: Preprocessed streaming data
    """
    logger.info("Applying preprocessing pipeline to streaming data")
    
    try:
        # Apply preprocessing pipeline
        preprocessed_stream = preprocessing_pipeline.transform(parsed_stream)
        
        logger.info("Successfully applied preprocessing pipeline")
        return preprocessed_stream
    
    except Exception as e:
        logger.error(f"Error applying preprocessing: {str(e)}")
        raise

def apply_model(preprocessed_stream: DataFrame, model: LogisticRegressionModel):
    """
    Apply trained model to generate predictions.
    
    Args:
        preprocessed_stream (DataFrame): Preprocessed streaming data
        model (LogisticRegressionModel): Trained model
        
    Returns:
        DataFrame: DataFrame with predictions
    """
    logger.info("Applying model for inference")
    
    try:
        # Apply model
        predictions = model.transform(preprocessed_stream)
        
        logger.info("Successfully applied model for inference")
        return predictions
    
    except Exception as e:
        logger.error(f"Error applying model: {str(e)}")
        raise

def apply_decision_logic(predictions: DataFrame):
    """
    Apply business rules and decision logic based on predictions.
    
    Args:
        predictions (DataFrame): DataFrame with model predictions
        
    Returns:
        DataFrame: DataFrame with decision flags
    """
    logger.info("Applying decision logic")
    
    try:
        # Apply decision logic based on prediction score
        decisions = predictions \
            .withColumn(
                "escalate",
                when(col("prediction") > config.PREDICTION_THRESHOLD, "Yes").otherwise("No")
            ) \
            .withColumn(
                "urgency_level",
                when(col("prediction") > 0.8, "High")
                .when(col("prediction") > 0.5, "Medium")
                .otherwise("Low")
            ) \
            .withColumn(
                "priority_routing",
                when(
                    (col("Product") == "Mortgage") & (col("prediction") > 0.7), "Specialized Team"
                ).when(
                    col("prediction") > 0.9, "Executive Review"
                ).when(
                    col("prediction") > 0.6, "Priority Queue"
                ).otherwise("Standard Routing")
            ) \
            .withColumn(
                "recommended_action",
                when(
                    col("urgency_level") == "High", "Immediate Response Required"
                ).when(
                    col("urgency_level") == "Medium", "24-Hour Response Window"
                ).otherwise("Standard Handling")
            )
        
        logger.info("Successfully applied decision logic")
        return decisions
    
    except Exception as e:
        logger.error(f"Error applying decision logic: {str(e)}")
        raise

def prepare_kafka_output(decisions: DataFrame):
    """
    Prepare final output for Kafka.
    
    Args:
        decisions (DataFrame): DataFrame with predictions and decisions
        
    Returns:
        DataFrame: DataFrame formatted for Kafka output
    """
    logger.info("Preparing Kafka output")
    
    try:
        # Select columns for output
        output_columns = [
            "row_id", "Complaint ID", "Date received", "Product", "Issue",
            "Consumer complaint narrative", "Company", "State",
            "prediction", "probability", "escalate", "urgency_level",
            "priority_routing", "recommended_action", "processing_timestamp"
        ]
        
        # Prepare output
        output = decisions.select(
            [col(c) for c in output_columns if c in decisions.columns]
        )
        
        # Format for Kafka
        kafka_output = output \
            .withColumn("key", col("row_id").cast("string")) \
            .withColumn("value", to_json(struct("*")))
        
        logger.info("Successfully prepared Kafka output")
        return kafka_output.select("key", "value")
    
    except Exception as e:
        logger.error(f"Error preparing Kafka output: {str(e)}")
        raise

def start_streaming_inference(checkpoint_path=None):
    """
    Start the streaming inference process.
    
    Args:
        checkpoint_path (str, optional): Path for checkpoint location
    """
    logger.info("Starting streaming inference")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Load models
        preprocessing_pipeline, model = load_models()
        
        # Create Kafka stream
        kafka_stream = create_kafka_stream(spark)
        
        # Define processing function
        def process_batch(batch_df, batch_id):
            """
            Process each streaming batch.
            
            Args:
                batch_df (DataFrame): Batch DataFrame
                batch_id (int): Batch identifier
            """
            try:
                # Get batch size
                batch_size = batch_df.count()
                logger.info(f"Processing batch {batch_id} with {batch_size} records")
                
                if batch_size == 0:
                    logger.info(f"Empty batch {batch_id}, skipping")
                    return
                
                start_time = time.time()
                
                # Parse JSON
                parsed_df = parse_kafka_stream(batch_df)
                
                # Apply preprocessing
                preprocessed_df = apply_preprocessing(parsed_df, preprocessing_pipeline)
                
                # Apply model
                predictions_df = apply_model(preprocessed_df, model)
                
                # Apply decision logic
                decisions_df = apply_decision_logic(predictions_df)
                
                # Prepare Kafka output
                output_df = prepare_kafka_output(decisions_df)
                
                # Write to Kafka
                output_df.write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
                    .option("topic", config.KAFKA_OUTPUT_TOPIC) \
                    .save()
                
                # Log processing time and rate
                processing_time = time.time() - start_time
                records_per_second = batch_size / processing_time if processing_time > 0 else 0
                
                logger.info(f"Batch {batch_id} processed in {processing_time:.2f} seconds "
                           f"({records_per_second:.2f} records/second)")
                
            except Exception as e:
                logger.error(f"Error processing batch {batch_id}: {str(e)}")
        
        # Configure checkpoint location
        checkpoint_location = checkpoint_path or str(config.CHECKPOINT_LOCATION / "processing")
        
        # Start streaming query
        logger.info(f"Starting streaming query with checkpoint at {checkpoint_location}")
        
        streaming_query = kafka_stream \
            .writeStream \
            .foreachBatch(process_batch) \
            .option("checkpointLocation", checkpoint_location) \
            .trigger(processingTime=config.SPARK_PROCESSING_TIME) \
            .start()
        
        logger.info("Streaming query started")
        
        # Wait for query termination
        streaming_query.awaitTermination()
    
    except KeyboardInterrupt:
        logger.info("Streaming inference interrupted by user")
    
    except Exception as e:
        logger.error(f"Error in streaming inference: {str(e)}")
        raise
    
    finally:
        # Clean up
        spark.stop()
        logger.info("Streaming inference stopped")

def stream_with_error_handling():
    """
    Run streaming inference with error handling and retry logic.
    """
    max_retries = 3
    retry_count = 0
    retry_delay = 60  # seconds
    
    while retry_count < max_retries:
        try:
            start_streaming_inference()
            break  # If no exception, break the loop
        
        except Exception as e:
            retry_count += 1
            logger.error(f"Streaming inference failed (attempt {retry_count}/{max_retries}): {str(e)}")
            
            if retry_count < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error("Maximum retry attempts reached. Exiting.")
                raise

if __name__ == "__main__":
    logger.info("Starting Mission 6: Real-time Inference System")
    stream_with_error_handling() 
