"""
src/utils/stream_utils.py - Streaming utilities for CFPB complaint data

This module provides utilities for simulating and processing streaming CFPB complaint data.
It includes functions for creating streaming sources, processing streaming data,
and integrating with the embedding and clustering pipeline.

Importance:
    - Enables simulation of real-time complaint data
    - Provides tools for processing streaming data
    - Connects batch and streaming components of the system

Main Functions:
    - create_streaming_source: Creates a streaming source from static data
    - process_streaming_batch: Processes a batch of streaming data
    - simulate_streaming: Simulates a stream of complaints
    - stream_to_kafka: Streams data to Kafka
    - stream_from_kafka: Reads streaming data from Kafka

Mathematical Aspects:
    - Implements windowing operations for time-based processing
    - Applies rate limiting for realistic simulation
    - Uses exponential backoff for error handling

Time Complexity:
    - Batch processing: O(b) where b is the batch size
    - Stream simulation: O(n) where n is the number of complaints
    - Kafka operations: O(1) per message

Space Complexity:
    - O(w) where w is the window size
    - O(b) for batch processing

Dependencies:
    - pyspark.sql: For DataFrame operations
    - pyspark.streaming: For streaming operations
    - kafka-python: For Kafka integration
"""

import os
import time
import json
import random
from typing import Dict, List, Tuple, Optional, Union, Callable, Any
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

def create_streaming_source(
    spark: SparkSession,
    data_path: str,
    schema: Optional[StructType] = None,
    format_type: str = "parquet"
) -> DataFrame:
    """
    Creates a streaming source from static data.
    
    This function sets up a streaming source from static data stored on disk,
    which can be used to simulate a real-time data stream.
    
    Parameters:
    -----------
    spark : SparkSession
        The active Spark session
    data_path : str
        Path to the data files
    schema : StructType, optional
        Schema for the streaming data
    format_type : str
        Format of the data files ('parquet', 'csv', 'json')
        
    Returns:
    --------
    DataFrame
        Streaming DataFrame
        
    Notes:
    ------
    - Creates a streaming source that can be used with Spark Structured Streaming
    - Supports various file formats
    - Can infer schema or use a provided schema
    """
    # Create streaming DataFrame
    stream_df = spark.readStream \
        .format(format_type) \
        .option("maxFilesPerTrigger", 1)  # Process one file per batch
    
    # Apply schema if provided
    if schema:
        stream_df = stream_df.schema(schema)
    
    # Load data
    stream_df = stream_df.load(data_path)
    
    return stream_df

def process_streaming_batch(
    batch_df: DataFrame,
    process_func: Callable[[DataFrame], DataFrame],
    timestamp_col: str = "timestamp"
) -> DataFrame:
    """
    Processes a batch of streaming data.
    
    This function applies a processing function to a batch of streaming data,
    handling timestamps and watermarking.
    
    Parameters:
    -----------
    batch_df : DataFrame
        Batch of streaming data
    process_func : Callable[[DataFrame], DataFrame]
        Function to process the batch
    timestamp_col : str
        Name of the timestamp column
        
    Returns:
    --------
    DataFrame
        Processed batch
        
    Notes:
    ------
    - Converts timestamp column to proper format
    - Applies watermarking for late data handling
    - Calls the provided processing function
    """
    # Ensure timestamp column exists
    if timestamp_col not in batch_df.columns:
        batch_df = batch_df.withColumn(
            timestamp_col,
            F.unix_timestamp(F.current_timestamp()) * 1000
        )
    
    # Convert timestamp to proper format
    batch_df = batch_df.withColumn(
        timestamp_col,
        F.to_timestamp(F.col(timestamp_col) / 1000)
    )
    
    # Apply watermarking
    batch_df = batch_df.withWatermark(timestamp_col, "1 hour")
    
    # Apply processing function
    processed_df = process_func(batch_df)
    
    return processed_df

def simulate_streaming(
    df: DataFrame,
    output_path: str,
    batch_size: int = 10,
    interval_seconds: float = 1.0,
    max_batches: Optional[int] = None,
    timestamp_col: str = "timestamp"
) -> None:
    """
    Simulates a stream of complaints.
    
    This function simulates a real-time stream by writing batches of data
    to disk at specified intervals.
    
    Parameters:
    -----------
    df : DataFrame
        DataFrame containing the data to stream
    output_path : str
        Path to write the streaming data
    batch_size : int
        Number of records per batch
    interval_seconds : float
        Time interval between batches in seconds
    max_batches : int, optional
        Maximum number of batches to generate
    timestamp_col : str
        Name of the timestamp column
        
    Notes:
    ------
    - Creates a directory structure for streaming data
    - Writes batches of data at specified intervals
    - Updates timestamps to simulate real-time data
    """
    # Convert to Pandas for easier manipulation
    pdf = df.toPandas()
    
    # Create output directory
    os.makedirs(output_path, exist_ok=True)
    
    # Calculate number of batches
    n_records = len(pdf)
    n_batches = (n_records + batch_size - 1) // batch_size
    
    if max_batches:
        n_batches = min(n_batches, max_batches)
    
    print(f"Simulating {n_batches} batches of {batch_size} records each")
    
    # Generate batches
    for i in range(n_batches):
        # Get batch data
        start_idx = i * batch_size
        end_idx = min(start_idx + batch_size, n_records)
        batch_pdf = pdf.iloc[start_idx:end_idx].copy()
        
        # Update timestamp to current time
        if timestamp_col in batch_pdf.columns:
            current_time = datetime.now().timestamp() * 1000
            batch_pdf[timestamp_col] = current_time
        
        # Write batch to disk
        batch_path = f"{output_path}/batch_{i:05d}.parquet"
        batch_pdf.to_parquet(batch_path, index=False)
        
        print(f"Wrote batch {i+1}/{n_batches} to {batch_path}")
        
        # Sleep before next batch
        if i < n_batches - 1:
            time.sleep(interval_seconds)
    
    print(f"Finished simulating {n_batches} batches")

def stream_to_kafka(
    df: DataFrame,
    bootstrap_servers: str,
    topic: str,
    key_col: Optional[str] = None,
    value_cols: Optional[List[str]] = None,
    interval_seconds: float = 1.0,
    max_records: Optional[int] = None
) -> None:
    """
    Streams data to Kafka.
    
    This function streams data from a DataFrame to a Kafka topic,
    simulating a real-time data source.
    
    Parameters:
    -----------
    df : DataFrame
        DataFrame containing the data to stream
    bootstrap_servers : str
        Kafka bootstrap servers (comma-separated)
    topic : str
        Kafka topic to write to
    key_col : str, optional
        Column to use as message key
    value_cols : List[str], optional
        Columns to include in the message value
    interval_seconds : float
        Time interval between messages in seconds
    max_records : int, optional
        Maximum number of records to stream
        
    Notes:
    ------
    - Requires kafka-python package
    - Converts DataFrame rows to JSON messages
    - Simulates real-time streaming with specified interval
    """
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("kafka-python package not found. Please install it with 'pip install kafka-python'")
        return
    
    # Convert to Pandas for easier manipulation
    pdf = df.toPandas()
    
    # Limit records if specified
    if max_records:
        pdf = pdf.head(max_records)
    
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None
    )
    
    # Select columns for message value
    if value_cols:
        value_pdf = pdf[value_cols]
    else:
        value_pdf = pdf
    
    # Stream records
    n_records = len(pdf)
    print(f"Streaming {n_records} records to Kafka topic '{topic}'")
    
    for i, (_, row) in enumerate(pdf.iterrows()):
        # Create message
        key = row[key_col] if key_col else None
        value = value_pdf.iloc[i].to_dict()
        
        # Send message
        producer.send(topic, key=key, value=value)
        
        # Print progress
        if (i + 1) % 100 == 0 or i == n_records - 1:
            print(f"Streamed {i+1}/{n_records} records")
        
        # Sleep before next message
        if i < n_records - 1:
            time.sleep(interval_seconds)
    
    # Flush and close producer
    producer.flush()
    producer.close()
    
    print(f"Finished streaming {n_records} records to Kafka")

def stream_from_kafka(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    schema: Optional[StructType] = None,
    starting_offsets: str = "latest"
) -> DataFrame:
    """
    Reads streaming data from Kafka.
    
    This function creates a streaming DataFrame that reads from a Kafka topic,
    which can be used with Spark Structured Streaming.
    
    Parameters:
    -----------
    spark : SparkSession
        The active Spark session
    bootstrap_servers : str
        Kafka bootstrap servers (comma-separated)
    topic : str
        Kafka topic to read from
    schema : StructType, optional
        Schema for the message value
    starting_offsets : str
        Where to start reading from ('latest', 'earliest', or JSON string)
        
    Returns:
    --------
    DataFrame
        Streaming DataFrame
        
    Notes:
    ------
    - Requires spark-sql-kafka package
    - Returns a streaming DataFrame with columns 'key', 'value', 'topic', 'partition', 'offset', 'timestamp'
    - Can parse JSON values if schema is provided
    """
    # Create streaming DataFrame
    stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", starting_offsets) \
        .load()
    
    # Parse JSON values if schema is provided
    if schema:
        stream_df = stream_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
        stream_df = stream_df.select(
            "key",
            F.from_json(F.col("value"), schema).alias("parsed_value"),
            "timestamp"
        )
        stream_df = stream_df.select("key", "timestamp", "parsed_value.*")
    
    return stream_df

def create_memory_stream(
    spark: SparkSession,
    data: List[Dict[str, Any]],
    schema: StructType,
    name: str = "memory_stream"
) -> DataFrame:
    """
    Creates an in-memory streaming source.
    
    This function creates a streaming source from in-memory data,
    which is useful for testing and development.
    
    Parameters:
    -----------
    spark : SparkSession
        The active Spark session
    data : List[Dict[str, Any]]
        List of dictionaries containing the data
    schema : StructType
        Schema for the streaming data
    name : str
        Name for the memory stream
        
    Returns:
    --------
    DataFrame
        Streaming DataFrame
        
    Notes:
    ------
    - Creates a temporary view that can be used as a streaming source
    - Useful for testing streaming pipelines without external dependencies
    - Data is stored in memory
    """
    # Create static DataFrame
    static_df = spark.createDataFrame(data, schema)
    
    # Create temporary view
    static_df.createOrReplaceTempView(name)
    
    # Create streaming DataFrame
    stream_df = spark.readStream \
        .format("memory") \
        .schema(schema) \
        .option("name", name) \
        .load()
    
    return stream_df

def write_stream_to_console(
    stream_df: DataFrame,
    output_mode: str = "append",
    truncate: bool = False,
    num_rows: int = 20,
    processing_time: str = "5 seconds"
) -> None:
    """
    Writes a streaming DataFrame to the console.
    
    This function starts a streaming query that writes the results to the console,
    which is useful for debugging and development.
    
    Parameters:
    -----------
    stream_df : DataFrame
        Streaming DataFrame to write
    output_mode : str
        Output mode ('append', 'complete', or 'update')
    truncate : bool
        Whether to truncate output
    num_rows : int
        Number of rows to show
    processing_time : str
        Trigger interval
        
    Notes:
    ------
    - Starts a streaming query that runs until terminated
    - Useful for debugging streaming pipelines
    - Shows output in the console
    """
    # Start streaming query
    query = stream_df.writeStream \
        .outputMode(output_mode) \
        .format("console") \
        .option("truncate", truncate) \
        .option("numRows", num_rows) \
        .trigger(processingTime=processing_time) \
        .start()
    
    # Wait for termination
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        print("Streaming query stopped")

def write_stream_to_memory(
    stream_df: DataFrame,
    query_name: str,
    output_mode: str = "append",
    processing_time: str = "5 seconds"
) -> None:
    """
    Writes a streaming DataFrame to memory.
    
    This function starts a streaming query that writes the results to memory,
    which can be queried using SQL.
    
    Parameters:
    -----------
    stream_df : DataFrame
        Streaming DataFrame to write
    query_name : str
        Name for the in-memory table
    output_mode : str
        Output mode ('append', 'complete', or 'update')
    processing_time : str
        Trigger interval
        
    Notes:
    ------
    - Starts a streaming query that runs until terminated
    - Results can be queried using SQL with the specified name
    - Useful for interactive analysis of streaming data
    """
    # Start streaming query
    query = stream_df.writeStream \
        .outputMode(output_mode) \
        .format("memory") \
        .queryName(query_name) \
        .trigger(processingTime=processing_time) \
        .start()
    
    # Wait for termination
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        print("Streaming query stopped")

def write_stream_to_parquet(
    stream_df: DataFrame,
    output_path: str,
    checkpoint_path: str,
    partition_cols: Optional[List[str]] = None,
    output_mode: str = "append",
    processing_time: str = "5 seconds"
) -> None:
    """
    Writes a streaming DataFrame to Parquet files.
    
    This function starts a streaming query that writes the results to Parquet files,
    which is useful for persisting streaming data.
    
    Parameters:
    -----------
    stream_df : DataFrame
        Streaming DataFrame to write
    output_path : str
        Path to write the Parquet files
    checkpoint_path : str
        Path for checkpointing
    partition_cols : List[str], optional
        Columns to partition by
    output_mode : str
        Output mode ('append', 'complete', or 'update')
    processing_time : str
        Trigger interval
        
    Notes:
    ------
    - Starts a streaming query that runs until terminated
    - Writes results to Parquet files
    - Uses checkpointing for fault tolerance
    """
    # Create output directory
    os.makedirs(output_path, exist_ok=True)
    os.makedirs(checkpoint_path, exist_ok=True)
    
    # Configure writer
    writer = stream_df.writeStream \
        .outputMode(output_mode) \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime=processing_time)
    
    # Add partitioning if specified
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    # Start streaming query
    query = writer.start()
    
    # Wait for termination
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        print("Streaming query stopped") 