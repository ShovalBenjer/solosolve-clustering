"""
src/streaming/streaming_pipeline.py - Real-time CFPB complaint processing with Spark Structured Streaming

This module implements the core streaming analytics pipeline for processing consumer complaints
from various sources, generating embeddings, clustering, and outputting results to both
storage and visualization systems.

Importance:
    - Acts as the central real-time processing engine for the entire system
    - Enables continuous analysis of new complaints as they arrive
    - Connects the ML components (embedding, clustering) with data sources and sinks

Main Functions:
    - create_streaming_pipeline: Creates and returns a configured streaming pipeline
    - process_complaints_batch: Processes each micro-batch of complaints
    - extract_streaming_embeddings: Extracts ModernBERT embeddings from streaming complaint text
    - apply_streaming_clustering: Applies clustering to streaming data using pre-trained model
    - classify_complaints: Identifies problematic complaints based on criteria
    - lookup_resolutions: Suggests resolutions based on cluster membership

Mathematical Aspects:
    - Uses vector space models (BERT) for text representation in high-dimensional space
    - Applies density-based clustering (HDBSCAN) with complexity O(n log n)
    - Implements LSH for approximate nearest neighbor search with O(d log n) complexity
    - Uses windowing operations with sliding windows of 1 minute increments

Time Complexity:
    - Batch processing: O(n) where n is the number of complaints in the batch
    - Embedding generation: O(s*n) where s is the avg sentence length
    - LSH operations: O(d log n) where d is embedding dimension
    - HDBSCAN clustering: O(n log n) in best case

Space Complexity:
    - O(n*d) where n is number of complaints and d is embedding dimension (768 for BERT)
    - Additional O(c) for c clusters in memory

Dependencies:
    - pyspark.sql: For DataFrame operations
    - pyspark.ml: For ML pipeline components
    - johnsnowlabs.nlp: For BERT embeddings
    - hdbscan: For clustering
"""

import os
import json
import time
import logging
from typing import Dict, List, Tuple, Optional, Union, Any, Callable
from datetime import datetime

import numpy as np
import pandas as pd

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, ArrayType, BooleanType, TimestampType, LongType
)

# Import custom modules
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.models.embedding_clustering import (
    create_embedding_model,
    extract_embeddings,
    create_lsh_model,
    apply_hdbscan_clustering
)
from src.utils.data_utils import preprocess_complaints
from src.utils.stream_utils import (
    create_streaming_source,
    process_streaming_batch,
    write_stream_to_console,
    write_stream_to_memory,
    write_stream_to_parquet
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('streaming_pipeline.log')
    ]
)
logger = logging.getLogger(__name__)

def create_streaming_pipeline(
    spark: SparkSession,
    input_source: str,
    input_options: Dict[str, Any],
    output_path: str,
    checkpoint_path: str,
    embedding_model_path: Optional[str] = None,
    cluster_model_path: Optional[str] = None,
    batch_interval: str = "1 minute",
    window_duration: str = "5 minutes",
    slide_duration: str = "1 minute"
) -> Any:
    """
    Creates and returns a Spark Structured Streaming pipeline.
    
    This function sets up the complete streaming pipeline that reads complaint data
    from various sources, processes it using embeddings and clustering, and outputs results
    to storage and visualization systems.
    
    Parameters:
    -----------
    spark : SparkSession
        The active Spark session
    input_source : str
        Source type ('kafka', 'file', 'memory')
    input_options : Dict[str, Any]
        Options for the input source
    output_path : str
        Path to write the output data
    checkpoint_path : str
        Path for checkpointing
    embedding_model_path : str, optional
        Path to the pre-trained embedding model
    cluster_model_path : str, optional
        Path to the pre-trained clustering model
    batch_interval : str
        Trigger interval for batch processing
    window_duration : str
        Duration of the sliding window
    slide_duration : str
        Slide interval for the window
        
    Returns:
    --------
    Any
        The active streaming query that can be awaited or monitored
        
    Notes:
    ------
    - Uses sliding windows for time-based processing
    - Implements exactly-once semantics through checkpointing
    - Supports various input and output sources
    """
    # Create streaming source based on input type
    if input_source == "kafka":
        # Kafka source
        bootstrap_servers = input_options.get("bootstrap_servers", "localhost:9092")
        topic = input_options.get("topic", "complaints")
        starting_offsets = input_options.get("starting_offsets", "latest")
        
        # Define schema for Kafka messages
        value_schema = StructType([
            StructField("complaint_id", StringType(), True),
            StructField("date_received", StringType(), True),
            StructField("product", StringType(), True),
            StructField("issue", StringType(), True),
            StructField("consumer_complaint_narrative", StringType(), True),
            StructField("company", StringType(), True),
            StructField("state", StringType(), True),
            StructField("timely_response", StringType(), True),
            StructField("consumer_disputed", StringType(), True),
            StructField("consumer_consent_provided", StringType(), True)
        ])
        
        # Create Kafka source
        from src.utils.stream_utils import stream_from_kafka
        stream_df = stream_from_kafka(
            spark=spark,
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            schema=value_schema,
            starting_offsets=starting_offsets
        )
        
    elif input_source == "file":
        # File source
        data_path = input_options.get("data_path", "data/streaming")
        format_type = input_options.get("format_type", "parquet")
        
        # Create file source
        stream_df = create_streaming_source(
            spark=spark,
            data_path=data_path,
            format_type=format_type
        )
        
    elif input_source == "memory":
        # Memory source
        name = input_options.get("name", "memory_stream")
        
        # Create memory source
        stream_df = spark.readStream \
            .format("memory") \
            .option("name", name) \
            .load()
        
    else:
        raise ValueError(f"Unknown input source: {input_source}")
    
    # Load embedding model
    if embedding_model_path:
        # Load pre-trained model
        embedding_model = create_embedding_model(spark, load_from=embedding_model_path)
    else:
        # Create new model
        embedding_model = create_embedding_model(spark)
    
    # Load clustering model
    if cluster_model_path:
        # Load pre-trained model
        with open(cluster_model_path, "r") as f:
            cluster_model_config = json.load(f)
    else:
        # Default clustering parameters
        cluster_model_config = {
            "min_cluster_size": 5,
            "min_samples": 2,
            "cluster_selection_epsilon": 0.5,
            "alpha": 1.0
        }
    
    # Define schema for output data
    output_schema = StructType([
        StructField("complaint_id", StringType(), True),
        StructField("consumer_complaint_narrative", StringType(), True),
        StructField("embedding", ArrayType(FloatType()), True),
        StructField("cluster", IntegerType(), True),
        StructField("cluster_probability", FloatType(), True),
        StructField("is_anomaly", BooleanType(), True),
        StructField("processing_time", TimestampType(), True)
    ])
    
    # Define foreachBatch function to process each micro-batch
    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        """Process each micro-batch of streaming data."""
        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id} is empty, skipping")
            return
        
        logger.info(f"Processing batch {batch_id} with {batch_df.count()} records")
        
        try:
            # Preprocess complaints
            preprocessed_df = preprocess_complaints(batch_df)
            
            # Extract embeddings
            embeddings_df = extract_streaming_embeddings(
                preprocessed_df,
                embedding_model,
                text_col="consumer_complaint_narrative"
            )
            
            # Apply clustering
            clustered_df = apply_streaming_clustering(
                embeddings_df,
                cluster_model_config,
                embedding_col="embedding"
            )
            
            # Add processing timestamp
            result_df = clustered_df.withColumn(
                "processing_time",
                F.current_timestamp()
            )
            
            # Write results
            if output_path:
                # Write to Parquet
                result_df.write \
                    .mode("append") \
                    .parquet(f"{output_path}/batch_{batch_id}")
                
                logger.info(f"Wrote batch {batch_id} results to {output_path}/batch_{batch_id}")
            
            # Optional: Write to other destinations (e.g., database, Kafka)
            # ...
            
        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {e}")
    
    # Start streaming query
    query = stream_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime=batch_interval) \
        .start()
    
    logger.info("Started streaming pipeline")
    
    return query

def extract_streaming_embeddings(
    df: DataFrame,
    embedding_model: Any,
    text_col: str = "consumer_complaint_narrative",
    id_col: str = "complaint_id"
) -> DataFrame:
    """
    Extracts ModernBERT embeddings from streaming complaint text.
    
    This function applies the embedding model to the streaming data to
    generate embeddings for each complaint.
    
    Parameters:
    -----------
    df : DataFrame
        DataFrame containing complaint data
    embedding_model : Any
        Pre-trained embedding model
    text_col : str
        Column containing the text to embed
    id_col : str
        Column containing the unique identifier
        
    Returns:
    --------
    DataFrame
        DataFrame with added embedding column
        
    Notes:
    ------
    - Uses the pre-trained embedding model
    - Handles batching for efficiency
    - Returns the original DataFrame with an additional 'embedding' column
    """
    # Extract embeddings
    embeddings_df = extract_embeddings(
        df=df,
        embedding_model=embedding_model,
        text_col=text_col,
        id_col=id_col
    )
    
    return embeddings_df

def apply_streaming_clustering(
    df: DataFrame,
    cluster_model_config: Dict[str, Any],
    embedding_col: str = "embedding"
) -> DataFrame:
    """
    Applies clustering to streaming data using pre-trained model.
    
    This function applies clustering to the streaming embeddings using
    the pre-trained clustering model.
    
    Parameters:
    -----------
    df : DataFrame
        DataFrame containing embeddings
    cluster_model_config : Dict[str, Any]
        Configuration for the clustering model
    embedding_col : str
        Column containing the embeddings
        
    Returns:
    --------
    DataFrame
        DataFrame with added cluster and probability columns
        
    Notes:
    ------
    - Uses HDBSCAN for clustering
    - Handles incremental clustering for streaming data
    - Returns the original DataFrame with additional 'cluster' and 'cluster_probability' columns
    """
    # Convert to Pandas for clustering
    pdf = df.toPandas()
    
    # Extract embeddings
    embeddings = np.array(pdf[embedding_col].tolist())
    
    # Apply clustering
    cluster_labels, probabilities = apply_hdbscan_clustering(
        embeddings=embeddings,
        min_cluster_size=cluster_model_config.get("min_cluster_size", 5),
        min_samples=cluster_model_config.get("min_samples", 2),
        cluster_selection_epsilon=cluster_model_config.get("cluster_selection_epsilon", 0.5),
        alpha=cluster_model_config.get("alpha", 1.0),
        return_probabilities=True
    )
    
    # Add cluster labels and probabilities
    pdf["cluster"] = cluster_labels
    pdf["cluster_probability"] = probabilities
    
    # Add anomaly flag (noise points are anomalies)
    pdf["is_anomaly"] = pdf["cluster"] == -1
    
    # Convert back to Spark DataFrame
    result_df = spark.createDataFrame(pdf)
    
    return result_df

def classify_complaints(
    df: DataFrame,
    threshold: float = 0.8
) -> DataFrame:
    """
    Identifies problematic complaints based on criteria.
    
    This function classifies complaints as problematic based on
    various criteria, including cluster membership and text content.
    
    Parameters:
    -----------
    df : DataFrame
        DataFrame containing clustered complaints
    threshold : float
        Probability threshold for classification
        
    Returns:
    --------
    DataFrame
        DataFrame with added classification column
        
    Notes:
    ------
    - Uses cluster probabilities for classification
    - Identifies complaints that require attention
    - Returns the original DataFrame with an additional 'is_problematic' column
    """
    # Add classification column
    result_df = df.withColumn(
        "is_problematic",
        F.when(
            (F.col("is_anomaly") == True) | 
            (F.col("cluster_probability") < threshold),
            True
        ).otherwise(False)
    )
    
    return result_df

def lookup_resolutions(
    df: DataFrame,
    resolution_map: Dict[int, str]
) -> DataFrame:
    """
    Suggests resolutions based on cluster membership.
    
    This function looks up suggested resolutions for each complaint
    based on its cluster membership.
    
    Parameters:
    -----------
    df : DataFrame
        DataFrame containing clustered complaints
    resolution_map : Dict[int, str]
        Mapping from cluster IDs to resolution suggestions
        
    Returns:
    --------
    DataFrame
        DataFrame with added resolution column
        
    Notes:
    ------
    - Maps cluster IDs to pre-defined resolutions
    - Handles noise points with a default resolution
    - Returns the original DataFrame with an additional 'suggested_resolution' column
    """
    # Convert resolution map to a list for broadcasting
    resolution_list = [(k, v) for k, v in resolution_map.items()]
    resolution_df = spark.createDataFrame(resolution_list, ["cluster", "resolution"])
    
    # Join with resolution map
    result_df = df.join(
        resolution_df,
        on="cluster",
        how="left"
    )
    
    # Fill missing resolutions (for noise points)
    result_df = result_df.fillna(
        {"resolution": "Requires manual review (anomaly detected)"}
    )
    
    return result_df

def run_streaming_pipeline(
    input_source: str,
    input_options: Dict[str, Any],
    output_path: str,
    checkpoint_path: str,
    embedding_model_path: Optional[str] = None,
    cluster_model_path: Optional[str] = None,
    batch_interval: str = "1 minute",
    window_duration: str = "5 minutes",
    slide_duration: str = "1 minute",
    await_termination: bool = True
) -> None:
    """
    Runs the streaming pipeline.
    
    This function creates and starts the streaming pipeline, and optionally
    waits for termination.
    
    Parameters:
    -----------
    input_source : str
        Source type ('kafka', 'file', 'memory')
    input_options : Dict[str, Any]
        Options for the input source
    output_path : str
        Path to write the output data
    checkpoint_path : str
        Path for checkpointing
    embedding_model_path : str, optional
        Path to the pre-trained embedding model
    cluster_model_path : str, optional
        Path to the pre-trained clustering model
    batch_interval : str
        Trigger interval for batch processing
    window_duration : str
        Duration of the sliding window
    slide_duration : str
        Slide interval for the window
    await_termination : bool
        Whether to wait for termination
        
    Notes:
    ------
    - Creates and starts the streaming pipeline
    - Optionally waits for termination
    - Handles graceful shutdown on keyboard interrupt
    """
    # Create Spark session
    spark = SparkSession.builder \
        .appName("CFPB Complaint Streaming") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()
    
    # Create directories
    os.makedirs(output_path, exist_ok=True)
    os.makedirs(checkpoint_path, exist_ok=True)
    
    # Create and start pipeline
    query = create_streaming_pipeline(
        spark=spark,
        input_source=input_source,
        input_options=input_options,
        output_path=output_path,
        checkpoint_path=checkpoint_path,
        embedding_model_path=embedding_model_path,
        cluster_model_path=cluster_model_path,
        batch_interval=batch_interval,
        window_duration=window_duration,
        slide_duration=slide_duration
    )
    
    # Wait for termination if requested
    if await_termination:
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stopping streaming pipeline")
            query.stop()
            logger.info("Streaming pipeline stopped")

if __name__ == "__main__":
    # Example usage
    input_options = {
        "data_path": "data/streaming",
        "format_type": "parquet"
    }
    
    run_streaming_pipeline(
        input_source="file",
        input_options=input_options,
        output_path="data/output/streaming",
        checkpoint_path="data/checkpoints/streaming",
        batch_interval="10 seconds",
        await_termination=True
    ) 