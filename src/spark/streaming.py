"""
Spark Structured Streaming application for processing CFPB complaints.
This script consumes complaint data from Kafka, processes it with ModernBERT embeddings,
performs HDBSCAN clustering, and publishes the results to another Kafka topic.
"""
import sys
import json
import numpy as np
import pandas as pd
import hdbscan
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, pandas_udf, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, 
    LongType, ArrayType, DoubleType, IntegerType
)
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.ml.linalg import Vectors, VectorUDT

# Import Spark NLP
from sparknlp.base import *
from sparknlp.annotator import *

# Add parent directory to Python path to import config module
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_INPUT_TOPIC,
    KAFKA_OUTPUT_TOPIC,
    SPARK_APP_NAME,
    SPARK_MASTER,
    SPARK_CHECKPOINT_DIR,
    SPARK_BATCH_DURATION,
    SPARK_WATERMARK_DELAY,
    BERT_MODEL_NAME,
    BERT_BATCH_SIZE,
    BERT_MAX_SEQ_LENGTH,
    HDBSCAN_MIN_CLUSTER_SIZE,
    HDBSCAN_METRIC,
    LSH_BUCKET_LENGTH,
    LSH_NUM_HASH_TABLES,
    RESOLUTION_MAPPING
)

# Define the schema for the input Kafka messages
input_schema = StructType([
    StructField("complaint_id", StringType(), True),
    StructField("date_received", StringType(), True),
    StructField("product", StringType(), True),
    StructField("issue", StringType(), True),
    StructField("consumer_complaint_narrative", StringType(), True),
    StructField("company", StringType(), True),
    StructField("state", StringType(), True),
    StructField("timely_response", BooleanType(), True),
    StructField("consumer_disputed", BooleanType(), True),
    StructField("timestamp", LongType(), True)
])

def create_spark_session():
    """Create and configure a Spark session."""
    return (SparkSession.builder
            .appName(SPARK_APP_NAME)
            .master(SPARK_MASTER)
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,"
                    "com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .getOrCreate())

def create_bert_pipeline():
    """Create a pipeline for BERT embeddings."""
    document_assembler = DocumentAssembler() \
        .setInputCol("consumer_complaint_narrative") \
        .setOutputCol("document")

    tokenizer = Tokenizer() \
        .setInputCols(["document"]) \
        .setOutputCol("token")

    # Use a smaller, faster BERT model for local execution
    bert_embeddings = BertEmbeddings.pretrained(BERT_MODEL_NAME, "en") \
        .setInputCols(["document", "token"]) \
        .setOutputCol("embeddings") \
        .setBatchSize(BERT_BATCH_SIZE) \
        .setMaxSentenceLength(BERT_MAX_SEQ_LENGTH)

    return Pipeline(stages=[document_assembler, tokenizer, bert_embeddings])

@pandas_udf(ArrayType(DoubleType()))
def extract_bert_embeddings(embeddings_col):
    """
    Extract BERT embeddings from Spark NLP output.
    
    Args:
        embeddings_col: Column with embeddings from Spark NLP
        
    Returns:
        Pandas Series with embeddings as numpy arrays
    """
    def extract(emb_json):
        if not emb_json:
            return np.zeros(768)  # Default empty embedding
        # Parse the embeddings JSON and get the first token embedding
        try:
            emb_data = json.loads(emb_json)
            if isinstance(emb_data, list) and len(emb_data) > 0:
                return np.array(emb_data[0]['embeddings'])
            return np.zeros(768)
        except:
            return np.zeros(768)
    
    return pd.Series([extract(emb) for emb in embeddings_col])

def to_vector(embeddings_col):
    """Convert embeddings array to Spark ML Vector."""
    return embeddings_col.rdd.map(
        lambda row: (row['complaint_id'], Vectors.dense(row['embedding']))
    ).toDF(['complaint_id', 'vector'])

@pandas_udf(IntegerType())
def cluster_partition(vectors):
    """
    Apply HDBSCAN clustering to a partition of vectors.
    
    Args:
        vectors: Pandas Series with vectors
        
    Returns:
        Pandas Series with cluster IDs
    """
    if len(vectors) < HDBSCAN_MIN_CLUSTER_SIZE:
        return pd.Series([0] * len(vectors))  # All in one cluster if too few samples
    
    try:
        # Convert vectors to numpy array
        vectors_np = np.array([v.toArray() for v in vectors])
        
        # Apply HDBSCAN
        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=min(HDBSCAN_MIN_CLUSTER_SIZE, len(vectors) // 2), 
            metric=HDBSCAN_METRIC
        )
        cluster_labels = clusterer.fit_predict(vectors_np)
        
        # Convert -1 (noise) to positive cluster IDs starting from max_cluster + 1
        if -1 in cluster_labels:
            max_cluster = max(cluster_labels)
            cluster_labels = np.where(cluster_labels == -1, max_cluster + 1, cluster_labels)
        
        return pd.Series(cluster_labels.astype(int))
    except Exception as e:
        print(f"Clustering error: {e}")
        return pd.Series([0] * len(vectors))  # Default cluster on error

def get_resolution(cluster_id):
    """Map cluster IDs to resolutions."""
    return RESOLUTION_MAPPING.get(cluster_id % len(RESOLUTION_MAPPING), "Uncategorized")

def process_batch(batch_df, batch_id):
    """
    Process each batch of streaming data.
    
    Args:
        batch_df: DataFrame containing the current batch of data
        batch_id: ID of the current batch
    """
    if batch_df.isEmpty():
        print(f"Batch {batch_id} is empty")
        return
    
    print(f"Processing batch {batch_id} with {batch_df.count()} records")
    
    try:
        # Apply BERT model to get embeddings
        bert_pipeline = create_bert_pipeline()
        embedding_df = bert_pipeline.fit(batch_df).transform(batch_df)
        
        # Extract embeddings as array
        embedding_df = embedding_df.selectExpr(
            "complaint_id", 
            "consumer_complaint_narrative as text",
            "to_json(embeddings) as embeddings_json",
            "timely_response",
            "consumer_disputed"
        )
        
        # Convert embeddings to vector format
        embedding_df = embedding_df.withColumn(
            "embedding", 
            extract_bert_embeddings(col("embeddings_json"))
        )
        
        # Convert to vector format for LSH
        vector_df = to_vector(embedding_df)
        
        # Apply LSH for efficient partitioning
        brp = BucketedRandomProjectionLSH(
            inputCol="vector", 
            outputCol="hashes", 
            bucketLength=LSH_BUCKET_LENGTH, 
            numHashTables=LSH_NUM_HASH_TABLES
        )
        lsh_model = brp.fit(vector_df)
        partitioned_df = lsh_model.transform(vector_df)
        
        # Join back with original data
        partitioned_df = partitioned_df.join(
            embedding_df, 
            on="complaint_id", 
            how="inner"
        )
        
        # Apply clustering
        clustered_df = partitioned_df.withColumn(
            "cluster_id", 
            cluster_partition(col("vector"))
        )
        
        # Add resolution based on cluster_id
        # Using a UDF would be better but for simplicity we'll map a few example resolutions
        cluster_to_resolution = {
            0: "Pending Review",
            1: "Closed with Explanation",
            2: "Closed with Monetary Relief",
            3: "Closed with Non-Monetary Relief",
            4: "Closed without Relief",
            5: "Closed with Explanation",
            6: "Uncategorized"
        }
        
        for cluster_id, resolution in cluster_to_resolution.items():
            clustered_df = clustered_df.withColumn(
                "resolution", 
                lit(resolution).where(col("cluster_id") == cluster_id).otherwise(col("resolution"))
            )
        
        # Set a default resolution for any unmapped clusters
        clustered_df = clustered_df.withColumn(
            "resolution", 
            col("resolution").when(col("resolution").isNull(), "Uncategorized")
        )
        
        # Select columns for output
        output_df = clustered_df.select(
            "complaint_id", 
            "text", 
            "embedding", 
            "cluster_id", 
            "resolution", 
            "timely_response", 
            "consumer_disputed"
        )
        
        # Write to Kafka output topic
        (output_df.selectExpr(
            "complaint_id as key",
            """
            to_json(struct(
                complaint_id,
                text,
                embedding,
                cluster_id,
                resolution,
                timely_response,
                consumer_disputed
            )) as value
            """
        ).write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", KAFKA_OUTPUT_TOPIC)
        .save())
        
        print(f"Batch {batch_id} processed successfully")
    
    except Exception as e:
        print(f"Error processing batch {batch_id}: {e}")

def main():
    """Main entry point for the Spark streaming application."""
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"Created Spark session with app name: {SPARK_APP_NAME}")
    
    # Create data stream from Kafka
    kafka_stream = (spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .load())
    
    # Parse JSON data using the defined schema
    parsed_stream = kafka_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    json_stream = parsed_stream.select(
        from_json(col("value"), input_schema).alias("data")
    ).select("data.*")
    
    # Add watermark for windowed operations
    windowed_stream = json_stream.withWatermark("timestamp", SPARK_WATERMARK_DELAY)
    
    # Process each batch
    query = (windowed_stream
        .writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", SPARK_CHECKPOINT_DIR)
        .trigger(processingTime=f"{SPARK_BATCH_DURATION} seconds")
        .start())
    
    # Wait for query to terminate
    query.awaitTermination()

if __name__ == "__main__":
    main() 