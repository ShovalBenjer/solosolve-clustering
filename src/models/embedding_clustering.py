"""
src/models/embedding_clustering.py - ModernBERT embeddings and HDBSCAN clustering with LSH optimization

This module implements the embedding generation and clustering components of the CFPB complaint
analysis system. It uses ModernBERT embeddings via Spark NLP for text representation and
HDBSCAN with LSH optimization for efficient clustering.

Importance:
    - Provides the core NLP and clustering functionality for the system
    - Enables semantic understanding of complaint narratives
    - Optimizes clustering performance through LSH-based partitioning

Main Functions:
    - create_embedding_model: Creates and returns a ModernBERT embedding model
    - extract_embeddings: Extracts embeddings from text using ModernBERT
    - create_lsh_model: Creates an LSH model for efficient partitioning
    - apply_hdbscan_clustering: Applies HDBSCAN clustering to embeddings
    - cluster_partition: Clusters a partition of embeddings (used in pandas UDF)

Mathematical Aspects:
    - Uses BERT's attention mechanism for contextual word embeddings
    - Implements LSH for approximate nearest neighbor search with O(d log n) complexity
    - Applies HDBSCAN's density-based clustering with O(n log n) complexity
    - Optimizes memory usage through partitioning

Time Complexity:
    - Embedding generation: O(s*n) where s is avg sentence length and n is number of complaints
    - LSH operations: O(d log n) where d is embedding dimension
    - HDBSCAN clustering: O(n log n) in best case

Space Complexity:
    - O(n*d) where n is number of complaints and d is embedding dimension (768 for BERT)
    - Reduced through partitioning to O(p*d) where p is partition size

Dependencies:
    - pyspark.ml: For ML pipeline components
    - johnsnowlabs.nlp: For BERT embeddings
    - hdbscan: For clustering
    - numpy: For numerical operations
"""

import json
import numpy as np
import pandas as pd
import hdbscan
from typing import List, Dict, Any, Tuple, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType, udf
from pyspark.sql.types import IntegerType, StringType, ArrayType, DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import BucketedRandomProjectionLSH, VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.functions import vector_to_array

# Import Spark NLP components
from johnsnowlabs.nlp import DocumentAssembler
from johnsnowlabs.nlp.annotator import Tokenizer as JSLTokenizer
from johnsnowlabs.nlp.embeddings import BertEmbeddings

def create_embedding_model(
    bert_model_name: str = "small_bert_L2_128",
    batch_size: int = 8,
    max_seq_length: int = 512
) -> Pipeline:
    """
    Creates a pipeline for generating ModernBERT embeddings from text.
    
    This function builds a Spark NLP pipeline that processes text data to generate
    contextual embeddings using a pre-trained BERT model. The pipeline includes
    document assembly, tokenization, and embedding generation.
    
    Parameters:
    -----------
    bert_model_name : str
        Name of the pre-trained BERT model to use
    batch_size : int
        Batch size for BERT embedding generation
    max_seq_length : int
        Maximum sequence length for BERT input
        
    Returns:
    --------
    Pipeline
        A Spark ML Pipeline that can be fit and applied to text data
        
    Notes:
    ------
    - Uses a smaller BERT model for efficiency in local execution
    - The pipeline must be fit to data before use
    - Output includes document, token, and embedding columns
    """
    # Document assembler
    document_assembler = DocumentAssembler() \
        .setInputCol("consumer_complaint_narrative") \
        .setOutputCol("document")

    # Tokenizer
    tokenizer = JSLTokenizer() \
        .setInputCols(["document"]) \
        .setOutputCol("token")

    # BERT embeddings
    bert_embeddings = BertEmbeddings.pretrained(bert_model_name, "en") \
        .setInputCols(["document", "token"]) \
        .setOutputCol("embeddings") \
        .setCaseSensitive(False) \
        .setBatchSize(batch_size) \
        .setMaxSentenceLength(max_seq_length)

    # Create pipeline
    return Pipeline(stages=[
        document_assembler,
        tokenizer,
        bert_embeddings
    ])

@pandas_udf(ArrayType(DoubleType()))
def extract_embeddings(embeddings_col):
    """
    Extracts BERT embeddings from Spark NLP output.
    
    This Pandas UDF processes the JSON output from Spark NLP's BERT embeddings
    and extracts the numerical vectors for further processing.
    
    Parameters:
    -----------
    embeddings_col : Series
        Pandas Series containing JSON strings of embeddings
        
    Returns:
    --------
    Series
        Pandas Series with embeddings as numpy arrays
        
    Notes:
    ------
    - Handles empty or malformed embeddings by returning zero vectors
    - Extracts the first token's embedding as the document embedding
    - Default embedding dimension is 768 (BERT base)
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

def create_lsh_model(
    vector_df: DataFrame,
    input_col: str = "embedding_vector",
    output_col: str = "hashes",
    bucket_length: float = 0.1,
    num_hash_tables: int = 3
) -> Tuple[BucketedRandomProjectionLSH, DataFrame]:
    """
    Creates an LSH model for efficient partitioning of high-dimensional vectors.
    
    This function builds and fits a Locality-Sensitive Hashing model to partition
    the embedding space, which improves clustering efficiency by grouping similar
    vectors together.
    
    Parameters:
    -----------
    vector_df : DataFrame
        DataFrame containing vectors to be partitioned
    input_col : str
        Name of the column containing vectors
    output_col : str
        Name of the output column for hash values
    bucket_length : float
        Length of the hash buckets (smaller values = more buckets)
    num_hash_tables : int
        Number of hash tables (more tables = better accuracy but more memory)
        
    Returns:
    --------
    Tuple[BucketedRandomProjectionLSH, DataFrame]
        The fitted LSH model and the transformed DataFrame with hash values
        
    Notes:
    ------
    - Smaller bucket_length creates more fine-grained partitions
    - More hash tables improves accuracy but increases memory usage
    - Time complexity: O(d * num_hash_tables * n) where d is dimension and n is number of vectors
    """
    # Create LSH model
    lsh = BucketedRandomProjectionLSH(
        inputCol=input_col,
        outputCol=output_col,
        bucketLength=bucket_length,
        numHashTables=num_hash_tables
    )
    
    # Fit and transform
    lsh_model = lsh.fit(vector_df)
    lsh_result = lsh_model.transform(vector_df)
    
    return lsh_model, lsh_result

@pandas_udf(IntegerType(), PandasUDFType.SCALAR)
def cluster_partition(vectors):
    """
    Applies HDBSCAN clustering to a partition of vectors.
    
    This Pandas UDF performs density-based clustering on a partition of embedding
    vectors, allowing for efficient parallel processing of large datasets.
    
    Parameters:
    -----------
    vectors : Series
        Pandas Series containing embedding vectors
        
    Returns:
    --------
    Series
        Pandas Series with cluster IDs for each vector
        
    Notes:
    ------
    - Handles small partitions by assigning all to one cluster
    - Converts HDBSCAN's noise label (-1) to positive cluster IDs
    - Optimized for memory efficiency within partitions
    - Time complexity: O(n log n) where n is the partition size
    """
    min_cluster_size = 5
    min_samples = 2
    
    if len(vectors) < min_cluster_size:
        return pd.Series([0] * len(vectors))  # All in one cluster if too few samples
    
    try:
        # Convert vectors to numpy array
        vectors_np = np.array([v.toArray() if hasattr(v, 'toArray') else v for v in vectors])
        
        # Apply HDBSCAN
        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=min(min_cluster_size, len(vectors) // 2),
            min_samples=min_samples,
            metric='euclidean',
            core_dist_n_jobs=-1  # Use all available cores
        )
        cluster_labels = clusterer.fit_predict(vectors_np)
        
        # Convert -1 (noise) to positive cluster IDs starting from max_cluster + 1
        if -1 in cluster_labels:
            max_cluster = max(cluster_labels) if max(cluster_labels) >= 0 else 0
            cluster_labels = np.where(cluster_labels == -1, max_cluster + 1, cluster_labels)
        
        return pd.Series(cluster_labels.astype(int))
    except Exception as e:
        print(f"Clustering error: {e}")
        return pd.Series([0] * len(vectors))  # Default cluster on error

def apply_hdbscan_clustering(
    spark: SparkSession,
    lsh_result: DataFrame,
    vector_col: str = "embedding_vector",
    hash_col: str = "hashes"
) -> DataFrame:
    """
    Applies HDBSCAN clustering to partitioned embedding vectors.
    
    This function processes each LSH partition separately using HDBSCAN clustering,
    which enables efficient clustering of large datasets by dividing them into
    smaller, more manageable chunks.
    
    Parameters:
    -----------
    spark : SparkSession
        The active Spark session
    lsh_result : DataFrame
        DataFrame containing vectors and LSH hash values
    vector_col : str
        Name of the column containing vectors
    hash_col : str
        Name of the column containing LSH hash values
        
    Returns:
    --------
    DataFrame
        DataFrame with original data plus cluster assignments
        
    Notes:
    ------
    - Uses groupBy + applyInPandas for parallel processing
    - Each partition is processed independently
    - Global cluster IDs are not guaranteed to be semantically meaningful across partitions
    - Time complexity: O(p * n_p log n_p) where p is number of partitions and n_p is average partition size
    """
    # Define the schema for the output DataFrame
    schema = lsh_result.schema.add("cluster_id", IntegerType())
    
    # Apply HDBSCAN to each partition
    return lsh_result.groupBy(hash_col).applyInPandas(
        lambda partition_df: partition_df.assign(
            cluster_id=cluster_partition(partition_df[vector_col])
        ),
        schema=schema
    )

def process_embeddings_for_clustering(
    df: DataFrame,
    embedding_col: str = "embedding"
) -> DataFrame:
    """
    Processes embeddings for clustering by converting them to the right format.
    
    This function prepares embedding vectors for LSH and clustering by ensuring
    they are in the correct format (Spark ML Vectors).
    
    Parameters:
    -----------
    df : DataFrame
        DataFrame containing embeddings
    embedding_col : str
        Name of the column containing embeddings
        
    Returns:
    --------
    DataFrame
        DataFrame with embeddings converted to Spark ML Vectors
        
    Notes:
    ------
    - Handles both array and vector input formats
    - Creates a new column with the standardized vector format
    - Required for compatibility with Spark ML functions
    """
    # Convert embeddings to vectors if they're not already
    assembler = VectorAssembler(
        inputCols=[embedding_col], 
        outputCol="embedding_vector"
    )
    
    return assembler.transform(df)

def get_cluster_statistics(
    clustered_df: DataFrame,
    cluster_col: str = "cluster_id"
) -> DataFrame:
    """
    Calculates statistics for each cluster.
    
    This function computes various statistics for each cluster, such as size,
    density, and other metrics that help evaluate clustering quality.
    
    Parameters:
    -----------
    clustered_df : DataFrame
        DataFrame containing cluster assignments
    cluster_col : str
        Name of the column containing cluster IDs
        
    Returns:
    --------
    DataFrame
        DataFrame with cluster statistics
        
    Notes:
    ------
    - Useful for evaluating clustering quality
    - Helps identify dominant clusters and outliers
    - Can be used to filter out noise or small clusters
    """
    return clustered_df.groupBy(cluster_col).count().orderBy("count", ascending=False) 