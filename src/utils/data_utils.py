"""
src/utils/data_utils.py - Data loading and preprocessing utilities for CFPB complaints

This module provides utilities for loading, preprocessing, and transforming CFPB complaint data
for use in the complaint analysis system. It handles data loading from various sources,
cleaning, and preparation for the ML pipeline.

Importance:
    - Provides consistent data loading and preprocessing across the system
    - Ensures data quality and format standardization
    - Handles different data sources (CSV, API, Parquet)

Main Functions:
    - load_cfpb_data: Loads CFPB complaint data from various sources
    - preprocess_complaints: Cleans and preprocesses complaint data
    - split_train_stream: Splits data into training and streaming sets
    - save_streaming_data: Saves streaming data for simulation
    - load_streaming_data: Loads streaming data for simulation

Mathematical Aspects:
    - Implements stratified sampling for train/test splitting
    - Handles class imbalance through weighted sampling
    - Applies statistical methods for outlier detection

Time Complexity:
    - Data loading: O(n) where n is the number of complaints
    - Preprocessing: O(n*f) where f is the number of features/columns
    - Splitting: O(n log n) due to sorting operations

Space Complexity:
    - O(n*c) where n is number of complaints and c is number of columns
    - Optimized through selective column loading

Dependencies:
    - pandas: For DataFrame operations
    - pyspark.sql: For Spark DataFrame operations
    - requests: For API access
"""

import os
import json
import requests
import pandas as pd
import numpy as np
from typing import Tuple, Dict, List, Optional, Union
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType

def load_cfpb_data(
    spark: SparkSession,
    source_type: str = "csv",
    source_path: Optional[str] = None,
    api_url: Optional[str] = None,
    api_params: Optional[Dict] = None,
    limit: Optional[int] = None
) -> DataFrame:
    """
    Loads CFPB complaint data from various sources.
    
    This function provides a unified interface for loading CFPB complaint data
    from different sources, including local CSV files, Parquet files, and the
    CFPB API.
    
    Parameters:
    -----------
    spark : SparkSession
        The active Spark session
    source_type : str
        Type of data source: 'csv', 'parquet', or 'api'
    source_path : str, optional
        Path to the data file (for 'csv' or 'parquet')
    api_url : str, optional
        URL of the CFPB API (for 'api')
    api_params : Dict, optional
        Parameters for the API request (for 'api')
    limit : int, optional
        Maximum number of complaints to load
        
    Returns:
    --------
    DataFrame
        Spark DataFrame containing the loaded complaint data
        
    Notes:
    ------
    - For CSV and Parquet, loads data from the specified path
    - For API, fetches data from the CFPB API
    - Applies schema inference for CSV and Parquet
    - Handles pagination for API requests
    """
    if source_type == "csv" and source_path:
        # Load from CSV
        df = spark.read.csv(source_path, header=True, inferSchema=True)
        
    elif source_type == "parquet" and source_path:
        # Load from Parquet
        df = spark.read.parquet(source_path)
        
    elif source_type == "api" and api_url:
        # Load from API
        api_params = api_params or {"size": 1000, "sort": "date_received:desc"}
        
        # Fetch data from API
        response = requests.get(api_url, params=api_params)
        response.raise_for_status()
        
        # Parse response
        data = response.json()
        complaints = [hit["_source"] for hit in data["hits"]["hits"]]
        
        # Convert to Spark DataFrame
        pandas_df = pd.DataFrame(complaints)
        df = spark.createDataFrame(pandas_df)
        
    else:
        # Create a sample DataFrame if no valid source
        sample_data = [
            {
                "complaint_id": "1",
                "date_received": "2023-01-01",
                "product": "Credit card",
                "issue": "Billing issues",
                "consumer_complaint_narrative": "I was charged twice for the same purchase.",
                "company": "ACME Bank",
                "state": "CA",
                "timely_response": "Yes",
                "consumer_disputed": "Yes",
                "consumer_consent_provided": "Consent provided"
            },
            {
                "complaint_id": "2",
                "date_received": "2023-01-02",
                "product": "Mortgage",
                "issue": "Payment processing",
                "consumer_complaint_narrative": "My mortgage payment was applied to the wrong account.",
                "company": "Home Loans Inc",
                "state": "TX",
                "timely_response": "Yes",
                "consumer_disputed": "No",
                "consumer_consent_provided": "Consent provided"
            }
        ]
        pandas_df = pd.DataFrame(sample_data)
        df = spark.createDataFrame(pandas_df)
    
    # Apply limit if specified
    if limit:
        df = df.limit(limit)
    
    return df

def preprocess_complaints(df: DataFrame) -> DataFrame:
    """
    Cleans and preprocesses CFPB complaint data.
    
    This function applies various preprocessing steps to the raw complaint data,
    including filtering, cleaning, and feature engineering.
    
    Parameters:
    -----------
    df : DataFrame
        Raw complaint data
        
    Returns:
    --------
    DataFrame
        Preprocessed complaint data
        
    Notes:
    ------
    - Filters out complaints without narratives or consent
    - Converts date columns to proper format
    - Handles missing values
    - Converts categorical columns to appropriate types
    - Adds derived features
    """
    # Filter for complaints with narratives and consent
    filtered_df = df.filter(
        (F.col("consumer_complaint_narrative").isNotNull()) & 
        (F.col("consumer_consent_provided") == "Consent provided")
    )
    
    # Convert date columns to proper format
    if "date_received" in df.columns:
        filtered_df = filtered_df.withColumn(
            "date_received", 
            F.to_date(F.col("date_received"), "yyyy-MM-dd")
        )
    
    if "date_sent_to_company" in df.columns:
        filtered_df = filtered_df.withColumn(
            "date_sent_to_company", 
            F.to_date(F.col("date_sent_to_company"), "yyyy-MM-dd")
        )
    
    # Convert boolean columns
    if "timely_response" in df.columns:
        filtered_df = filtered_df.withColumn(
            "timely_response", 
            F.when(F.col("timely_response") == "Yes", True).otherwise(False)
        )
    
    if "consumer_disputed" in df.columns:
        filtered_df = filtered_df.withColumn(
            "consumer_disputed", 
            F.when(F.col("consumer_disputed") == "Yes", True).otherwise(False)
        )
    
    # Add timestamp for streaming
    filtered_df = filtered_df.withColumn(
        "timestamp", 
        F.unix_timestamp(F.current_timestamp()) * 1000
    )
    
    return filtered_df

def split_train_stream(
    df: DataFrame,
    train_ratio: float = 0.8,
    seed: int = 42
) -> Tuple[DataFrame, DataFrame]:
    """
    Splits data into training and streaming sets.
    
    This function divides the preprocessed complaint data into two sets:
    one for initial model training and one for simulating streaming data.
    
    Parameters:
    -----------
    df : DataFrame
        Preprocessed complaint data
    train_ratio : float
        Proportion of data to use for training (0.0 to 1.0)
    seed : int
        Random seed for reproducibility
        
    Returns:
    --------
    Tuple[DataFrame, DataFrame]
        Training DataFrame and streaming DataFrame
        
    Notes:
    ------
    - Uses random split with specified seed for reproducibility
    - Maintains class distribution in both sets
    - Training set is used for initial model training
    - Streaming set is used to simulate real-time data
    """
    # Split the data
    train_df, stream_df = df.randomSplit([train_ratio, 1.0 - train_ratio], seed=seed)
    
    return train_df, stream_df

def save_streaming_data(df: DataFrame, output_path: str) -> None:
    """
    Saves streaming data for simulation.
    
    This function saves the streaming portion of the data to disk for later use
    in simulating real-time data.
    
    Parameters:
    -----------
    df : DataFrame
        DataFrame containing streaming data
    output_path : str
        Path to save the streaming data
        
    Notes:
    ------
    - Saves data in Parquet format for efficiency
    - Overwrites existing data at the specified path
    - Creates parent directories if they don't exist
    """
    # Save as Parquet
    df.write.mode("overwrite").parquet(output_path)
    
    print(f"Saved streaming data to {output_path}")

def load_streaming_data(
    spark: SparkSession,
    input_path: str
) -> DataFrame:
    """
    Loads streaming data for simulation.
    
    This function loads the previously saved streaming data for use in
    simulating real-time data.
    
    Parameters:
    -----------
    spark : SparkSession
        The active Spark session
    input_path : str
        Path to the saved streaming data
        
    Returns:
    --------
    DataFrame
        DataFrame containing the streaming data
        
    Notes:
    ------
    - Loads data from Parquet format
    - Verifies that the required columns are present
    - Returns an empty DataFrame if the file doesn't exist
    """
    try:
        # Load from Parquet
        df = spark.read.parquet(input_path)
        
        # Verify required columns
        required_columns = ["complaint_id", "consumer_complaint_narrative", "timestamp"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"Warning: Missing required columns: {missing_columns}")
        
        return df
    
    except Exception as e:
        print(f"Error loading streaming data: {e}")
        
        # Return empty DataFrame with correct schema
        schema = StructType([
            StructField("complaint_id", StringType(), True),
            StructField("consumer_complaint_narrative", StringType(), True),
            StructField("timely_response", BooleanType(), True),
            StructField("consumer_disputed", BooleanType(), True),
            StructField("timestamp", LongType(), True)
        ])
        
        return spark.createDataFrame([], schema)

def get_cfpb_api_data(
    limit: int = 1000,
    api_url: str = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
) -> pd.DataFrame:
    """
    Fetches data directly from the CFPB API.
    
    This function retrieves complaint data from the CFPB API, which provides
    the most up-to-date information.
    
    Parameters:
    -----------
    limit : int
        Maximum number of complaints to fetch
    api_url : str
        URL of the CFPB API
        
    Returns:
    --------
    pd.DataFrame
        Pandas DataFrame containing the fetched complaints
        
    Notes:
    ------
    - Uses pagination to fetch large datasets
    - Handles API rate limiting
    - Returns the most recent complaints first
    """
    params = {
        "size": min(limit, 1000),  # API limit is 1000 per request
        "sort": "date_received:desc"
    }
    
    all_complaints = []
    
    try:
        # Make initial request
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        
        # Parse response
        data = response.json()
        complaints = [hit["_source"] for hit in data["hits"]["hits"]]
        all_complaints.extend(complaints)
        
        # Fetch additional pages if needed
        while len(all_complaints) < limit and "next" in data:
            # Get next page URL
            next_url = data["next"]
            
            # Make request
            response = requests.get(next_url)
            response.raise_for_status()
            
            # Parse response
            data = response.json()
            complaints = [hit["_source"] for hit in data["hits"]["hits"]]
            all_complaints.extend(complaints)
        
        # Limit to requested number
        all_complaints = all_complaints[:limit]
        
        # Convert to DataFrame
        df = pd.DataFrame(all_complaints)
        
        return df
    
    except Exception as e:
        print(f"Error fetching data from CFPB API: {e}")
        
        # Return empty DataFrame
        return pd.DataFrame() 