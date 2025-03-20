"""
Utility functions for Spark session management and configuration
"""
from pyspark.sql import SparkSession
import logging
from src import config

def create_spark_session(app_name=config.SPARK_APP_NAME, master=config.SPARK_MASTER):
    """
    Create and configure a Spark session
    
    Args:
        app_name (str): Name of the Spark application
        master (str): Spark master URL
        
    Returns:
        SparkSession: Configured Spark session
    """
    # Initialize spark session builder
    builder = SparkSession.builder.appName(app_name).master(master)
    
    # Add configurations from config file
    for key, value in config.SPARK_CONFIG.items():
        builder = builder.config(key, value)
    
    # Create and return session
    spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel(config.LOG_LEVEL)
    
    logging.info(f"Created Spark session with app name: {app_name}")
    return spark

def stop_spark_session(spark):
    """
    Properly stop a Spark session
    
    Args:
        spark (SparkSession): The Spark session to stop
    """
    if spark is not None:
        spark.stop()
        logging.info("Spark session stopped")
        
def get_streaming_query_details(query):
    """
    Get details about a streaming query for monitoring
    
    Args:
        query: The streaming query to get details from
        
    Returns:
        dict: A dictionary containing query metrics
    """
    if query is None:
        return None
        
    return {
        "name": query.name,
        "id": str(query.id),
        "status": query.status,
        "is_active": query.isActive,
        "recent_progress": query.recentProgress
    } 