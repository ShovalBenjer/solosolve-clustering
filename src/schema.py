"""
Schema definitions for CFPB Complaint Processing System

This module defines the schema for CFPB complaint data using PySpark StructType,
ensuring consistent data structure across the application.
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType, DoubleType

def get_complaint_schema():
    """
    Get the schema for CFPB complaint data
    
    Returns:
        StructType: Schema for CFPB complaint data with all 18 fields
    """
    # Define schema with all 18 fields from CFPB complaint data
    schema = StructType([
        StructField("Date received", StringType(), True),
        StructField("Product", StringType(), True),
        StructField("Sub-product", StringType(), True),
        StructField("Issue", StringType(), True),
        StructField("Sub-issue", StringType(), True),
        StructField("Consumer complaint narrative", StringType(), True),
        StructField("Company public response", StringType(), True),
        StructField("Company", StringType(), True),
        StructField("State", StringType(), True),
        StructField("ZIP code", StringType(), True),
        StructField("Tags", StringType(), True),
        StructField("Consumer consent provided?", StringType(), True),
        StructField("Submitted via", StringType(), True),
        StructField("Date sent to company", StringType(), True),
        StructField("Company response to consumer", StringType(), True),
        StructField("Timely response?", StringType(), True),
        StructField("Consumer disputed?", StringType(), True),
        StructField("Complaint ID", StringType(), True)
    ])
    
    return schema

def get_processed_complaint_schema():
    """
    Get the schema for processed CFPB complaint data with additional columns
    
    Returns:
        StructType: Schema for processed CFPB complaint data
    """
    base_schema = get_complaint_schema()
    
    # Add additional fields for processed data
    processed_fields = [
        StructField("DateReceivedTimestamp", TimestampType(), True),
        StructField("DateSentToCompanyTimestamp", TimestampType(), True),
        StructField("ProcessingTimeInDays", DoubleType(), True),
        StructField("SuccessfulResolution", BooleanType(), True)
    ]
    
    # Create a new schema with all fields
    processed_schema = StructType(base_schema.fields + processed_fields)
    
    return processed_schema

def get_prediction_schema():
    """
    Get the schema for prediction results
    
    Returns:
        StructType: Schema for prediction results
    """
    # Define schema for prediction output
    schema = StructType([
        StructField("Complaint ID", StringType(), True),
        StructField("DateReceived", TimestampType(), True),
        StructField("Product", StringType(), True),
        StructField("Company", StringType(), True),
        StructField("State", StringType(), True),
        StructField("PredictionScore", DoubleType(), True),
        StructField("PredictedResolution", BooleanType(), True),
        StructField("Escalate", BooleanType(), True),
        StructField("ProcessingTime", DoubleType(), True)
    ])
    
    return schema 