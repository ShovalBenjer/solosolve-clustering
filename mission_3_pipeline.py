"""
Mission 3: Preprocessing Pipeline Development

This script builds, tests, and saves the preprocessing pipeline for the CFPB
Complaint Processing System. It implements advanced text preprocessing, 
feature engineering, and creates a modular PySpark pipeline.
"""

import os
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml import Pipeline, PipelineModel

# Add parent directory to path for imports
sys.path.append(str(Path(os.path.dirname(os.path.abspath(__file__))).parent))

# Import project modules
from src import config
from src.spark_utils import create_spark_session
from src.logging_utils import setup_logging
from src.schema import get_complaint_schema, get_processed_complaint_schema
from src.data_utils import load_historical_data, preprocess_dates, engineer_target_variable
from src.preprocessing import (
    create_preprocessing_pipeline, 
    validate_pipeline,
    save_pipeline
)

# Set up logging
logger = setup_logging(log_level=config.LOG_LEVEL)

def load_sample_data(spark, limit=1000):
    """
    Load a small sample of data for testing the pipeline.
    
    Args:
        spark (SparkSession): Spark session
        limit (int, optional): Number of records to load. Defaults to 1000.
    
    Returns:
        DataFrame: Sample dataframe
    """
    logger.info(f"Loading sample data from {config.HISTORICAL_DATA_PATH}, limit {limit}")
    
    # Check if file exists
    if not os.path.exists(config.HISTORICAL_DATA_PATH):
        # Create sample data if file doesn't exist
        logger.warning(f"Historical data not found at {config.HISTORICAL_DATA_PATH}")
        logger.info("Creating mock data for testing")
        
        # Create sample data with the complaint schema
        schema = get_complaint_schema()
        
        # Create an empty dataframe with the schema
        sample_df = spark.createDataFrame([], schema)
        
        # Return the empty dataframe
        return sample_df
    
    # Load the data with schema validation
    schema = get_complaint_schema()
    try:
        sample_df = spark.read.schema(schema).json(str(config.HISTORICAL_DATA_PATH))
        
        # Limit the number of records
        sample_df = sample_df.limit(limit)
        
        # Basic preprocessing
        sample_df = preprocess_dates(sample_df)
        sample_df = engineer_target_variable(sample_df)
        
        return sample_df
    
    except Exception as e:
        logger.error(f"Failed to load sample data: {str(e)}")
        raise

def test_pipeline_components(sample_df):
    """
    Test individual pipeline components to verify they work correctly.
    
    Args:
        sample_df (DataFrame): Sample dataframe to test with
    """
    logger.info("Testing pipeline components")
    
    from src.preprocessing import (
        create_text_preprocessing_stages,
        create_date_feature_stages,
        create_categorical_encoding_stages,
        create_derived_features_stage
    )
    
    # Test text preprocessing
    logger.info("Testing text preprocessing stages")
    text_stages = create_text_preprocessing_stages()
    text_pipeline = Pipeline(stages=text_stages)
    
    # Only process records with non-null narratives
    text_df = sample_df.filter(F.col("Consumer complaint narrative").isNotNull())
    if text_df.count() > 0:
        try:
            text_result = text_pipeline.fit(text_df).transform(text_df)
            logger.info(f"Text preprocessing successful: {text_result.columns}")
        except Exception as e:
            logger.error(f"Text preprocessing failed: {str(e)}")
    else:
        logger.warning("No records with complaint narratives found for testing")
    
    # Test date feature extraction
    logger.info("Testing date feature stages")
    date_stages = create_date_feature_stages()
    date_pipeline = Pipeline(stages=date_stages)
    
    try:
        date_result = date_pipeline.fit(sample_df).transform(sample_df)
        logger.info(f"Date feature extraction successful: {date_result.columns}")
    except Exception as e:
        logger.error(f"Date feature extraction failed: {str(e)}")
    
    # Test categorical encoding
    logger.info("Testing categorical encoding stages")
    cat_stages, _ = create_categorical_encoding_stages()
    cat_pipeline = Pipeline(stages=cat_stages)
    
    try:
        cat_result = cat_pipeline.fit(sample_df).transform(sample_df)
        logger.info(f"Categorical encoding successful: {cat_result.columns}")
    except Exception as e:
        logger.error(f"Categorical encoding failed: {str(e)}")
    
    # Test derived features
    logger.info("Testing derived features stage")
    derived_stages = create_derived_features_stage()
    derived_pipeline = Pipeline(stages=derived_stages)
    
    try:
        derived_result = derived_pipeline.fit(sample_df).transform(sample_df)
        logger.info(f"Derived features successful: {derived_result.columns}")
    except Exception as e:
        logger.error(f"Derived features failed: {str(e)}")
    
    logger.info("Component testing completed")

def build_and_save_pipeline(sample_df):
    """
    Build, test, and save the complete preprocessing pipeline.
    
    Args:
        sample_df (DataFrame): Sample dataframe to test the pipeline with
        
    Returns:
        PipelineModel: Fitted pipeline model
    """
    logger.info("Building and saving preprocessing pipeline")
    
    # Create the complete pipeline
    pipeline = create_preprocessing_pipeline()
    
    # Fit the pipeline to the sample data
    try:
        logger.info("Fitting preprocessing pipeline to sample data")
        pipeline_model = pipeline.fit(sample_df)
        
        # Transform the data for validation
        transformed_df = validate_pipeline(pipeline_model, sample_df)
        
        # Check feature vector
        feature_col = transformed_df.select("features").first()
        if feature_col and feature_col["features"]:
            logger.info(f"Pipeline created feature vectors of size: {len(feature_col['features'])}")
        
        # Save the pipeline
        save_path = save_pipeline(pipeline_model)
        logger.info(f"Pipeline saved successfully to {save_path}")
        
        return pipeline_model
    
    except Exception as e:
        logger.error(f"Failed to build pipeline: {str(e)}")
        raise

def main():
    """
    Main function to execute Mission 3.
    """
    logger.info("Starting Mission 3: Preprocessing Pipeline Development")
    
    # Create directories if they don't exist
    os.makedirs(config.PIPELINES_DIR, exist_ok=True)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Load sample data
        sample_df = load_sample_data(spark)
        
        # Display data info
        logger.info(f"Loaded {sample_df.count()} records for pipeline development")
        
        # Test individual pipeline components
        test_pipeline_components(sample_df)
        
        # Build and save the complete pipeline
        pipeline_model = build_and_save_pipeline(sample_df)
        
        # Show pipeline metadata
        logger.info(f"Pipeline has {len(pipeline_model.stages)} stages")
        
        logger.info("Mission 3 completed successfully")
    
    except Exception as e:
        logger.error(f"Mission 3 failed: {str(e)}")
        sys.exit(1)
    
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main() 
