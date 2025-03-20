"""
Mission 4: Model Training (Batch Processing)

This script implements the model training process for the CFPB Complaint Processing System.
It loads the preprocessed data, trains a logistic regression model using cross-validation,
evaluates the model performance, and saves the best model for deployment.
"""

import os
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession
import mlflow

# Add parent directory to path for imports
sys.path.append(str(Path(os.path.dirname(os.path.abspath(__file__))).parent))

# Import project modules
from src import config
from src.spark_utils import create_spark_session
from src.logging_utils import setup_logging
from src.data_utils import load_historical_data
from src.preprocessing import load_pipeline
from src.model_training import (
    prepare_training_data,
    create_param_grid,
    train_model,
    evaluate_model,
    save_model,
    analyze_feature_importance
)

# Set up logging
logger = setup_logging(log_level=config.LOG_LEVEL)

def load_and_preprocess_data(spark):
    """
    Load historical data and apply preprocessing pipeline.
    
    Args:
        spark (SparkSession): Active Spark session
        
    Returns:
        DataFrame: Preprocessed data ready for training
    """
    logger.info("Loading and preprocessing historical data")
    
    try:
        # Load historical data
        raw_data = load_historical_data(spark, config.HISTORICAL_DATA_PATH)
        logger.info(f"Loaded {raw_data.count()} records from historical data")
        
        # Load preprocessing pipeline
        pipeline_model = load_pipeline()
        
        # Apply preprocessing
        processed_data = pipeline_model.transform(raw_data)
        logger.info("Preprocessing completed successfully")
        
        return processed_data
    
    except Exception as e:
        logger.error(f"Failed to load and preprocess data: {str(e)}")
        raise

def train_and_evaluate():
    """
    Main function to train and evaluate the model.
    """
    logger.info("Starting Mission 4: Model Training")
    
    # Create directories if they don't exist
    os.makedirs(config.MODELS_DIR, exist_ok=True)
    
    # Configure MLflow
    mlflow.set_tracking_uri("file:" + str(config.MODELS_DIR / "mlruns"))
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Load and preprocess data
        processed_data = load_and_preprocess_data(spark)
        
        # Prepare data splits
        train_data, val_data, test_data = prepare_training_data(processed_data)
        
        # Create parameter grid for tuning
        param_grid = create_param_grid()
        
        # Train model with cross validation
        logger.info("Starting model training with cross validation")
        model, val_metrics = train_model(train_data, val_data, param_grid)
        
        # Evaluate on test set
        logger.info("Evaluating model on test set")
        test_predictions = model.transform(test_data)
        test_metrics = evaluate_model(test_predictions)
        
        logger.info("Test set metrics:")
        for metric, value in test_metrics.items():
            logger.info(f"{metric}: {value:.4f}")
        
        # Analyze feature importance
        feature_names = [field.name for field in processed_data.schema.fields 
                        if field.name != "features"]
        importance_analysis = analyze_feature_importance(model, feature_names)
        
        # Save the model
        save_path = save_model(model, test_metrics)
        logger.info(f"Model saved to {save_path}")
        
        logger.info("Mission 4 completed successfully")
        
        return model, test_metrics
    
    except Exception as e:
        logger.error(f"Mission 4 failed: {str(e)}")
        raise
    
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    train_and_evaluate() 
