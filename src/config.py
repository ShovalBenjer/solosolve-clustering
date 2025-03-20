"""
Configuration file for CFPB Complaint Processing System
This file contains all configurations required for the system including:
- File paths
- Kafka settings
- Spark parameters
- Model settings
"""
import os
from pathlib import Path

# Base directory
BASE_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# File paths
DATA_DIR = BASE_DIR / "data"
MODELS_DIR = BASE_DIR / "models"
PIPELINES_DIR = BASE_DIR / "pipelines"
NOTEBOOKS_DIR = BASE_DIR / "notebooks"

# Historical data path (will be populated when data is available)
HISTORICAL_DATA_PATH = DATA_DIR / "historical_complaints_75percent.json"
TEST_DATA_PATH = DATA_DIR / "historical_complaints_25percent.json"

# Model and pipeline paths
MODEL_PATH = MODELS_DIR / "logistic_regression_model"
PREPROCESSING_PIPELINE_PATH = PIPELINES_DIR / "preprocessing_pipeline"

# Spark checkpoint location
CHECKPOINT_LOCATION = DATA_DIR / "checkpoints"

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_INPUT_TOPIC = "complaints-input"
KAFKA_OUTPUT_TOPIC = "complaints-predictions"

# Spark configuration parameters
SPARK_BATCH_SIZE = 1000
SPARK_PROCESSING_TIME = "6 seconds"
SPARK_MAX_OFFSETS_PER_TRIGGER = 1670  # For 10,000 complaints per minute (10000/6)

# Spark session configuration
SPARK_APP_NAME = "CFPB_Complaint_Processing"
SPARK_MASTER = "local[*]"  # Use all available cores locally
SPARK_CONFIG = {
    "spark.executor.memory": "4g",
    "spark.driver.memory": "4g",
    "spark.sql.shuffle.partitions": "16",
    "spark.default.parallelism": "16",
    "spark.streaming.kafka.maxRatePerPartition": "1000",
    "spark.streaming.backpressure.enabled": "true",
    "spark.streaming.kafka.consumer.cache.enabled": "false"
}

# Model training parameters
MODEL_PARAMS = {
    "maxIter": 10,
    "regParam": 0.1,
    "elasticNetParam": 0.8
}

# Prediction threshold for decision logic
PREDICTION_THRESHOLD = 0.5

# Logging configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE = DATA_DIR / "logs" / "app.log" 