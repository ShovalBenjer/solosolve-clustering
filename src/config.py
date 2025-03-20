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
import logging

# Base directory
BASE_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# File paths
DATA_DIR = BASE_DIR / "data"
MODELS_DIR = BASE_DIR / "models"
PIPELINES_DIR = BASE_DIR / "pipelines"
NOTEBOOKS_DIR = BASE_DIR / "notebooks"
LOGS_DIR = BASE_DIR / "logs"

# Create directories if they don't exist
for dir_path in [DATA_DIR, MODELS_DIR, LOGS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Historical data path (will be populated when data is available)
HISTORICAL_DATA_PATH = DATA_DIR / "historical_complaints_75percent.json"
TEST_DATA_PATH = DATA_DIR / "historical_complaints_25percent.json"
SCHEMA_PATH = DATA_DIR / "complaint_schema.json"

# Model and pipeline paths
MODEL_PATH = MODELS_DIR / "logistic_regression_model"
PREPROCESSING_PIPELINE_PATH = PIPELINES_DIR / "preprocessing_pipeline"

# Spark checkpoint location
CHECKPOINT_LOCATION = DATA_DIR / "checkpoints"

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "cfpb_complaints"
KAFKA_GROUP_ID = "complaint_processor"

# Spark configuration parameters
SPARK_BATCH_SIZE = 1000
SPARK_PROCESSING_TIME = "6 seconds"
SPARK_MAX_OFFSETS_PER_TRIGGER = 1670  # For 10,000 complaints per minute (10000/6)

# Spark session configuration
SPARK_APP_NAME = "CFPB_Complaint_Processing"
SPARK_MASTER = "local[*]"  # Use all available cores locally
SPARK_LOG_LEVEL = "WARN"
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
    "maxIter": 20,
    "regParam": 0.1,
    "elasticNetParam": 0.5
}

# Prediction threshold for decision logic
PREDICTION_THRESHOLD = 0.5

# Advanced NLP features configuration
ENABLE_ADVANCED_NLP = True  # Set to True to enable advanced NLP features
NLP_BATCH_SIZE = 32  # Batch size for NLP operations

# Feature engineering settings
TEXT_FEATURES = [
    "Consumer complaint narrative",
    "Company public response",
    "Sub-product",
    "Issue",
    "Sub-issue"
]

CATEGORICAL_FEATURES = [
    "Product",
    "Sub-product",
    "Issue",
    "Sub-issue",
    "Company response to consumer",
    "Timely response",
    "Consumer consent provided"
]

DATE_FEATURES = [
    "Date received",
    "Date sent to company"
]

# Logging configuration
LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE = LOGS_DIR / "cfpb_processing.log"

# MLflow settings
MLFLOW_TRACKING_URI = str(MODELS_DIR / "mlruns")
EXPERIMENT_NAME = "cfpb_complaint_classification"

# Model evaluation settings
METRICS = [
    "areaUnderROC",
    "areaUnderPR",
    "accuracy",
    "precision",
    "recall",
    "f1"
]

# Cross-validation settings
CV_FOLDS = 5
CV_METRIC = "areaUnderROC"

# Feature importance settings
TOP_FEATURES_TO_LOG = 10 