"""
Configuration settings for the CFPB complaint clustering system.
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'cfpb-complaints-input')
KAFKA_OUTPUT_TOPIC = os.getenv('KAFKA_OUTPUT_TOPIC', 'cfpb-complaints-output')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'cfpb-complaint-processor')

# Spark configuration
SPARK_APP_NAME = 'CFPB_Complaint_Processor'
SPARK_MASTER = os.getenv('SPARK_MASTER', 'local[*]')
SPARK_CHECKPOINT_DIR = os.getenv('SPARK_CHECKPOINT_DIR', './checkpoint')
SPARK_BATCH_DURATION = 60  # Seconds (1 minute window)
SPARK_WATERMARK_DELAY = '1 minute'

# BERT model configuration
BERT_MODEL_NAME = os.getenv('BERT_MODEL_NAME', 'small_bert_L2_128')
BERT_BATCH_SIZE = int(os.getenv('BERT_BATCH_SIZE', '8'))
BERT_MAX_SEQ_LENGTH = int(os.getenv('BERT_MAX_SEQ_LENGTH', '512'))

# HDBSCAN configuration
HDBSCAN_MIN_CLUSTER_SIZE = int(os.getenv('HDBSCAN_MIN_CLUSTER_SIZE', '5'))
HDBSCAN_METRIC = os.getenv('HDBSCAN_METRIC', 'euclidean')
LSH_BUCKET_LENGTH = float(os.getenv('LSH_BUCKET_LENGTH', '0.1'))
LSH_NUM_HASH_TABLES = int(os.getenv('LSH_NUM_HASH_TABLES', '3'))

# Database configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '3306'))
DB_NAME = os.getenv('DB_NAME', 'cfpb_complaints')
DB_USER = os.getenv('DB_USER', 'cfpb_app')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
DB_URI = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Data paths
SAMPLE_DATA_PATH = os.getenv('SAMPLE_DATA_PATH', './data/complaints.csv')

# Resolution mapping (example - would be extended with more mappings)
RESOLUTION_MAPPING = {
    0: "Pending Review",
    1: "Closed with Explanation",
    2: "Closed with Monetary Relief",
    3: "Closed with Non-Monetary Relief",
    4: "Closed without Relief",
    5: "Closed with Explanation",
    -1: "Uncategorized"  # Default for noise
} 