"""
Setup script for CFPB Complaint Processing System

This script handles the initial setup for the system:
1. Verify project structure
2. Configure logging
3. Test Spark session
4. Print configuration summary
"""
import os
import sys
from pathlib import Path

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# Import project modules
from src import config
from src.logging_utils import setup_logging
from src.spark_utils import create_spark_session, stop_spark_session

def ensure_directories_exist():
    """Ensure all required directories exist"""
    directories = [
        config.DATA_DIR,
        config.MODELS_DIR,
        config.PIPELINES_DIR,
        config.NOTEBOOKS_DIR,
        config.CHECKPOINT_LOCATION,
        Path(config.LOG_FILE).parent  # Log directory
    ]
    
    for directory in directories:
        if not directory.exists():
            print(f"Creating directory: {directory}")
            directory.mkdir(parents=True, exist_ok=True)
        else:
            print(f"Directory exists: {directory}")

def print_config_summary():
    """Print a summary of the configuration settings"""
    print("\n==== Configuration Summary ====")
    print(f"\nFile Paths:")
    print(f"  Project Root: {project_root}")
    print(f"  Data Directory: {config.DATA_DIR}")
    print(f"  Models Directory: {config.MODELS_DIR}")
    print(f"  Pipelines Directory: {config.PIPELINES_DIR}")
    print(f"  Historical Data Path: {config.HISTORICAL_DATA_PATH}")
    print(f"  Test Data Path: {config.TEST_DATA_PATH}")
    print(f"  Checkpoint Location: {config.CHECKPOINT_LOCATION}")
    
    print(f"\nKafka Configuration:")
    print(f"  Bootstrap Servers: {config.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Input Topic: {config.KAFKA_INPUT_TOPIC}")
    print(f"  Output Topic: {config.KAFKA_OUTPUT_TOPIC}")
    
    print(f"\nSpark Parameters:")
    print(f"  App Name: {config.SPARK_APP_NAME}")
    print(f"  Master: {config.SPARK_MASTER}")
    print(f"  Batch Size: {config.SPARK_BATCH_SIZE}")
    print(f"  Processing Time: {config.SPARK_PROCESSING_TIME}")
    print(f"  Max Offsets Per Trigger: {config.SPARK_MAX_OFFSETS_PER_TRIGGER}")
    
    print(f"\nModel Parameters:")
    for key, value in config.MODEL_PARAMS.items():
        print(f"  {key}: {value}")
    print(f"  Prediction Threshold: {config.PREDICTION_THRESHOLD}")
    
    print(f"\nLogging Configuration:")
    print(f"  Log Level: {config.LOG_LEVEL}")
    print(f"  Log Format: {config.LOG_FORMAT}")
    print(f"  Log File: {config.LOG_FILE}")

def test_spark_session():
    """Test creating a Spark session"""
    print("\nInitializing Spark session...")
    try:
        # Create and configure Spark session
        spark = create_spark_session()
        
        # Display Spark version
        print(f"Spark version: {spark.version}")
        
        # Display Spark configuration
        print("\nSpark configuration:")
        for key, value in config.SPARK_CONFIG.items():
            print(f"  {key}: {value}")
        
        # Create a simple test DataFrame
        test_data = [(1, "Test1"), (2, "Test2"), (3, "Test3")]
        test_df = spark.createDataFrame(test_data, ["id", "value"])
        
        # Display the DataFrame
        print("\nTest DataFrame:")
        test_df.show()
        
        # Verify Spark operations
        print("\nTest SQL query:")
        test_df.createOrReplaceTempView("test_table")
        spark.sql("SELECT * FROM test_table WHERE id > 1").show()
        
        print("\nSpark session test successful")
        
        # Clean up resources
        stop_spark_session(spark)
        
        return True
    except Exception as e:
        print(f"Error testing Spark session: {str(e)}")
        return False

def main():
    """Main function to run setup"""
    print("Starting CFPB Complaint Processing System setup...")
    
    # 1. Ensure directories exist
    print("\n1. Verifying project structure...")
    ensure_directories_exist()
    
    # 2. Configure logging
    print("\n2. Configuring logging...")
    logger = setup_logging("setup_script")
    logger.info("Logging configured successfully")
    
    # 3. Test Spark session
    print("\n3. Testing Spark session...")
    spark_success = test_spark_session()
    
    # 4. Print configuration summary
    print("\n4. Configuration summary...")
    print_config_summary()
    
    # Print setup results
    print("\n==== Setup Results ====")
    print(f"Project structure: DONE")
    print(f"Logging configuration: DONE")
    print(f"Spark session test: {'DONE' if spark_success else 'FAILED'}")
    print("\nSetup completed. Run 'python -m src.setup' to verify again.")

if __name__ == "__main__":
    main() 