"""
CFPB Complaint Processing System - Mission 2

This script is the main entry point for Mission 2 - Data Preparation and Schema Definition.
It will:
1. Define data schema
2. Load and validate historical data
3. Preprocess data and engineer target variable
4. Split data into training and test sets
5. Create Kafka producer simulation

Run this script with: python mission_2_data_preparation.py
"""
import os
import sys
import json
from pathlib import Path

# Add project root to path
project_root = Path(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(str(project_root))

# Import project modules
from src import config
from src.logging_utils import setup_logging
from src.spark_utils import create_spark_session, stop_spark_session
from src.schema import get_complaint_schema, get_processed_complaint_schema
from src.data_utils import (load_historical_data, validate_data_quality, preprocess_dates,
                           engineer_target_variable, split_train_test_data, save_dataframes,
                           create_kafka_producer_simulation, create_data_summary)

def create_sample_data():
    """Create sample data for testing"""
    # Create sample data
    sample_data = [
        {
            "Date received": "01/01/2023",
            "Product": "Credit card",
            "Sub-product": "General-purpose credit card",
            "Issue": "Billing disputes",
            "Sub-issue": "Billing statement not delivered",
            "Consumer complaint narrative": "I have not received my credit card statement for the past 3 months.",
            "Company public response": "Company believes it acted appropriately",
            "Company": "ABC Bank",
            "State": "CA",
            "ZIP code": "90210",
            "Tags": "Older American",
            "Consumer consent provided?": "Consent provided",
            "Submitted via": "Web",
            "Date sent to company": "01/03/2023",
            "Company response to consumer": "Closed with explanation",
            "Timely response?": "Yes",
            "Consumer disputed?": "No",
            "Complaint ID": "1234567"
        },
        {
            "Date received": "02/15/2023",
            "Product": "Mortgage",
            "Sub-product": "Conventional fixed mortgage",
            "Issue": "Loan servicing",
            "Sub-issue": "Escrow account",
            "Consumer complaint narrative": "My mortgage company is not correctly handling my escrow account.",
            "Company public response": "Company believes it acted appropriately",
            "Company": "XYZ Mortgage",
            "State": "TX",
            "ZIP code": "75001",
            "Tags": None,
            "Consumer consent provided?": "Consent provided",
            "Submitted via": "Phone",
            "Date sent to company": "02/16/2023",
            "Company response to consumer": "Closed with monetary relief",
            "Timely response?": "Yes",
            "Consumer disputed?": "No",
            "Complaint ID": "7654321"
        },
        {
            "Date received": "03/10/2023",
            "Product": "Student loan",
            "Sub-product": "Federal student loan servicing",
            "Issue": "Dealing with my lender or servicer",
            "Sub-issue": "Don't agree with fees charged",
            "Consumer complaint narrative": "I was charged late fees even though I submitted my payment on time.",
            "Company public response": None,
            "Company": "Student Loan Corp",
            "State": "NY",
            "ZIP code": "10001",
            "Tags": None,
            "Consumer consent provided?": "Consent provided",
            "Submitted via": "Web",
            "Date sent to company": "03/12/2023",
            "Company response to consumer": "Closed with non-monetary relief",
            "Timely response?": "Yes",
            "Consumer disputed?": "Yes",
            "Complaint ID": "9876543"
        },
        {
            "Date received": "04/22/2023",
            "Product": "Checking or savings account",
            "Sub-product": "Checking account",
            "Issue": "Deposits and withdrawals",
            "Sub-issue": "Unauthorized transactions",
            "Consumer complaint narrative": "I found unauthorized withdrawals from my checking account.",
            "Company public response": "Company helped consumer to understand product",
            "Company": "Big National Bank",
            "State": "FL",
            "ZIP code": "33101",
            "Tags": "Servicemember",
            "Consumer consent provided?": "Consent provided",
            "Submitted via": "Phone",
            "Date sent to company": "04/23/2023",
            "Company response to consumer": "Closed with explanation",
            "Timely response?": "No",
            "Consumer disputed?": "Yes",
            "Complaint ID": "1357924"
        },
        {
            "Date received": "05/30/2023",
            "Product": "Credit reporting",
            "Sub-product": "Credit reporting company",
            "Issue": "Incorrect information on credit report",
            "Sub-issue": "Information belongs to someone else",
            "Consumer complaint narrative": "There is an account on my credit report that does not belong to me.",
            "Company public response": None,
            "Company": "Credit Bureau Inc",
            "State": "IL",
            "ZIP code": "60601",
            "Tags": None,
            "Consumer consent provided?": "Consent provided",
            "Submitted via": "Web",
            "Date sent to company": "06/01/2023",
            "Company response to consumer": "In progress",
            "Timely response?": None,
            "Consumer disputed?": None,
            "Complaint ID": "2468013"
        }
    ]
    
    # Ensure data directory exists
    os.makedirs(config.DATA_DIR, exist_ok=True)
    
    # Save sample data
    sample_data_path = config.DATA_DIR / "sample_complaints.json"
    
    with open(sample_data_path, 'w') as f:
        for record in sample_data:
            f.write(json.dumps(record) + '\n')
    
    print(f"Created sample data at {sample_data_path}")
    return sample_data_path

def main():
    """Main function to run Mission 2"""
    print("Starting CFPB Complaint Processing System - Mission 2")
    
    # 1. Set up logging and Spark session
    logger = setup_logging("mission_2_script")
    logger.info("Starting Mission 2: Data Preparation and Schema Definition")
    
    spark = create_spark_session()
    logger.info(f"Created Spark session with version: {spark.version}")
    
    # 2. Define data schema
    complaint_schema = get_complaint_schema()
    logger.info(f"Defined schema with {len(complaint_schema.fields)} fields")
    
    # 3. Create sample data if needed
    sample_data_path = create_sample_data()
    
    # 4. Load data with schema validation
    df = spark.read.json(str(sample_data_path), schema=complaint_schema)
    logger.info(f"Loaded {df.count()} records from sample data")
    
    # 5. Validate data quality
    quality_metrics = validate_data_quality(df)
    logger.info(f"Data quality validation completed")
    
    # 6. Preprocess date fields
    processed_df = preprocess_dates(df)
    logger.info("Date preprocessing completed")
    
    # 7. Engineer target variable
    df_with_target = engineer_target_variable(processed_df)
    logger.info("Target variable engineering completed")
    
    # 8. Split into training and test sets
    train_df, test_df = split_train_test_data(df_with_target)
    logger.info(f"Data split completed. Training: {train_df.count()}, Test: {test_df.count()}")
    
    # 9. Create data summary
    data_summary = create_data_summary(df_with_target)
    logger.info("Data summary created")
    
    # Display summary
    print("\nData Summary:")
    print(f"Total records: {data_summary['total_records']}")
    
    print("\nProduct Summary:")
    data_summary['product_summary'].show(truncate=False)
    
    print("\nResponse Summary:")
    data_summary['response_summary'].show(truncate=False)
    
    # 10. Save data for future use
    save_dataframes(train_df, test_df)
    logger.info(f"Data saved to disk")
    
    # 11. Create Kafka producer simulation
    simulation_script_path = project_root / "src" / "kafka_producer_simulation.py"
    create_kafka_producer_simulation(test_df, str(simulation_script_path))
    logger.info(f"Kafka producer simulation script created at {simulation_script_path}")
    
    # 12. Clean up
    stop_spark_session(spark)
    logger.info("Completed Mission 2: Data Preparation and Schema Definition")
    
    print("\n==== Mission 2 Results ====")
    print(f"Defined schema with {len(complaint_schema.fields)} fields")
    print(f"Processed {df.count()} sample records")
    print(f"Created target variable 'SuccessfulResolution'")
    print(f"Split data: {train_df.count()} training, {test_df.count()} test")
    print(f"Saved data to:\n  - {config.HISTORICAL_DATA_PATH}\n  - {config.TEST_DATA_PATH}")
    print(f"Created Kafka simulation script at: {simulation_script_path}")
    print("\nMission 2 completed successfully!")

if __name__ == "__main__":
    main() 