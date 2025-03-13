"""
Tests for the data_utils module.
"""

import os
import sys
import unittest
import tempfile
import pandas as pd
from pyspark.sql import SparkSession

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.data_utils import (
    load_cfpb_data,
    preprocess_complaints,
    split_train_stream,
    save_streaming_data,
    load_streaming_data
)

class TestDataUtils(unittest.TestCase):
    """Test cases for data_utils module."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        # Create Spark session
        cls.spark = SparkSession.builder \
            .appName("TestDataUtils") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        
        # Create test data
        test_data = [
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
            },
            {
                "complaint_id": "3",
                "date_received": "2023-01-03",
                "product": "Credit card",
                "issue": "Fraud",
                "consumer_complaint_narrative": "My credit card was used for unauthorized purchases.",
                "company": "ACME Bank",
                "state": "NY",
                "timely_response": "No",
                "consumer_disputed": "Yes",
                "consumer_consent_provided": "Consent provided"
            },
            {
                "complaint_id": "4",
                "date_received": "2023-01-04",
                "product": "Checking account",
                "issue": "Fees",
                "consumer_complaint_narrative": "I was charged excessive fees for overdrafts.",
                "company": "Big Bank",
                "state": "FL",
                "timely_response": "Yes",
                "consumer_disputed": "No",
                "consumer_consent_provided": "Consent provided"
            },
            {
                "complaint_id": "5",
                "date_received": "2023-01-05",
                "product": "Mortgage",
                "issue": "Loan modification",
                "consumer_complaint_narrative": "I've been trying to get my mortgage modified for over a year.",
                "company": "Home Loans Inc",
                "state": "CA",
                "timely_response": "No",
                "consumer_disputed": "Yes",
                "consumer_consent_provided": "Consent provided"
            }
        ]
        
        # Create DataFrame
        cls.test_df = cls.spark.createDataFrame(test_data)
        
        # Create temporary directory for test files
        cls.temp_dir = tempfile.TemporaryDirectory()
    
    @classmethod
    def tearDownClass(cls):
        """Tear down test fixtures."""
        cls.spark.stop()
        cls.temp_dir.cleanup()
    
    def test_load_cfpb_data(self):
        """Test loading CFPB data."""
        # Test with default parameters
        df = load_cfpb_data(self.spark)
        
        # Check that data is loaded
        self.assertIsNotNone(df)
        self.assertGreater(df.count(), 0)
        
        # Check required columns
        required_columns = [
            "complaint_id",
            "consumer_complaint_narrative",
            "consumer_consent_provided"
        ]
        for col in required_columns:
            self.assertIn(col, df.columns)
    
    def test_preprocess_complaints(self):
        """Test preprocessing complaints."""
        # Preprocess data
        preprocessed_df = preprocess_complaints(self.test_df)
        
        # Check that data is preprocessed
        self.assertIsNotNone(preprocessed_df)
        
        # Check that only complaints with narratives and consent are kept
        self.assertEqual(preprocessed_df.count(), self.test_df.count())
        
        # Check that date columns are converted
        if "date_received" in preprocessed_df.columns:
            date_type = preprocessed_df.schema["date_received"].dataType.typeName()
            self.assertEqual(date_type, "date")
        
        # Check that boolean columns are converted
        if "timely_response" in preprocessed_df.columns:
            timely_response = preprocessed_df.select("timely_response").first()[0]
            self.assertIsInstance(timely_response, bool)
        
        if "consumer_disputed" in preprocessed_df.columns:
            consumer_disputed = preprocessed_df.select("consumer_disputed").first()[0]
            self.assertIsInstance(consumer_disputed, bool)
        
        # Check that timestamp column is added
        self.assertIn("timestamp", preprocessed_df.columns)
    
    def test_split_train_stream(self):
        """Test splitting data into training and streaming sets."""
        # Split data
        train_df, stream_df = split_train_stream(self.test_df, train_ratio=0.6, seed=42)
        
        # Check that data is split
        self.assertIsNotNone(train_df)
        self.assertIsNotNone(stream_df)
        
        # Check that all data is accounted for
        self.assertEqual(train_df.count() + stream_df.count(), self.test_df.count())
        
        # Check approximate split ratio
        train_count = train_df.count()
        total_count = self.test_df.count()
        ratio = train_count / total_count
        self.assertAlmostEqual(ratio, 0.6, delta=0.2)  # Allow some deviation due to small dataset
    
    def test_save_and_load_streaming_data(self):
        """Test saving and loading streaming data."""
        # Create output path
        output_path = os.path.join(self.temp_dir.name, "streaming_data")
        
        # Save data
        save_streaming_data(self.test_df, output_path)
        
        # Check that data is saved
        self.assertTrue(os.path.exists(output_path))
        
        # Load data
        loaded_df = load_streaming_data(self.spark, output_path)
        
        # Check that data is loaded
        self.assertIsNotNone(loaded_df)
        self.assertEqual(loaded_df.count(), self.test_df.count())
        
        # Check that columns are preserved
        for col in self.test_df.columns:
            self.assertIn(col, loaded_df.columns)

if __name__ == "__main__":
    unittest.main() 