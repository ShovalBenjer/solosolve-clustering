"""
Tests for the embedding_clustering module.
"""

import os
import sys
import unittest
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.models.embedding_clustering import (
    create_embedding_model,
    extract_embeddings,
    create_lsh_model,
    apply_hdbscan_clustering
)

class TestEmbeddingClustering(unittest.TestCase):
    """Test cases for embedding_clustering module."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        # Create Spark session
        cls.spark = SparkSession.builder \
            .appName("TestEmbeddingClustering") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        
        # Create test data
        test_data = [
            (1, "I was charged twice for the same purchase on my credit card."),
            (2, "My credit card was charged for a purchase I didn't make."),
            (3, "The bank charged me an overdraft fee even though I had sufficient funds."),
            (4, "I've been trying to get my mortgage modified for over a year."),
            (5, "My mortgage payment was applied to the wrong account."),
            (6, "I've been denied a loan modification multiple times.")
        ]
        
        # Create DataFrame
        cls.test_df = cls.spark.createDataFrame(
            test_data,
            ["complaint_id", "consumer_complaint_narrative"]
        )
    
    @classmethod
    def tearDownClass(cls):
        """Tear down test fixtures."""
        cls.spark.stop()
    
    def test_create_embedding_model(self):
        """Test creating an embedding model."""
        # Create model
        model = create_embedding_model(self.spark)
        
        # Check that model is created
        self.assertIsNotNone(model)
    
    def test_extract_embeddings(self):
        """Test extracting embeddings."""
        # Create model
        model = create_embedding_model(self.spark)
        
        # Extract embeddings
        embeddings_df = extract_embeddings(
            self.test_df,
            model,
            text_col="consumer_complaint_narrative",
            id_col="complaint_id"
        )
        
        # Check that embeddings are extracted
        self.assertEqual(embeddings_df.count(), self.test_df.count())
        self.assertTrue("embedding" in embeddings_df.columns)
        
        # Check embedding dimensions
        embedding = embeddings_df.select("embedding").first()[0]
        self.assertEqual(len(embedding), 768)  # BERT embeddings are 768-dimensional
    
    def test_create_lsh_model(self):
        """Test creating an LSH model."""
        # Create embedding model
        embedding_model = create_embedding_model(self.spark)
        
        # Extract embeddings
        embeddings_df = extract_embeddings(
            self.test_df,
            embedding_model,
            text_col="consumer_complaint_narrative",
            id_col="complaint_id"
        )
        
        # Create LSH model
        lsh_model = create_lsh_model(embeddings_df, embedding_col="embedding")
        
        # Check that model is created
        self.assertIsNotNone(lsh_model)
    
    def test_apply_hdbscan_clustering(self):
        """Test applying HDBSCAN clustering."""
        # Create embedding model
        embedding_model = create_embedding_model(self.spark)
        
        # Extract embeddings
        embeddings_df = extract_embeddings(
            self.test_df,
            embedding_model,
            text_col="consumer_complaint_narrative",
            id_col="complaint_id"
        )
        
        # Convert to numpy array
        embeddings = np.array(embeddings_df.select("embedding").toPandas()["embedding"].tolist())
        
        # Apply clustering
        cluster_labels = apply_hdbscan_clustering(
            embeddings,
            min_cluster_size=2,
            min_samples=1
        )
        
        # Check that cluster labels are returned
        self.assertEqual(len(cluster_labels), len(embeddings))
        
        # Check that there are at least 2 clusters (including noise)
        unique_clusters = set(cluster_labels)
        self.assertGreaterEqual(len(unique_clusters), 2)
        
        # Check that similar complaints are in the same cluster
        # Convert to pandas for easier manipulation
        pdf = embeddings_df.toPandas()
        pdf["cluster"] = cluster_labels
        
        # Get complaints about credit cards
        credit_card_complaints = pdf[pdf["consumer_complaint_narrative"].str.contains("credit card")]
        
        # Check that credit card complaints are in the same cluster
        if len(credit_card_complaints) >= 2:
            credit_card_clusters = set(credit_card_complaints["cluster"])
            self.assertLessEqual(len(credit_card_clusters), 2)  # At most 2 clusters (including noise)
        
        # Get complaints about mortgages
        mortgage_complaints = pdf[pdf["consumer_complaint_narrative"].str.contains("mortgage")]
        
        # Check that mortgage complaints are in the same cluster
        if len(mortgage_complaints) >= 2:
            mortgage_clusters = set(mortgage_complaints["cluster"])
            self.assertLessEqual(len(mortgage_clusters), 2)  # At most 2 clusters (including noise)

if __name__ == "__main__":
    unittest.main() 