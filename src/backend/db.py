"""
MySQL database utility for the CFPB complaint backend.
"""
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

# Add parent directory to Python path to import config module
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from config.config import DB_URI

class Database:
    """Database connection and query utility."""
    
    def __init__(self):
        """Initialize the database connection."""
        self.engine = create_engine(DB_URI)
    
    def get_connection(self):
        """Get a SQLAlchemy connection."""
        return self.engine.connect()
    
    def execute_query(self, query, params=None):
        """Execute a query and return the result as a DataFrame."""
        with self.get_connection() as conn:
            return pd.read_sql(text(query), conn, params=params)
    
    def insert_dataframe(self, df, table_name, if_exists='append'):
        """Insert a DataFrame into a table."""
        df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
    
    def get_recent_complaints(self, hours=1):
        """Get recent complaints for analysis."""
        query = f"""
        SELECT 
            cluster_id,
            resolution,
            COUNT(*) AS complaint_count,
            SUM(CASE WHEN timely_response = 1 THEN 1 ELSE 0 END) AS timely_responses,
            SUM(CASE WHEN consumer_disputed = 1 THEN 1 ELSE 0 END) AS disputed_complaints
        FROM processed_complaints
        WHERE created_at > DATE_SUB(NOW(), INTERVAL {hours} HOUR)
        GROUP BY cluster_id, resolution
        """
        return self.execute_query(query)
    
    def get_complaints_by_cluster(self, cluster_id, limit=10):
        """Get complaints for a specific cluster."""
        query = """
        SELECT 
            complaint_id,
            narrative,
            resolution,
            timely_response,
            consumer_disputed,
            created_at
        FROM processed_complaints
        WHERE cluster_id = :cluster_id
        ORDER BY created_at DESC
        LIMIT :limit
        """
        return self.execute_query(query, params={"cluster_id": cluster_id, "limit": limit})
    
    def get_cluster_stats(self):
        """Get statistics for each cluster."""
        query = """
        SELECT 
            cluster_id,
            resolution,
            COUNT(*) AS total_complaints,
            SUM(CASE WHEN timely_response = 1 THEN 1 ELSE 0 END) AS timely_responses,
            SUM(CASE WHEN consumer_disputed = 1 THEN 1 ELSE 0 END) AS disputed_complaints,
            (SUM(CASE WHEN timely_response = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS timely_percentage,
            (SUM(CASE WHEN consumer_disputed = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS disputed_percentage
        FROM processed_complaints
        GROUP BY cluster_id, resolution
        ORDER BY total_complaints DESC
        """
        return self.execute_query(query)
    
    def close(self):
        """Close the database connection."""
        self.engine.dispose() 