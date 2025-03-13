README.md:

A real-time clustering system for Consumer Financial Protection Bureau (CFPB) complaints using ModernBERT embeddings and HDBSCAN clustering.

## Overview

This system processes consumer complaints from the CFPB database, generates embeddings using ModernBERT, and applies density-based clustering to identify patterns and anomalies. It supports both batch processing for historical data and real-time streaming for new complaints.

### Key Features

- **ModernBERT Embeddings**: Transforms complaint text into high-dimensional vector representations
- **HDBSCAN Clustering**: Identifies natural clusters in the embedding space without specifying the number of clusters
- **LSH Optimization**: Uses Locality-Sensitive Hashing for efficient approximate nearest neighbor search
- **Real-time Processing**: Processes new complaints as they arrive using Spark Structured Streaming
- **Interactive Visualization**: Provides interactive visualizations of clusters and insights
- **Anomaly Detection**: Identifies outlier complaints that don't fit into any cluster

## System Architecture

The system consists of the following components:

1. **Data Loading and Preprocessing**: Loads and preprocesses CFPB complaint data from various sources
2. **Embedding Generation**: Generates embeddings from complaint text using ModernBERT
3. **Clustering**: Applies HDBSCAN clustering to the embeddings
4. **Streaming Pipeline**: Processes new complaints in real-time
5. **Visualization**: Provides interactive visualizations of clusters and insights
6. **Dashboard Integration**: Connects with Apache Superset for interactive dashboards

## Directory Structure

```
solosolve-clustering/
├── data/                      # Data directory
│   ├── raw/                   # Raw data
│   ├── processed/             # Processed data
│   ├── streaming/             # Streaming data
│   ├── output/                # Output data
│   └── checkpoints/           # Checkpoints for streaming
├── notebooks/                 # Jupyter notebooks
│   ├── 01_data_exploration.ipynb
│   ├── 02_embedding_generation.ipynb
│   ├── 03_clustering.ipynb
│   └── 04_streaming_demo.ipynb
├── src/                       # Source code
│   ├── models/                # Model implementations
│   │   └── embedding_clustering.py
│   ├── utils/                 # Utility functions
│   │   ├── data_utils.py
│   │   ├── viz_utils.py
│   │   └── stream_utils.py
│   └── streaming/             # Streaming pipeline
│       └── streaming_pipeline.py
├── tests/                     # Tests
│   ├── test_embedding_clustering.py
│   ├── test_data_utils.py
│   └── test_streaming.py
├── requirements.txt           # Dependencies
└── README.md                  # This file
```

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/solosolve-clustering.git
   cd solosolve-clustering
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Download CFPB data (optional):
   ```bash
   mkdir -p data/raw
   python -c "from src.utils.data_utils import get_cfpb_api_data; df = get_cfpb_api_data(limit=10000); df.to_csv('data/raw/cfpb_complaints.csv', index=False)"
   ```

## Step-by-Step Execution Guide

### 1. Data Exploration and Preprocessing

Start by exploring the data and preprocessing it for further analysis:

```bash
# Navigate to the notebooks directory
cd notebooks

# Start Jupyter Notebook
jupyter notebook
```

Open `01_data_exploration.ipynb` and run all cells to:
- Load CFPB complaint data
- Preprocess the data
- Analyze complaint distributions
- Save preprocessed data for the next steps

### 2. Embedding Generation

Next, generate embeddings for the complaint text:

Open `02_embedding_generation.ipynb` and run all cells to:
- Load preprocessed data
- Create an embedding model
- Generate embeddings for each complaint
- Save embeddings for clustering

### 3. Clustering

Apply clustering to the embeddings:

Open `03_clustering.ipynb` and run all cells to:
- Load embeddings
- Apply HDBSCAN clustering
- Visualize clusters
- Analyze cluster characteristics
- Save clustering results

### 4. Streaming Pipeline

Run the streaming pipeline to process new complaints in real-time:

```bash
# Create simulated streaming data
python -c "
from pyspark.sql import SparkSession
from src.utils.data_utils import load_cfpb_data, split_train_stream
from src.utils.stream_utils import simulate_streaming

spark = SparkSession.builder.appName('Simulate Streaming').getOrCreate()
df = load_cfpb_data(spark, source_type='csv', source_path='data/raw/cfpb_complaints.csv')
_, stream_df = split_train_stream(df, train_ratio=0.8)
simulate_streaming(stream_df, 'data/streaming', batch_size=10, interval_seconds=5)
spark.stop()
"

# Run the streaming pipeline
python -c "
from src.streaming.streaming_pipeline import run_streaming_pipeline

input_options = {
    'data_path': 'data/streaming',
    'format_type': 'parquet'
}

run_streaming_pipeline(
    input_source='file',
    input_options=input_options,
    output_path='data/output/streaming',
    checkpoint_path='data/checkpoints/streaming',
    batch_interval='10 seconds',
    await_termination=False
)
"
```

### 5. Setting Up Superset for Visualization

#### Install and Configure Superset

1. Install Superset:
   ```bash
   pip install apache-superset
   ```

2. Initialize Superset database:
   ```bash
   superset db upgrade
   ```

3. Create an admin user:
   ```bash
   superset fab create-admin \
      --username admin \
      --firstname Admin \
      --lastname User \
      --email admin@example.com \
      --password admin
   ```

4. Load examples (optional):
   ```bash
   superset load_examples
   ```

5. Initialize Superset:
   ```bash
   superset init
   ```

6. Start Superset server:
   ```bash
   superset run -p 8088 --with-threads --reload --debugger
   ```

7. Access Superset at http://localhost:8088 and log in with the admin credentials

#### Connect Superset to Clustering Results

1. In Superset, go to "Data" > "Databases" and add a new database connection
   - For local files, you can use SQLite:
     ```
     sqlite:///path/to/solosolve-clustering/data/output/report/clustering_results.db
     ```
     
2. Create a SQLite database with clustering results:
   ```bash
   python -c "
   import sqlite3
   import pandas as pd
   import numpy as np
   import os
   
   # Create directory for SQLite database
   os.makedirs('data/output/report', exist_ok=True)
   
   # Load clustering results
   clustered_data = pd.read_csv('data/output/clustering_results.csv')
   
   # Connect to SQLite database
   conn = sqlite3.connect('data/output/report/clustering_results.db')
   
   # Write data to database
   clustered_data.to_sql('clustering_results', conn, index=False, if_exists='replace')
   
   # Close connection
   conn.close()
   
   print('Created SQLite database with clustering results')
   "
   ```

3. In Superset, go to "Data" > "Datasets" and add a new dataset
   - Select the database you added
   - Choose the "clustering_results" table

4. Create dashboards:
   - Create a scatter plot of the reduced embeddings colored by cluster
   - Create a bar chart of complaint counts by cluster
   - Create a word cloud for each cluster
   - Create a heatmap of cluster characteristics

## Usage Examples

### Batch Processing

To run the batch processing pipeline:

```python
from pyspark.sql import SparkSession
from src.models.embedding_clustering import create_embedding_model, extract_embeddings, apply_hdbscan_clustering
from src.utils.data_utils import load_cfpb_data, preprocess_complaints

# Create Spark session
spark = SparkSession.builder.appName("CFPB Complaint Clustering").getOrCreate()

# Load and preprocess data
df = load_cfpb_data(spark, source_type="csv", source_path="data/raw/cfpb_complaints.csv")
preprocessed_df = preprocess_complaints(df)

# Create embedding model and extract embeddings
embedding_model = create_embedding_model(spark)
embeddings_df = extract_embeddings(preprocessed_df, embedding_model)

# Apply clustering
embeddings = embeddings_df.toPandas()["embedding"].tolist()
cluster_labels = apply_hdbscan_clustering(embeddings)

# Print results
print(f"Number of clusters: {len(set(cluster_labels)) - (1 if -1 in cluster_labels else 0)}")
print(f"Number of noise points: {list(cluster_labels).count(-1)}")
```

### Streaming Processing

To run the streaming pipeline:

```python
from src.streaming.streaming_pipeline import run_streaming_pipeline

# Configure input options
input_options = {
    "data_path": "data/streaming",
    "format_type": "parquet"
}

# Run streaming pipeline
run_streaming_pipeline(
    input_source="file",
    input_options=input_options,
    output_path="data/output/streaming",
    checkpoint_path="data/checkpoints/streaming",
    batch_interval="10 seconds",
    await_termination=True
)
```

### Visualization

To visualize the clustering results:

```python
import numpy as np
import pandas as pd
from src.utils.viz_utils import reduce_dimensions, plot_clusters, extract_cluster_keywords, generate_cluster_report

# Load embeddings and cluster labels
embeddings = np.load("data/processed/embeddings.npy")
cluster_labels = np.load("data/processed/cluster_labels.npy")
texts = pd.read_csv("data/processed/complaints.csv")["consumer_complaint_narrative"].tolist()

# Reduce dimensions for visualization
reduced_embeddings = reduce_dimensions(embeddings, method="umap")

# Plot clusters
html = plot_clusters(reduced_embeddings, cluster_labels, interactive=True)

# Extract keywords for each cluster
keywords = extract_cluster_keywords(texts, cluster_labels)

# Generate comprehensive report
report = generate_cluster_report(texts, embeddings, cluster_labels, output_path="data/output/report")
```

## Troubleshooting

- **Memory Issues**: If you encounter memory errors, try reducing the batch size or the number of complaints processed
- **Spark Connection Issues**: Make sure your Spark installation is properly configured
- **Missing Dependencies**: Ensure all dependencies are installed with `pip install -r requirements.txt`
- **Superset Connection Issues**: Verify that your database connection string is correct

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- CFPB for providing the complaint data
- Hugging Face for the transformer models
- The HDBSCAN authors for the clustering algorithm
- Apache Spark for the distributed computing framework
- Apache Superset for the visualization dashboard
