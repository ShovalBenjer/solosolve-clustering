# CFPB Complaint Real-time Processing System

This project builds a system to process consumer complaints from the CFPB in real-time, predict resolution outcomes, and visualize key metrics for monitoring and improvement. It uses Apache Kafka for streaming data, Apache Spark (with Spark NLP) for processing and machine learning, and Apache Superset for visualization.

## Project Structure

```
solosolve-clustering/
├── data/                   # Data files
│   ├── sample_complaints.json  # Sample data for testing
│   ├── historical_complaints_75percent.json  # Training data
│   ├── historical_complaints_25percent.json  # Test data
│   └── logs/               # Log files
├── models/                 # Saved models
├── notebooks/              # Jupyter notebooks
│   ├── mission_1_project_setup.ipynb      # Mission 1 notebook
│   └── mission_2_data_preparation.ipynb   # Mission 2 notebook
├── pipelines/              # Preprocessing pipelines
├── src/                    # Python source code
│   ├── config.py           # Configuration settings
│   ├── logging_utils.py    # Logging utilities
│   ├── spark_utils.py      # Spark utilities
│   ├── schema.py           # Data schema definitions
│   ├── data_utils.py       # Data loading and processing utilities
│   ├── kafka_producer_simulation.py  # Kafka simulation
│   └── setup.py            # Setup script
├── mission_1_setup.py      # Mission 1 entry point
├── mission_2_data_preparation.py  # Mission 2 entry point
├── requirements.txt        # Project dependencies
└── README.md               # This file
```

## Setup Instructions

### 1. Install Dependencies

Install the required Python packages:

```bash
pip install -r requirements.txt
```

### 2. Run Mission 1 Setup

Execute the setup script to configure the project:

```bash
python mission_1_setup.py
```

This will:
- Create the necessary directory structure
- Configure logging
- Test Spark session
- Display configuration summary

### 3. Run Mission 2 Data Preparation

Process the data and prepare it for model training:

```bash
python mission_2_data_preparation.py
```

This will:
- Define data schema with all 18 CFPB complaint fields
- Create and load sample complaint data
- Validate data quality
- Preprocess date fields and calculate processing time
- Engineer the 'SuccessfulResolution' target variable
- Split data into training and test sets
- Generate a Kafka producer simulation script

## Development Environment Requirements

- Python 3.8+
- Java 8+ (for Spark)
- Apache Spark 3.3.2
- Apache Kafka
- Apache Superset (for later missions)

## Project Missions

This project is divided into several missions:

1. **Project Setup and Configuration** (Completed)
   - Create configuration files
   - Set up project structure
   - Prepare development environment

2. **Data Preparation and Schema Definition** (Completed)
   - Define data schema
   - Set up data loading mechanisms
   - Prepare historical dataset for training

3. **Preprocessing Pipeline Development** (Future)
   - Create data preprocessing functions
   - Build and test preprocessing pipeline
   - Implement feature extraction and transformation

4. **Model Training** (Future)
   - Implement batch processing for historical data
   - Train logistic regression model
   - Evaluate model performance

5. **Streaming Infrastructure Setup** (Future)
   - Configure Kafka infrastructure
   - Set up streaming data sources
   - Implement checkpoint mechanisms

6. **Real-time Inference System** (Future)
   - Load preprocessing pipeline and trained model
   - Process streaming data from Kafka
   - Apply model and write results back to Kafka

7. **Superset Dashboard Setup** (Future)
   - Install and configure Apache Superset
   - Connect to data sources
   - Configure datasets for visualization

8. **Dashboard Development** (Future)
   - Create visualizations for complaint metrics
   - Implement real-time and historical data blending
   - Build comprehensive monitoring dashboard

9. **System Monitoring and Optimization** (Future)
   - Set up throughput monitoring
   - Implement Kafka monitoring
   - Optimize streaming parameters for performance

10. **Testing and Validation** (Future)
    - Test end-to-end pipeline
    - Validate model accuracy in real-time
    - Ensure system handles target load

## License

[License information would go here] 