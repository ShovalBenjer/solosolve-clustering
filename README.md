# CFPB Complaint Clustering

A real-time complaint clustering system that processes Consumer Financial Protection Bureau complaints using Apache Kafka, Apache Spark, ModernBERT embeddings, and HDBSCAN clustering, with visualization via Superset dashboards.

## Architecture

```
+---------------------+     +-----------------+     +-------------------------------------+     +---------------------+     +---------------------+
| CFPB Data (Local)  | --> | Kafka Producer  | --> | Spark Structured Streaming (Consumer)| --> | Kafka Output Topic  | --> | Python Backend      |
+---------------------+     +-----------------+     +-------------------------------------+     +---------------------+     +---------------------+
                                                                 ^                                                        |
                                                                 |                                                        v
                                                       (ModernBERT Embeddings via Spark NLP)                           +---------------------+
                                                                 |                                                        | MySQL Database      |
                                                                 v                                                        +---------------------+
                                                       (HDBSCAN Clustering via Pandas UDF + LSH)                                ^
                                                                 |                                                        |
                                                                 v                                                        |
                                                         (Resolution Lookup - In-memory Map)                              |
                                                                                                                         |
                                                                                                                         | (Superset Polling)
                                                                                                                         |
                                                                                                                         v
                                                                                                                     +---------------------+
                                                                                                                     | Superset Dashboard  |
                                                                                                                     +---------------------+
```

## Project Structure

```
.
├── config/                 # Configuration files
├── data/                   # Sample CFPB data
├── scripts/                # Utility scripts
└── src/
    ├── kafka/              # Kafka producer
    ├── spark/              # Spark streaming application
    └── backend/            # Python backend for MySQL and Superset
```

## Setup

### Prerequisites

- Java 8+
- Python 3.8+
- Apache Kafka
- Apache Spark 3.x
- MySQL
- Superset
- PySpark
- Spark NLP

### Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up Kafka:
```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka
bin/kafka-server-start.sh config/server.properties

# Create topics
bin/kafka-topics.sh --create --topic cfpb-complaints-input --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic cfpb-complaints-output --bootstrap-server localhost:9092
```

3. Set up MySQL:
```bash
# Create database and tables (see scripts/setup_mysql.sql)
```

4. Configure Superset to connect to MySQL.

## Usage

1. Start the data pipeline:
```bash
# Start Kafka producer
python src/kafka/producer.py

# Start Spark streaming job
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4 src/spark/streaming.py

# Start Python backend
python src/backend/app.py
```

2. Access the Superset dashboard at http://localhost:8088

## License

MIT 