#!/bin/bash
# Start the entire CFPB complaint processing pipeline

# Set the base directory
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
cd "$BASE_DIR"

# Create checkpoint directory if it doesn't exist
mkdir -p checkpoint

# Function to check if a process is running
check_process() {
    pgrep -f "$1" > /dev/null
    return $?
}

# Function to start a component in the background
start_component() {
    local name="$1"
    local command="$2"
    local log_file="$3"
    
    echo "Starting $name..."
    eval "$command" > "$log_file" 2>&1 &
    local pid=$!
    sleep 2
    
    if ps -p $pid > /dev/null; then
        echo "$name started with PID $pid"
        return 0
    else
        echo "Failed to start $name. Check $log_file for details."
        return 1
    fi
}

# Start Kafka if not already running
if ! check_process "kafka.Kafka"; then
    # Check if Zookeeper is running, start if not
    if ! check_process "zookeeper.server"; then
        echo "Zookeeper not running. Please start Zookeeper first."
        exit 1
    fi
    
    # Start Kafka
    echo "Kafka not running. Please start Kafka first."
    exit 1
fi

# Create Kafka topics if they don't exist
kafka-topics.sh --create --if-not-exists --topic cfpb-complaints-input --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics.sh --create --if-not-exists --topic cfpb-complaints-output --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Start the Spark streaming job
start_component "Spark streaming job" "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.4 src/spark/streaming.py" "logs/spark.log"
spark_status=$?

# Start the Python backend
start_component "Python backend" "python src/backend/app.py" "logs/backend.log"
backend_status=$?

# Only start the Kafka producer if Spark and backend started successfully
if [ $spark_status -eq 0 ] && [ $backend_status -eq 0 ]; then
    # Start the Kafka producer
    start_component "Kafka producer" "python src/kafka/producer.py" "logs/producer.log"
    producer_status=$?
    
    if [ $producer_status -ne 0 ]; then
        echo "Failed to start the pipeline. Check the logs for details."
        exit 1
    fi
else
    echo "Failed to start Spark or backend. Check the logs for details."
    exit 1
fi

echo "CFPB complaint pipeline started successfully!"
echo "Logs are available in the logs directory."
echo ""
echo "To stop the pipeline, run: scripts/stop_pipeline.sh" 