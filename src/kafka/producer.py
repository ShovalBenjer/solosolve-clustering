"""
Kafka producer for CFPB complaint data.
This script reads complaint data from a CSV file and publishes it to a Kafka topic.
"""
import csv
import json
import sys
import time
from pathlib import Path
import random
from datetime import datetime

from kafka import KafkaProducer

# Add parent directory to Python path to import config module
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_INPUT_TOPIC,
    SAMPLE_DATA_PATH
)

def read_complaints(file_path):
    """
    Read complaint data from a CSV file.
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        list: List of complaint dictionaries
    """
    complaints = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Only include complaints with narratives and consumer consent
                if (row.get('consumer_complaint_narrative') and 
                    row.get('consumer_consent_provided') == 'Consent provided'):
                    complaints.append(row)
        print(f"Read {len(complaints)} valid complaints from {file_path}")
        return complaints
    except Exception as e:
        print(f"Error reading complaints file: {e}")
        # If file doesn't exist or has issues, create a few sample complaints
        return create_sample_complaints()

def create_sample_complaints():
    """Create sample complaints if the CSV file is not available."""
    sample_narratives = [
        "I made a purchase of $500 at XYZ store, but the charge on my statement is showing as $750.",
        "I have been trying to make my mortgage payment online but keep getting an error message.",
        "I opened a new checking account and was told there would be no monthly maintenance fee.",
        "I received a collection notice for $3,500 on a credit card I paid off over two years ago.",
        "I disputed an error on my credit report with TransUnion in November 2022.",
        "I noticed a charge on my credit card for $89.99 monthly subscription that I never signed up for.",
        "My student loan servicer told me I qualified for Public Service Loan Forgiveness after 5 years of payments.",
        "I lost my job due to COVID-19 in December and have been unable to make my full mortgage payment.",
        "I took out a payday loan for $300 with what I thought was a one-time fee.",
        "I was approved for an auto loan at 3.9% interest rate, but when I received the final paperwork, the rate was 7.25%."
    ]
    
    complaints = []
    for i, narrative in enumerate(sample_narratives):
        complaints.append({
            'complaint_id': f"SAMPLE_{i}",
            'date_received': datetime.now().strftime('%Y-%m-%d'),
            'consumer_complaint_narrative': narrative,
            'consumer_consent_provided': 'Consent provided',
            'timely_response': random.choice(['Yes', 'No']),
            'consumer_disputed': random.choice(['Yes', 'No'])
        })
    
    print(f"Created {len(complaints)} sample complaints")
    return complaints

def produce_complaints(complaints):
    """
    Produce complaints to Kafka topic.
    
    Args:
        complaints (list): List of complaint dictionaries
    """
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    
    print(f"Connected to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Publishing to topic: {KAFKA_INPUT_TOPIC}")
    
    # Send each complaint to Kafka with a delay
    for complaint in complaints:
        # Use complaint_id as the key for partitioning
        key = complaint.get('complaint_id', str(random.randint(1000, 9999)))
        
        # Send only necessary fields to Kafka
        message = {
            'complaint_id': complaint.get('complaint_id', ''),
            'date_received': complaint.get('date_received', ''),
            'product': complaint.get('product', ''),
            'issue': complaint.get('issue', ''),
            'consumer_complaint_narrative': complaint.get('consumer_complaint_narrative', ''),
            'company': complaint.get('company', ''),
            'state': complaint.get('state', ''),
            'timely_response': complaint.get('timely_response', '') == 'Yes',
            'consumer_disputed': complaint.get('consumer_disputed', '') == 'Yes',
            'timestamp': int(time.time() * 1000)  # Add timestamp for windowing
        }
        
        producer.send(KAFKA_INPUT_TOPIC, key=key, value=message)
        print(f"Sent complaint {key}")
        
        # Simulate real-time data by adding a delay
        time.sleep(random.uniform(0.5, 2.0))
    
    # Flush and close the producer
    producer.flush()
    producer.close()
    print("All complaints sent to Kafka")

def main():
    print("Starting CFPB Complaint Kafka Producer")
    
    # Read complaints from CSV file
    complaints = read_complaints(SAMPLE_DATA_PATH)
    
    # Produce complaints to Kafka
    if complaints:
        produce_complaints(complaints)
    else:
        print("No valid complaints found")

if __name__ == "__main__":
    main() 