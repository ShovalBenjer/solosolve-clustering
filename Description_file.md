1. Introduction
Purpose
This project aims to develop an AI-powered complaint management system to automate the processing
of customer complaints for online delivery services. By leveraging advanced Natural Language
Processing (NLP) and decision-making algorithms, the system will extract key details, apply businessspecific compensation rules, and resolve issues efficiently with minimal human intervention.
Scope
The system will automate complaints submitted via app and email, streamlining information
extraction, rule-based decision-making, and resolution processes. Manual complaint handling and
chatbot-based interactions are explicitly out of scope.
Target Audience
• Delivery service providers (e.g., Wolt, 10bis, amazon) aiming to:
• Reduce complaint resolution times.
• Enhance customer satisfaction through streamlined automation.
Key Performance Indicators (KPIs):
• Reduce average resolution time by 40%.
• Achieve a 90% first-time resolution rate.
• Increase customer satisfaction ratings by 20% within six months of deployment.
2. Functional Requirements
2.1 Possible data sources:
https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023
https://opendata.fcc.gov/Consumer/CGB-Consumer-Complaints-Data/3xyp-aqkj/about_data
https://brightdata.com/products/datasets/yelp/reviews
https://cfpb.github.io/api/ccdb/api.html

2.2 Complaint Classification and Prioritization
Use a system to classify complaints into predefined categories and prioritize them based on urgency or
severity indicators (e.g., sentiment analysis or predefined rules).

2.3 Data Extraction and Analysis
Use text processing techniques to extract relevant details (e.g., order numbers, customer names) from
complaints to streamline resolution.

2.4 Performance Metrics for Customer Satisfaction
2.4.1 Average Resolution Time: Measure the time taken to resolve complaints.
2.4.2 Customer Feedback Scores: Collect and aggregate customer feedback to evaluate satisfaction
levels post-resolution.
2.4.3 Complaint Closure Rate: Calculate the percentage of complaints resolved within specified
timeframes.
2.4.4 Recurring Complaint Trends: Analyze patterns in recurring complaints to identify systemic
issues.
2.4.5 Sentiment Improvement: Track shifts in sentiment from initial complaint submission to
resolution, leveraging NLP techniques for sentiment analysis.

2.5 Batch Complaint Processing
Enable bulk upload and processing of complaints for businesses dealing with high complaint volumes,
using standardized formats such as CSV or Excel.

2.6 Sentiment Trends and Analytics
Provide visualization tools to monitor sentiment trends over time, helping businesses understand
customer sentiment and act proactively

5. User Personas
• Customer: Needs quick and accurate resolutions to complaints.
• Business Manager: Requires efficient complaint handling to improve customer satisfaction and
reduce operational costs.
• Customer Support Agent: Aims to resolve complex complaints escalated by the system.

6. Design Principles
User-Centric: Provide an intuitive and responsive experience for users.
Scalability: Ensure the system can handle increasing complaint volumes seamlessly.
Efficiency: Minimize processing times to enhance customer satisfaction.
Proactivity: Identify and address recurring issues before escalation.

7. Non-Functional Requirements
• Performance: Process 90% of complaints within 2 seconds.
• Scalability: Handle up to 10,000 complaints daily.
• Security: Ensure GDPR-compliant data handling and storage.
• Supports hundreds of concurrent requests without degradation.

cfpb-complaint-clustering/
├── notebooks/
│   ├── setup.ipynb           # Environment setup
│   ├── eda.ipynb             # AutoViz-powered EDA
│   └── main.ipynb            # Main execution notebook
├── src/
│   ├── kafka/
│   │   ├── producer.py       # Kafka producer for complaints
│   │   └── consumer.py       # Kafka consumer for processed data
│   ├── spark/
│   │   ├── batch.py          # Batch processing for 80% training data
│   │   └── streaming.py      # Structured Streaming pipeline
│   ├── models/
│   │   ├── embedding.py      # ModernBERT embeddings
│   │   └── clustering.py     # HDBSCAN clustering with LSH
│   ├── utils/
│   │   ├── preprocessing.py  # Text preprocessing with TransmogrifAI-like features
│   │   └── data_utils.py     # Data loading and parsing
│   └── visualization/
│       └── dashboard.py      # MySQL and Superset connection
├── config/
│   └── config.py             # Configuration parameters
├── scripts/
│   ├── setup_mysql.sql       # MySQL setup script
│   └── setup_superset.py     # Superset dashboard setup
└── requirements.txt          # Python dependencies

