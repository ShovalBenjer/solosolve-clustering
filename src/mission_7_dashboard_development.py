"""
Mission 7: Dashboard Development for CFPB Complaint Processing System

This script provides utilities for developing and managing dashboards for the
CFPB Complaint Processing System using Apache Superset. It includes:
1. Sample SQL queries for dashboard charts
2. Dashboard export/import functionality
3. Chart template generation

This is a companion script to mission_7_superset_setup.py
"""

import os
import sys
import json
import logging
import time
from pathlib import Path
import argparse
import requests
from typing import Dict, List, Optional

# Add parent directory to path for imports
sys.path.append(str(Path(os.path.dirname(os.path.abspath(__file__))).parent))

# Import project modules
from src import config
from src.logging_utils import setup_logging

# Set up logging
logger = setup_logging(log_level=config.LOG_LEVEL)

# Superset configuration
SUPERSET_PORT = 8088
SUPERSET_BASE_URL = f"http://localhost:{SUPERSET_PORT}"
DASHBOARD_DIR = config.BASE_DIR / "dashboards"

# Create dashboard directory if it doesn't exist
os.makedirs(DASHBOARD_DIR, exist_ok=True)

def generate_chart_templates():
    """
    Generate SQL query templates for common dashboard charts.
    """
    logger.info("Generating chart templates")
    
    chart_templates = {
        "complaints_by_product": {
            "name": "Complaints by Product",
            "query": """
                SELECT 
                    "Product" AS product,
                    COUNT(*) AS complaint_count
                FROM 
                    "historical_complaints"
                GROUP BY 
                    "Product"
                ORDER BY 
                    complaint_count DESC
                LIMIT 10
            """,
            "visualization_type": "PIE_CHART",
            "description": "Distribution of complaints by product category"
        },
        "complaints_over_time": {
            "name": "Complaints Over Time",
            "query": """
                SELECT 
                    DATE_TRUNC('month', "Date received") AS month,
                    "Product" AS product,
                    COUNT(*) AS complaint_count
                FROM 
                    "historical_complaints"
                GROUP BY 
                    month, product
                ORDER BY 
                    month, product
            """,
            "visualization_type": "LINE_CHART",
            "description": "Trend of complaints received over time by product"
        },
        "complaints_by_state": {
            "name": "Complaints by State",
            "query": """
                SELECT 
                    "State" AS state,
                    COUNT(*) AS complaint_count
                FROM 
                    "historical_complaints"
                WHERE 
                    "State" IS NOT NULL
                GROUP BY 
                    "State"
                ORDER BY 
                    complaint_count DESC
            """,
            "visualization_type": "US_MAP",
            "description": "Geographic distribution of complaints across states"
        },
        "escalation_rate_by_product": {
            "name": "Escalation Rate by Product",
            "query": """
                SELECT 
                    "Product" AS product,
                    COUNT(*) AS total_complaints,
                    SUM(CASE WHEN escalate = 'Yes' THEN 1 ELSE 0 END) AS escalated_complaints,
                    SUM(CASE WHEN escalate = 'Yes' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS escalation_rate
                FROM 
                    "predictions"
                GROUP BY 
                    "Product"
                HAVING 
                    COUNT(*) > 10
                ORDER BY 
                    escalation_rate DESC
            """,
            "visualization_type": "BAR_CHART",
            "description": "Percentage of complaints escalated by product category"
        },
        "high_urgency_complaints": {
            "name": "High Urgency Complaints",
            "query": """
                SELECT 
                    "Complaint ID" AS complaint_id,
                    "Product" AS product,
                    "Issue" AS issue,
                    "State" AS state,
                    prediction,
                    escalate,
                    urgency_level,
                    recommended_action,
                    "Date received" AS date_received
                FROM 
                    "predictions"
                WHERE 
                    urgency_level = 'High'
                ORDER BY 
                    prediction DESC
                LIMIT 100
            """,
            "visualization_type": "TABLE",
            "description": "List of complaints with high urgency level"
        },
        "average_prediction_by_issue": {
            "name": "Average Prediction by Issue",
            "query": """
                SELECT 
                    "Issue" AS issue,
                    COUNT(*) AS complaint_count,
                    AVG(prediction) AS avg_prediction
                FROM 
                    "predictions"
                GROUP BY 
                    "Issue"
                HAVING 
                    COUNT(*) > 5
                ORDER BY 
                    avg_prediction DESC
            """,
            "visualization_type": "BAR_CHART",
            "description": "Average prediction score by complaint issue"
        },
        "complaint_processing_kpi": {
            "name": "Complaint Processing KPI",
            "query": """
                SELECT 
                    COUNT(*) AS total_processed,
                    SUM(CASE WHEN escalate = 'Yes' THEN 1 ELSE 0 END) AS total_escalated,
                    SUM(CASE WHEN urgency_level = 'High' THEN 1 ELSE 0 END) AS high_urgency,
                    AVG(prediction) AS avg_prediction
                FROM 
                    "predictions"
            """,
            "visualization_type": "BIG_NUMBER_TOTAL",
            "description": "Key performance indicators for complaint processing"
        },
        "real_time_prediction_distribution": {
            "name": "Real-time Prediction Distribution",
            "query": """
                WITH prediction_bins AS (
                    SELECT
                        CASE 
                            WHEN prediction < 0.3 THEN 'Low (0.0-0.3)'
                            WHEN prediction < 0.7 THEN 'Medium (0.3-0.7)'
                            ELSE 'High (0.7-1.0)'
                        END AS prediction_range,
                        COUNT(*) AS count
                    FROM "predictions"
                    GROUP BY prediction_range
                )
                SELECT 
                    prediction_range,
                    count,
                    count * 100.0 / SUM(count) OVER () AS percentage
                FROM 
                    prediction_bins
                ORDER BY 
                    prediction_range
            """,
            "visualization_type": "PIE_CHART",
            "description": "Distribution of prediction scores in real-time data"
        }
    }
    
    # Save chart templates to file
    templates_file = DASHBOARD_DIR / "chart_templates.json"
    with open(templates_file, 'w') as f:
        json.dump(chart_templates, f, indent=2)
    
    logger.info(f"Chart templates saved to {templates_file}")
    
    # Generate individual SQL files for each template
    sql_dir = DASHBOARD_DIR / "sql"
    os.makedirs(sql_dir, exist_ok=True)
    
    for chart_id, chart in chart_templates.items():
        sql_file = sql_dir / f"{chart_id}.sql"
        with open(sql_file, 'w') as f:
            f.write(f"-- {chart['name']}\n")
            f.write(f"-- {chart['description']}\n")
            f.write(f"-- Visualization Type: {chart['visualization_type']}\n\n")
            f.write(chart['query'].strip())
        
        logger.info(f"SQL template for '{chart['name']}' saved to {sql_file}")
    
    return chart_templates

def create_dashboard_json_template():
    """
    Create a JSON template for a Superset dashboard.
    This can be used as a reference for the dashboard structure.
    """
    logger.info("Creating dashboard JSON template")
    
    # Create a template for the Complaint Overview Dashboard
    overview_dashboard = {
        "dashboard_title": "CFPB Complaint Overview",
        "description": "Overview of CFPB complaint data with key metrics and trends",
        "css": "",
        "json_metadata": json.dumps({
            "chart_configuration": {
                "complaints_by_product": {
                    "width": 6,
                    "height": 50,
                    "position_x": 0,
                    "position_y": 0
                },
                "complaints_over_time": {
                    "width": 12,
                    "height": 50,
                    "position_x": 0,
                    "position_y": 50
                },
                "complaints_by_state": {
                    "width": 6,
                    "height": 50,
                    "position_x": 6,
                    "position_y": 0
                }
            },
            "refresh_frequency": 0,
            "timed_refresh_immune_slices": [],
            "expanded_slices": {},
            "default_filters": "{}",
            "color_scheme": "supersetColors"
        }),
        "position_json": json.dumps({
            "complaints_by_product": {
                "children": [],
                "id": "complaints_by_product",
                "meta": {
                    "chartId": 1,  # Placeholder, to be replaced in Superset
                    "height": 50,
                    "sliceName": "Complaints by Product",
                    "width": 6
                },
                "type": "CHART",
                "parents": ["ROOT_ID", "GRID_ID", "ROW-1"]
            },
            "complaints_over_time": {
                "children": [],
                "id": "complaints_over_time",
                "meta": {
                    "chartId": 2,  # Placeholder
                    "height": 50,
                    "sliceName": "Complaints Over Time",
                    "width": 12
                },
                "type": "CHART",
                "parents": ["ROOT_ID", "GRID_ID", "ROW-2"]
            },
            "complaints_by_state": {
                "children": [],
                "id": "complaints_by_state",
                "meta": {
                    "chartId": 3,  # Placeholder
                    "height": 50,
                    "sliceName": "Complaints by State",
                    "width": 6
                },
                "type": "CHART",
                "parents": ["ROOT_ID", "GRID_ID", "ROW-1"]
            }
        })
    }
    
    # Create a template for the Real-time Monitoring Dashboard
    monitoring_dashboard = {
        "dashboard_title": "CFPB Real-time Monitoring",
        "description": "Real-time monitoring of complaint processing and predictions",
        "css": "",
        "json_metadata": json.dumps({
            "chart_configuration": {
                "complaint_processing_kpi": {
                    "width": 4,
                    "height": 20,
                    "position_x": 0,
                    "position_y": 0
                },
                "high_urgency_complaints": {
                    "width": 12,
                    "height": 60,
                    "position_x": 0,
                    "position_y": 40
                },
                "real_time_prediction_distribution": {
                    "width": 4,
                    "height": 40,
                    "position_x": 8,
                    "position_y": 0
                },
                "escalation_rate_by_product": {
                    "width": 4,
                    "height": 40,
                    "position_x": 4,
                    "position_y": 0
                }
            },
            "refresh_frequency": 10,  # 10 seconds
            "timed_refresh_immune_slices": [],
            "expanded_slices": {},
            "default_filters": "{}",
            "color_scheme": "supersetColors"
        }),
        "position_json": json.dumps({
            "complaint_processing_kpi": {
                "children": [],
                "id": "complaint_processing_kpi",
                "meta": {
                    "chartId": 4,  # Placeholder
                    "height": 20,
                    "sliceName": "Complaint Processing KPI",
                    "width": 4
                },
                "type": "CHART",
                "parents": ["ROOT_ID", "GRID_ID", "ROW-1"]
            },
            "high_urgency_complaints": {
                "children": [],
                "id": "high_urgency_complaints",
                "meta": {
                    "chartId": 5,  # Placeholder
                    "height": 60,
                    "sliceName": "High Urgency Complaints",
                    "width": 12
                },
                "type": "CHART",
                "parents": ["ROOT_ID", "GRID_ID", "ROW-3"]
            },
            "real_time_prediction_distribution": {
                "children": [],
                "id": "real_time_prediction_distribution",
                "meta": {
                    "chartId": 6,  # Placeholder
                    "height": 40,
                    "sliceName": "Real-time Prediction Distribution",
                    "width": 4
                },
                "type": "CHART",
                "parents": ["ROOT_ID", "GRID_ID", "ROW-2"]
            },
            "escalation_rate_by_product": {
                "children": [],
                "id": "escalation_rate_by_product",
                "meta": {
                    "chartId": 7,  # Placeholder
                    "height": 40,
                    "sliceName": "Escalation Rate by Product",
                    "width": 4
                },
                "type": "CHART",
                "parents": ["ROOT_ID", "GRID_ID", "ROW-2"]
            }
        })
    }
    
    # Save dashboard templates
    dashboards = {
        "overview_dashboard": overview_dashboard,
        "monitoring_dashboard": monitoring_dashboard
    }
    
    dashboard_file = DASHBOARD_DIR / "dashboard_templates.json"
    with open(dashboard_file, 'w') as f:
        json.dump(dashboards, f, indent=2)
    
    logger.info(f"Dashboard templates saved to {dashboard_file}")
    
    return dashboards

def create_dashboard_documentation():
    """
    Create documentation for the dashboards with screenshots and descriptions.
    """
    logger.info("Creating dashboard documentation")
    
    documentation = {
        "title": "CFPB Complaint Dashboard Documentation",
        "overview": "This documentation provides details on the dashboards created for monitoring and analyzing CFPB complaint data.",
        "dashboards": [
            {
                "name": "CFPB Complaint Overview",
                "description": "This dashboard provides a high-level overview of historical complaint data, including trends over time, distribution by product, and geographic patterns.",
                "charts": [
                    {
                        "name": "Complaints by Product",
                        "description": "Pie chart showing the distribution of complaints by product category.",
                        "use_case": "Identify which products generate the most complaints to focus improvement efforts."
                    },
                    {
                        "name": "Complaints Over Time",
                        "description": "Line chart showing trends of complaints received over time, broken down by product.",
                        "use_case": "Identify seasonal patterns or increasing/decreasing trends in complaint volumes."
                    },
                    {
                        "name": "Complaints by State",
                        "description": "Map visualization showing the distribution of complaints across US states.",
                        "use_case": "Identify geographic hot spots that may require region-specific attention."
                    }
                ],
                "refresh_rate": "Daily",
                "target_audience": "Management, Product teams, Compliance officers"
            },
            {
                "name": "CFPB Real-time Monitoring",
                "description": "This dashboard provides real-time monitoring of complaint processing, focusing on predictions, urgency levels, and escalation patterns.",
                "charts": [
                    {
                        "name": "Complaint Processing KPI",
                        "description": "Big number visualization showing key metrics like total processed complaints, escalation rate, and average prediction score.",
                        "use_case": "At-a-glance view of current system performance."
                    },
                    {
                        "name": "High Urgency Complaints",
                        "description": "Table showing the most recent high-urgency complaints with prediction scores and recommended actions.",
                        "use_case": "Identify and address the most critical complaints quickly."
                    },
                    {
                        "name": "Real-time Prediction Distribution",
                        "description": "Pie chart showing the distribution of prediction scores across low, medium, and high ranges.",
                        "use_case": "Monitor the overall distribution of complaint severity in real-time."
                    },
                    {
                        "name": "Escalation Rate by Product",
                        "description": "Bar chart showing the escalation rate by product category.",
                        "use_case": "Identify products with unusually high escalation rates that may require attention."
                    }
                ],
                "refresh_rate": "10 seconds",
                "target_audience": "Operations team, Customer service managers, Executive dashboard"
            }
        ],
        "usage_guidelines": [
            "The Overview dashboard is designed for strategic analysis and is updated daily.",
            "The Real-time Monitoring dashboard is for operational use and refreshes every 10 seconds.",
            "Filters can be applied to both dashboards to focus on specific time periods, products, or states.",
            "Dashboard export functionality is available for sharing or offline analysis."
        ]
    }
    
    # Save documentation
    doc_file = DASHBOARD_DIR / "dashboard_documentation.json"
    with open(doc_file, 'w') as f:
        json.dump(documentation, f, indent=2)
    
    # Create a markdown version for better readability
    markdown_file = DASHBOARD_DIR / "dashboard_documentation.md"
    with open(markdown_file, 'w') as f:
        f.write(f"# {documentation['title']}\n\n")
        f.write(f"{documentation['overview']}\n\n")
        
        for dashboard in documentation['dashboards']:
            f.write(f"## {dashboard['name']}\n\n")
            f.write(f"{dashboard['description']}\n\n")
            f.write(f"**Refresh Rate:** {dashboard['refresh_rate']}\n\n")
            f.write(f"**Target Audience:** {dashboard['target_audience']}\n\n")
            
            f.write("### Charts\n\n")
            for chart in dashboard['charts']:
                f.write(f"#### {chart['name']}\n\n")
                f.write(f"{chart['description']}\n\n")
                f.write(f"**Use Case:** {chart['use_case']}\n\n")
            
        f.write("## Usage Guidelines\n\n")
        for guideline in documentation['usage_guidelines']:
            f.write(f"- {guideline}\n")
    
    logger.info(f"Dashboard documentation saved to {doc_file} and {markdown_file}")
    
    return documentation

def generate_metrics_documentation():
    """
    Generate documentation for metrics used in dashboards.
    """
    logger.info("Generating metrics documentation")
    
    metrics = [
        {
            "name": "Complaint Count",
            "definition": "The total number of complaints received or processed",
            "calculation": "COUNT(*)",
            "interpretation": "Higher values indicate more customer issues"
        },
        {
            "name": "Escalation Rate",
            "definition": "The percentage of complaints that require escalation",
            "calculation": "SUM(CASE WHEN escalate = 'Yes' THEN 1 ELSE 0 END) / COUNT(*) * 100",
            "interpretation": "Higher values indicate more severe issues requiring attention"
        },
        {
            "name": "Average Prediction Score",
            "definition": "The average likelihood of successful resolution",
            "calculation": "AVG(prediction)",
            "interpretation": "Higher values indicate better expected outcomes"
        },
        {
            "name": "High Urgency Complaint Count",
            "definition": "Number of complaints with high urgency level",
            "calculation": "SUM(CASE WHEN urgency_level = 'High' THEN 1 ELSE 0 END)",
            "interpretation": "Higher values indicate potentially critical customer issues"
        },
        {
            "name": "Response Time",
            "definition": "Average time between complaint receipt and company response",
            "calculation": "AVG(DATEDIFF('day', \"Date received\", \"Date sent to company\"))",
            "interpretation": "Lower values indicate faster response times"
        }
    ]
    
    # Save metrics documentation
    metrics_file = DASHBOARD_DIR / "metrics_documentation.json"
    with open(metrics_file, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    # Create a markdown version
    markdown_file = DASHBOARD_DIR / "metrics_documentation.md"
    with open(markdown_file, 'w') as f:
        f.write("# CFPB Complaint Dashboard Metrics\n\n")
        
        for metric in metrics:
            f.write(f"## {metric['name']}\n\n")
            f.write(f"**Definition:** {metric['definition']}\n\n")
            f.write(f"**Calculation:**\n```sql\n{metric['calculation']}\n```\n\n")
            f.write(f"**Interpretation:** {metric['interpretation']}\n\n")
            f.write("---\n\n")
    
    logger.info(f"Metrics documentation saved to {metrics_file} and {markdown_file}")
    
    return metrics

def create_all_dashboard_artifacts():
    """
    Create all dashboard artifacts: templates, documentation, and metrics.
    """
    logger.info("Creating all dashboard artifacts")
    
    generate_chart_templates()
    create_dashboard_json_template()
    create_dashboard_documentation()
    generate_metrics_documentation()
    
    logger.info("""
    ===== DASHBOARD DEVELOPMENT ARTIFACTS CREATED =====
    
    The following artifacts have been created to help with dashboard development:
    
    1. Chart Templates:
       - SQL queries for common charts
       - Individual SQL files for each chart
       
    2. Dashboard Templates:
       - JSON structures for dashboard layout
       - Configuration for chart positioning
       
    3. Documentation:
       - Dashboard descriptions and use cases
       - Chart explanations
       - Usage guidelines
       
    4. Metrics Documentation:
       - Definitions of key metrics
       - Calculation formulas
       - Interpretation guidelines
       
    These artifacts are located in the 'dashboards' directory.
    Use them as references when creating dashboards in Superset.
    """)
    
    return True

def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Dashboard Development Tools')
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Chart templates command
    charts_parser = subparsers.add_parser('charts', help='Generate chart templates')
    
    # Dashboard templates command
    dashboards_parser = subparsers.add_parser('dashboards', help='Create dashboard JSON templates')
    
    # Documentation command
    docs_parser = subparsers.add_parser('docs', help='Create dashboard documentation')
    
    # Metrics command
    metrics_parser = subparsers.add_parser('metrics', help='Generate metrics documentation')
    
    # All command
    all_parser = subparsers.add_parser('all', help='Create all dashboard artifacts')
    
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()
    
    # Execute the requested command
    if args.command == 'charts':
        generate_chart_templates()
    
    elif args.command == 'dashboards':
        create_dashboard_json_template()
    
    elif args.command == 'docs':
        create_dashboard_documentation()
    
    elif args.command == 'metrics':
        generate_metrics_documentation()
    
    elif args.command == 'all':
        create_all_dashboard_artifacts()
    
    else:
        # If no command or invalid command
        logger.info("""
        Dashboard Development Tools
        
        Available commands:
        - charts: Generate chart templates
        - dashboards: Create dashboard JSON templates
        - docs: Create dashboard documentation
        - metrics: Generate metrics documentation
        - all: Create all dashboard artifacts
        
        Example:
          python src/mission_7_dashboard_development.py all
        """) 