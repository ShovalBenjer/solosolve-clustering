"""
Mission 7: Superset Dashboard Setup

This script helps set up and configure Apache Superset for CFPB complaint data visualization.
It provides functionality for:
1. Installing and configuring Superset
2. Connecting to both historical and real-time data sources
3. Creating datasets for visualization
4. Setting up initial dashboard templates
"""

import os
import sys
import json
import logging
import subprocess
import time
from pathlib import Path
import argparse
import getpass
from typing import Dict, List, Optional

# Add parent directory to path for imports
sys.path.append(str(Path(os.path.dirname(os.path.abspath(__file__))).parent))

# Import project modules
from src import config
from src.logging_utils import setup_logging
from src.schema import complaint_schema, processed_schema

# Set up logging
logger = setup_logging(log_level=config.LOG_LEVEL)

# Superset configuration
SUPERSET_ADMIN_USERNAME = "admin"
SUPERSET_PORT = 8088
SUPERSET_DATABASE_URI = "sqlite:///superset.db"
SUPERSET_ENV_DIR = config.BASE_DIR / "superset_env"
SUPERSET_CONFIG_DIR = config.BASE_DIR / "superset"

def install_superset():
    """
    Install Apache Superset and its dependencies in a virtual environment.
    """
    logger.info("Installing Apache Superset")
    
    try:
        # Create virtual environment
        venv_dir = str(SUPERSET_ENV_DIR)
        logger.info(f"Creating virtual environment at {venv_dir}")
        subprocess.run([sys.executable, "-m", "venv", venv_dir], check=True)
        
        # Determine the Python executable in the virtual environment
        if os.name == 'nt':  # Windows
            python_exec = os.path.join(venv_dir, "Scripts", "python.exe")
            pip_exec = os.path.join(venv_dir, "Scripts", "pip.exe")
        else:  # Unix/Linux/Mac
            python_exec = os.path.join(venv_dir, "bin", "python")
            pip_exec = os.path.join(venv_dir, "bin", "pip")
        
        # Upgrade pip
        logger.info("Upgrading pip")
        subprocess.run([pip_exec, "install", "--upgrade", "pip"], check=True)
        
        # Install Superset
        logger.info("Installing Apache Superset")
        subprocess.run([pip_exec, "install", "apache-superset"], check=True)
        
        # Install additional dependencies
        logger.info("Installing additional dependencies")
        subprocess.run([
            pip_exec, "install", 
            "psycopg2-binary",  # PostgreSQL driver
            "redis",            # Redis for caching
            "flask-appbuilder",
            "sqlalchemy"
        ], check=True)
        
        # Create Superset config directory
        os.makedirs(SUPERSET_CONFIG_DIR, exist_ok=True)
        
        logger.info("Apache Superset successfully installed")
        return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Error during Superset installation: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during Superset installation: {e}")
        return False

def initialize_superset():
    """
    Initialize Superset database and create admin user.
    """
    logger.info("Initializing Superset")
    
    try:
        # Determine the Superset executable in the virtual environment
        if os.name == 'nt':  # Windows
            superset_exec = os.path.join(SUPERSET_ENV_DIR, "Scripts", "superset.exe")
        else:  # Unix/Linux/Mac
            superset_exec = os.path.join(SUPERSET_ENV_DIR, "bin", "superset")
        
        # Initialize the database
        logger.info("Initializing Superset database")
        subprocess.run([superset_exec, "db", "upgrade"], check=True)
        
        # Create admin user
        logger.info("Creating admin user")
        admin_password = getpass.getpass("Enter password for Superset admin: ")
        
        subprocess.run([
            superset_exec, "fab", "create-admin",
            "--username", SUPERSET_ADMIN_USERNAME,
            "--firstname", "Admin",
            "--lastname", "User",
            "--email", "admin@example.com",
            "--password", admin_password
        ], check=True)
        
        # Create default roles and permissions
        logger.info("Creating default roles and permissions")
        subprocess.run([superset_exec, "init"], check=True)
        
        logger.info("Superset successfully initialized")
        return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Error during Superset initialization: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during Superset initialization: {e}")
        return False

def create_superset_config():
    """
    Create a custom Superset configuration file.
    """
    logger.info("Creating Superset configuration")
    
    config_file = SUPERSET_CONFIG_DIR / "superset_config.py"
    
    try:
        with open(config_file, 'w') as f:
            f.write(f"""# Superset specific config
SUPERSET_WEBSERVER_PORT = {SUPERSET_PORT}
SQLALCHEMY_DATABASE_URI = '{SUPERSET_DATABASE_URI}'
SECRET_KEY = 'cfpb-superset-secret-key'  # Replace with a secure key in production

# Flask App Builder configuration
APP_NAME = "CFPB Complaint Dashboard"
APP_ICON = "/static/assets/images/superset-logo-horiz.png"

# Feature flags
FEATURE_FLAGS = {{
    'DASHBOARD_NATIVE_FILTERS': True,
    'DASHBOARD_CROSS_FILTERS': True,
    'DASHBOARD_NATIVE_FILTERS_SET': True,
    'ENABLE_TEMPLATE_PROCESSING': True,
    'GLOBAL_ASYNC_QUERIES': True,
}}

# Cache configuration
CACHE_CONFIG = {{
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300
}}

# Data source caching
DATA_CACHE_CONFIG = {{
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300
}}
""")
        
        logger.info(f"Superset configuration saved to {config_file}")
        
        # Set the environment variable for Superset to use this config
        os.environ["SUPERSET_CONFIG_PATH"] = str(config_file)
        
        return True
    
    except Exception as e:
        logger.error(f"Error creating Superset configuration: {e}")
        return False

def configure_kafka_connection():
    """
    Configure connection to Kafka for real-time visualization.
    This is a conceptual function as actual implementation depends on Superset version.
    """
    logger.info("Configuring Kafka connection for Superset")
    
    logger.info("""
    ===== IMPORTANT MANUAL STEPS =====
    To connect Superset to Kafka, you'll need to:
    
    1. Start Superset and log in with admin credentials
    2. Go to Data -> Databases -> + Database
    3. Select 'SQLAlchemy URI' and use the following:
       - For Kafka: Use a Kafka REST proxy with the following URI format:
         kafka://kafka_broker:9092/
       
       Note: Direct Kafka connection in Superset may require custom configuration.
             As an alternative, consider using Kafka Connect JDBC Sink.
    
    4. Test the connection and save.
    """)
    
    return True

def configure_database_connection():
    """
    Configure connection to a SQL database for historical data.
    """
    logger.info("Configuring Database connection for historical data")
    
    logger.info("""
    ===== MANUAL STEPS FOR DATABASE CONNECTION =====
    
    1. In Superset, go to Data -> Databases -> + Database
    2. Select 'SQLAlchemy URI' and use the appropriate URI for your database:
       - SQLite (for demo): sqlite:///path/to/your/historical_data.db
       - PostgreSQL: postgresql://username:password@host:port/database
    
    3. Test the connection and save with the name 'CFPB Historical Data'
    """)
    
    return True

def define_datasets():
    """
    Define Superset datasets for both historical and real-time data.
    """
    logger.info("Defining Superset datasets")
    
    # Convert PySpark schema to a simpler format for documentation
    dataset_columns = []
    for field in complaint_schema.fields:
        column_type = "STRING"
        if "timestamp" in field.name.lower() or "date" in field.name.lower():
            column_type = "DATETIME"
        elif field.dataType.simpleString() == "integer":
            column_type = "INTEGER"
        elif field.dataType.simpleString() == "double":
            column_type = "NUMERIC"
        
        dataset_columns.append({
            "name": field.name,
            "type": column_type
        })
    
    # Add prediction and decision columns
    prediction_columns = [
        {"name": "prediction", "type": "NUMERIC"},
        {"name": "probability", "type": "NUMERIC"},
        {"name": "escalate", "type": "STRING"},
        {"name": "urgency_level", "type": "STRING"},
        {"name": "priority_routing", "type": "STRING"},
        {"name": "recommended_action", "type": "STRING"}
    ]
    
    dataset_columns.extend(prediction_columns)
    
    # Create documentation for manual implementation
    logger.info("""
    ===== MANUAL STEPS FOR DATASET CREATION =====
    
    1. In Superset, go to Data -> Datasets -> + Dataset
    
    2. Create a Real-time Complaints Dataset:
       - Select the database you created for Kafka
       - Select the complaints-predictions topic as the table
       - Set column types appropriately:
         - Date fields as DATETIME
         - Prediction scores as NUMERIC
         - IDs and text fields as STRING
       - Add calculated columns as needed:
         - Resolution time (difference between date fields)
         - Prediction confidence (based on probability)
    
    3. Create a Historical Complaints Dataset:
       - Select the database you created for historical data
       - Select the historical complaints table
       - Configure similar column types as the real-time dataset
    
    4. Save both datasets and verify they're accessible in the Datasets menu
    """)
    
    # Save column definitions to a file for reference
    with open(config.BASE_DIR / "dataset_columns.json", "w") as f:
        json.dump(dataset_columns, f, indent=2)
    
    logger.info(f"Dataset column definitions saved to {config.BASE_DIR / 'dataset_columns.json'}")
    
    return True

def create_initial_dashboard_templates():
    """
    Create initial dashboard templates for quick start.
    """
    logger.info("Creating initial dashboard templates")
    
    dashboard_templates = {
        "Complaint Overview Dashboard": [
            {
                "type": "time_series",
                "name": "Complaints Over Time",
                "description": "Number of complaints received over time, by product",
                "metrics": ["count(*)"],
                "groupby": ["Product"],
                "time_column": "Date received"
            },
            {
                "type": "pie_chart",
                "name": "Complaints by Product",
                "description": "Distribution of complaints by product category",
                "metrics": ["count(*)"],
                "groupby": ["Product"]
            },
            {
                "type": "map",
                "name": "Complaints by State",
                "description": "Geographic distribution of complaints across US states",
                "metrics": ["count(*)"],
                "groupby": ["State"]
            }
        ],
        "Real-time Monitoring Dashboard": [
            {
                "type": "big_number_total",
                "name": "Total Complaints Processed",
                "description": "Total number of complaints processed in real-time",
                "metrics": ["count(*)"]
            },
            {
                "type": "big_number_total",
                "name": "Escalation Rate",
                "description": "Percentage of complaints escalated",
                "metrics": ["sum(CASE WHEN escalate = 'Yes' THEN 1 ELSE 0 END) / count(*)"]
            },
            {
                "type": "table",
                "name": "Recent High Urgency Complaints",
                "description": "List of recent complaints with high urgency",
                "metrics": ["prediction", "Product", "Issue", "State", "recommended_action"],
                "filters": [{"col": "urgency_level", "op": "==", "val": "High"}]
            },
            {
                "type": "time_series",
                "name": "Prediction Distribution Over Time",
                "description": "Distribution of prediction scores over time",
                "metrics": ["avg(prediction)"],
                "time_column": "processing_timestamp"
            }
        ]
    }
    
    # Save dashboard templates to a file for reference
    with open(config.BASE_DIR / "dashboard_templates.json", "w") as f:
        json.dump(dashboard_templates, f, indent=2)
    
    logger.info(f"Dashboard templates saved to {config.BASE_DIR / 'dashboard_templates.json'}")
    
    logger.info("""
    ===== MANUAL STEPS FOR DASHBOARD CREATION =====
    
    1. In Superset, go to Dashboards -> + Dashboard
    
    2. Use the templates saved in dashboard_templates.json as a reference
       to create the suggested dashboards:
       - Complaint Overview Dashboard
       - Real-time Monitoring Dashboard
    
    3. Add various chart types according to the templates:
       - Time Series: Complaints over time
       - Pie Charts: Complaint distribution by product
       - US Map: Geographic distribution by state
       - Big Number: KPI metrics for monitoring
       - Tables: Detailed complaint information
    
    4. Configure auto-refresh for real-time dashboards:
       - Settings -> Advanced -> Refresh frequency -> 10 seconds
    """)
    
    return True

def start_superset_server():
    """
    Start the Superset server for testing.
    """
    logger.info("Starting Superset server")
    
    try:
        # Determine the Superset executable in the virtual environment
        if os.name == 'nt':  # Windows
            superset_exec = os.path.join(SUPERSET_ENV_DIR, "Scripts", "superset.exe")
        else:  # Unix/Linux/Mac
            superset_exec = os.path.join(SUPERSET_ENV_DIR, "bin", "superset")
        
        logger.info(f"Superset will be available at http://localhost:{SUPERSET_PORT}")
        logger.info("Press Ctrl+C to stop the server")
        
        # Start Superset server
        subprocess.run([
            superset_exec, "run", 
            "-p", str(SUPERSET_PORT), 
            "--with-threads", 
            "--reload"
        ], check=True)
        
        return True
    
    except KeyboardInterrupt:
        logger.info("Superset server stopped")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error starting Superset server: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error starting Superset server: {e}")
        return False

def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Superset Dashboard Setup')
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Install command
    install_parser = subparsers.add_parser('install', help='Install and initialize Superset')
    
    # Configure command
    configure_parser = subparsers.add_parser('configure', help='Configure Superset data sources')
    
    # Datasets command
    datasets_parser = subparsers.add_parser('datasets', help='Define Superset datasets')
    
    # Templates command
    templates_parser = subparsers.add_parser('templates', help='Create dashboard templates')
    
    # Start command
    start_parser = subparsers.add_parser('start', help='Start Superset server')
    
    # All command
    all_parser = subparsers.add_parser('all', help='Complete setup (install, configure, datasets, templates)')
    
    return parser.parse_args()

def run_all_setup():
    """
    Run the complete Superset setup process.
    """
    logger.info("Starting complete Superset setup")
    
    # Install and initialize
    if not install_superset():
        logger.error("Superset installation failed, stopping setup")
        return False
    
    if not initialize_superset():
        logger.error("Superset initialization failed, stopping setup")
        return False
    
    if not create_superset_config():
        logger.error("Superset configuration failed, stopping setup")
        return False
    
    # Configure data sources
    configure_kafka_connection()
    configure_database_connection()
    
    # Define datasets and templates
    define_datasets()
    create_initial_dashboard_templates()
    
    logger.info("""
    ===== SUPERSET SETUP COMPLETE =====
    
    Superset has been installed and configured. To start the server, run:
      python src/mission_7_superset_setup.py start
      
    Then open your browser and navigate to:
      http://localhost:8088
      
    Log in with the admin credentials you provided during setup.
    
    Follow the manual steps outlined during setup to:
    1. Connect to the Kafka and historical data sources
    2. Create datasets for visualization
    3. Build dashboards using the provided templates
    """)
    
    return True

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()
    
    # Execute the requested command
    if args.command == 'install':
        install_superset()
        initialize_superset()
        create_superset_config()
    
    elif args.command == 'configure':
        configure_kafka_connection()
        configure_database_connection()
    
    elif args.command == 'datasets':
        define_datasets()
    
    elif args.command == 'templates':
        create_initial_dashboard_templates()
    
    elif args.command == 'start':
        start_superset_server()
    
    elif args.command == 'all':
        run_all_setup()
    
    else:
        # If no command or invalid command
        logger.info("""
        CFPB Complaint Dashboard Setup
        
        Available commands:
        - install: Install and initialize Superset
        - configure: Configure Superset data sources
        - datasets: Define Superset datasets
        - templates: Create dashboard templates
        - start: Start Superset server
        - all: Complete setup process
        
        Example:
          python src/mission_7_superset_setup.py install
        """) 
