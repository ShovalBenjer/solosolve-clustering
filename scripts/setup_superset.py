"""
Script to set up Superset database connection and dashboards for CFPB complaints.

This is meant to be run after Superset is installed and the MySQL database is set up.
"""
import os
import sys
import json
import requests
from pathlib import Path

# Add parent directory to Python path to import config module
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)

# Superset API endpoints
SUPERSET_HOST = os.getenv('SUPERSET_HOST', 'http://localhost:8088')
LOGIN_URL = f"{SUPERSET_HOST}/api/v1/security/login"
DATABASES_URL = f"{SUPERSET_HOST}/api/v1/database/"
DATASET_URL = f"{SUPERSET_HOST}/api/v1/dataset/"
CHART_URL = f"{SUPERSET_HOST}/api/v1/chart/"
DASHBOARD_URL = f"{SUPERSET_HOST}/api/v1/dashboard/"

# Admin credentials for Superset (these should be changed in production)
ADMIN_USERNAME = os.getenv('SUPERSET_ADMIN_USER', 'admin')
ADMIN_PASSWORD = os.getenv('SUPERSET_ADMIN_PASSWORD', 'admin')

def get_access_token():
    """Get access token from Superset API."""
    login_data = {
        "username": ADMIN_USERNAME,
        "password": ADMIN_PASSWORD,
        "provider": "db"
    }
    
    try:
        response = requests.post(LOGIN_URL, json=login_data)
        response.raise_for_status()
        return response.json()['access_token']
    except requests.exceptions.RequestException as e:
        print(f"Failed to get access token: {e}")
        sys.exit(1)

def create_database_connection(access_token):
    """Create database connection in Superset."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # MySQL connection string
    sqlalchemy_uri = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    database_data = {
        "database_name": "CFPB Complaints",
        "sqlalchemy_uri": sqlalchemy_uri,
        "expose_in_sqllab": True,
        "allow_run_async": True,
        "allow_csv_upload": False,
        "allow_ctas": True,
        "allow_cvas": True,
        "extra": json.dumps({
            "metadata_cache_timeout": 86400,
            "engine_params": {
                "connect_args": {
                    "charset": "utf8mb4"
                }
            }
        })
    }
    
    try:
        # Check if database already exists
        response = requests.get(
            DATABASES_URL,
            headers=headers,
            params={"q": json.dumps({"filters": [{"col": "database_name", "opr": "eq", "value": "CFPB Complaints"}]})}
        )
        response.raise_for_status()
        
        if response.json()['count'] > 0:
            print("Database connection already exists")
            return response.json()['result'][0]['id']
        
        # Create database connection
        response = requests.post(DATABASES_URL, headers=headers, json=database_data)
        response.raise_for_status()
        db_id = response.json()['id']
        print(f"Created database connection with ID: {db_id}")
        return db_id
    
    except requests.exceptions.RequestException as e:
        print(f"Failed to create database connection: {e}")
        if hasattr(e, 'response') and e.response:
            print(f"Response: {e.response.text}")
        sys.exit(1)

def create_dataset(access_token, db_id, dataset_name, sql_query):
    """Create a dataset in Superset."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    dataset_data = {
        "database": db_id,
        "schema": DB_NAME,
        "table_name": dataset_name,
        "sql": sql_query
    }
    
    try:
        # Check if dataset already exists
        response = requests.get(
            DATASET_URL,
            headers=headers,
            params={"q": json.dumps({"filters": [{"col": "table_name", "opr": "eq", "value": dataset_name}]})}
        )
        response.raise_for_status()
        
        if response.json()['count'] > 0:
            print(f"Dataset '{dataset_name}' already exists")
            return response.json()['result'][0]['id']
        
        # Create dataset
        response = requests.post(DATASET_URL, headers=headers, json=dataset_data)
        response.raise_for_status()
        dataset_id = response.json()['id']
        print(f"Created dataset '{dataset_name}' with ID: {dataset_id}")
        return dataset_id
    
    except requests.exceptions.RequestException as e:
        print(f"Failed to create dataset: {e}")
        if hasattr(e, 'response') and e.response:
            print(f"Response: {e.response.text}")
        return None

def create_chart(access_token, dataset_id, chart_name, viz_type, query_context):
    """Create a chart in Superset."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    chart_data = {
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "slice_name": chart_name,
        "viz_type": viz_type,
        "params": json.dumps({
            "viz_type": viz_type,
            "datasource": f"{dataset_id}__table",
            **query_context
        })
    }
    
    try:
        # Check if chart already exists
        response = requests.get(
            CHART_URL,
            headers=headers,
            params={"q": json.dumps({"filters": [{"col": "slice_name", "opr": "eq", "value": chart_name}]})}
        )
        response.raise_for_status()
        
        if response.json()['count'] > 0:
            print(f"Chart '{chart_name}' already exists")
            return response.json()['result'][0]['id']
        
        # Create chart
        response = requests.post(CHART_URL, headers=headers, json=chart_data)
        response.raise_for_status()
        chart_id = response.json()['id']
        print(f"Created chart '{chart_name}' with ID: {chart_id}")
        return chart_id
    
    except requests.exceptions.RequestException as e:
        print(f"Failed to create chart: {e}")
        if hasattr(e, 'response') and e.response:
            print(f"Response: {e.response.text}")
        return None

def create_dashboard(access_token, dashboard_name, chart_ids):
    """Create a dashboard in Superset."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    dashboard_data = {
        "dashboard_title": dashboard_name,
        "slug": dashboard_name.lower().replace(" ", "_"),
        "position_json": json.dumps({
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"children": []},
            "GRID_ID": {"children": []}
        }),
        "json_metadata": json.dumps({
            "refresh_frequency": 10,  # Auto-refresh every 10 seconds
            "timed_refresh_immune_slices": [],
            "expanded_slices": {},
            "default_filters": "{}",
            "color_scheme": "supersetColors"
        })
    }
    
    try:
        # Check if dashboard already exists
        response = requests.get(
            DASHBOARD_URL,
            headers=headers,
            params={"q": json.dumps({"filters": [{"col": "dashboard_title", "opr": "eq", "value": dashboard_name}]})}
        )
        response.raise_for_status()
        
        if response.json()['count'] > 0:
            dashboard_id = response.json()['result'][0]['id']
            print(f"Dashboard '{dashboard_name}' already exists with ID: {dashboard_id}")
        else:
            # Create dashboard
            response = requests.post(DASHBOARD_URL, headers=headers, json=dashboard_data)
            response.raise_for_status()
            dashboard_id = response.json()['id']
            print(f"Created dashboard '{dashboard_name}' with ID: {dashboard_id}")
        
        # Add charts to dashboard
        for chart_id in chart_ids:
            if chart_id:
                add_chart_to_dashboard(access_token, dashboard_id, chart_id)
        
        return dashboard_id
    
    except requests.exceptions.RequestException as e:
        print(f"Failed to create dashboard: {e}")
        if hasattr(e, 'response') and e.response:
            print(f"Response: {e.response.text}")
        return None

def add_chart_to_dashboard(access_token, dashboard_id, chart_id):
    """Add a chart to a dashboard."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    chart_data = {
        "dashboardId": dashboard_id,
        "size_x": 6,
        "size_y": 5,
        "slice_id": chart_id
    }
    
    try:
        response = requests.post(f"{DASHBOARD_URL}/{dashboard_id}/charts", headers=headers, json=chart_data)
        response.raise_for_status()
        print(f"Added chart {chart_id} to dashboard {dashboard_id}")
    
    except requests.exceptions.RequestException as e:
        print(f"Failed to add chart to dashboard: {e}")
        if hasattr(e, 'response') and e.response:
            print(f"Response: {e.response.text}")

def main():
    """Main function to set up Superset."""
    print("Setting up Superset for CFPB Complaints Dashboard")
    
    # Get access token
    access_token = get_access_token()
    
    # Create database connection
    db_id = create_database_connection(access_token)
    
    # Create datasets
    cluster_dataset_id = create_dataset(
        access_token,
        db_id,
        "recent_complaints",
        "SELECT * FROM recent_complaints"
    )
    
    complaints_dataset_id = create_dataset(
        access_token,
        db_id,
        "processed_complaints",
        "SELECT * FROM processed_complaints"
    )
    
    # Create charts for the dashboard
    charts = []
    
    # Cluster distribution pie chart
    cluster_pie_id = create_chart(
        access_token,
        cluster_dataset_id,
        "Complaint Cluster Distribution",
        "pie",
        {
            "metrics": [{"label": "Complaint Count", "expressionType": "SQL", "sqlExpression": "SUM(complaint_count)"}],
            "groupby": ["resolution"],
            "adhoc_filters": [],
            "row_limit": 10,
            "sort_by_metric": True,
            "color_scheme": "supersetColors"
        }
    )
    charts.append(cluster_pie_id)
    
    # Timely responses by cluster
    timely_bar_id = create_chart(
        access_token,
        cluster_dataset_id,
        "Timely Responses by Cluster",
        "bar",
        {
            "metrics": [
                {"label": "Timely Responses", "expressionType": "SQL", "sqlExpression": "SUM(timely_responses)"},
                {"label": "Total Complaints", "expressionType": "SQL", "sqlExpression": "SUM(complaint_count)"}
            ],
            "groupby": ["resolution"],
            "adhoc_filters": [],
            "row_limit": 10,
            "color_scheme": "supersetColors"
        }
    )
    charts.append(timely_bar_id)
    
    # Disputed complaints by cluster
    disputed_bar_id = create_chart(
        access_token,
        cluster_dataset_id,
        "Disputed Complaints by Cluster",
        "bar",
        {
            "metrics": [
                {"label": "Disputed Complaints", "expressionType": "SQL", "sqlExpression": "SUM(disputed_complaints)"},
                {"label": "Total Complaints", "expressionType": "SQL", "sqlExpression": "SUM(complaint_count)"}
            ],
            "groupby": ["resolution"],
            "adhoc_filters": [],
            "row_limit": 10,
            "color_scheme": "supersetColors"
        }
    )
    charts.append(disputed_bar_id)
    
    # Create dashboard with charts
    dashboard_id = create_dashboard(access_token, "CFPB Complaints Dashboard", charts)
    
    print(f"Superset setup complete. Dashboard URL: {SUPERSET_HOST}/superset/dashboard/{dashboard_id}/")

if __name__ == "__main__":
    main() 