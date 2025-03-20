"""
CFPB Complaint Processing System - Mission 1

This script is the main entry point for Mission 1 - Project Setup and Configuration.
It will:
1. Set up the project directory structure
2. Configure logging
3. Test Spark session
4. Display configurations

Run this script with: python mission_1_setup.py
"""
import os
import sys

# Add the current directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the setup module
from src.setup import main

if __name__ == "__main__":
    # Run the setup
    main() 