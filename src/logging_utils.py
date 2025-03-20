"""
Logging utilities for the CFPB Complaint Processing System
"""
import logging
import os
from src import config

def setup_logging(name=__name__, level=None, log_file=None):
    """
    Configure logging for the application
    
    Args:
        name (str): Logger name, typically __name__ from the calling module
        level (str): Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file (str): Path to log file
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Use defaults from config if not provided
    if level is None:
        level = config.LOG_LEVEL
    
    if log_file is None:
        log_file = config.LOG_FILE
    
    # Create logger
    logger = logging.getLogger(name)
    
    # Convert string level to logging level
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {level}')
    
    logger.setLevel(numeric_level)
    
    # Create formatters and handlers
    formatter = logging.Formatter(config.LOG_FORMAT)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if log file specified)
    if log_file:
        # Ensure log directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    logger.info(f"Logging configured with level: {level}")
    return logger 