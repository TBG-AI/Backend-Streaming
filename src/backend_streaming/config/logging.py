import logging
import os
from datetime import datetime
import colorlog

# Create logs directory if it doesn't exist
LOGS_DIR = "logs"
os.makedirs(LOGS_DIR, exist_ok=True)

# Create a new log file for each run
LOG_FILENAME = f"{LOGS_DIR}/stream_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

def setup_logging():
    """Configure logging to both file and console with colors"""
    # NOTE: keeping different formatter since .log can't handle colors
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)-8s - %(message)s'
    )

    # Console formatter (with colors)
    console_formatter = colorlog.ColoredFormatter(
        "%(green)s%(asctime)s%(reset)s - %(purple)s%(name)s - %(log_color)s%(levelname)s%(reset)s - %(message)s",
        log_colors={
            'DEBUG':    'cyan',
            'INFO':     'blue',
            'WARNING':  'yellow',
            'ERROR':    'red',
            'CRITICAL': 'red,bg_white',
        },
        secondary_log_colors={},
        style='%'
    )

    # File handler
    file_handler = logging.FileHandler(LOG_FILENAME)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)

    # Console handler
    console_handler = colorlog.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Remove any existing handlers
    root_logger.handlers = []
    
    # Add handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler) 