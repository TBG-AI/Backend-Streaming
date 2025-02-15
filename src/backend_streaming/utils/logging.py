# import logging
# import logging.handlers
# from pathlib import Path

# def setup_logger(
#     name: str,
#     log_file: str = None,
#     level: int = logging.INFO,
#     log_dir: str = "logs",
#     max_bytes: int = 1024*1024,  # 1MB
#     backup_count: int = 5
# ) -> logging.Logger:
#     """
#     Set up a logger that writes to both file and console.
    
#     Args:
#         name: Logger name (typically __name__)
#         log_file: Name of the log file (default: derived from logger name)
#         level: Logging level (default: INFO)
#         log_dir: Directory to store log files (default: 'logs')
#         max_bytes: Max size of each log file before rotation
#         backup_count: Number of backup files to keep
    
#     Returns:
#         Configured logger instance
#     """
#     logger = logging.getLogger(name)
#     logger.setLevel(level)
    
#     # Prevent adding handlers multiple times
#     if logger.handlers:
#         return logger
        
#     formatter = logging.Formatter(
#         '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     )
    
#     # Console handler
#     console_handler = logging.StreamHandler()
#     console_handler.setFormatter(formatter)
#     logger.addHandler(console_handler)
    
#     # File handler
#     if log_file is None:
#         log_file = f"{name.split('.')[-1]}.log"
    
#     # Create log directory if it doesn't exist
#     log_path = Path(log_dir)
#     log_path.mkdir(exist_ok=True)
    
#     file_handler = logging.handlers.RotatingFileHandler(
#         log_path / log_file,
#         maxBytes=max_bytes,
#         backupCount=backup_count
#     )
#     file_handler.setFormatter(formatter)
#     logger.addHandler(file_handler)
    
#     return logger 

import logging
from datetime import datetime
import sys

def setup_logger(game_id: str):
    """Setup logger for this game process"""
    logger = logging.getLogger(f"game_{game_id}")
    logger.setLevel(logging.INFO)
    
    # Create formatters and handlers
    formatter = logging.Formatter(
        '%(asctime)s - Game %(game_id)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # File Handler - log to a file specific to this game
    file_handler = logging.FileHandler(f"game_{game_id}.log")
    file_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    # Add game_id to all log records
    logger = logging.LoggerAdapter(logger, {"game_id": game_id})
    
    return logger