import logging
import logging.handlers
from pathlib import Path
import re
from datetime import datetime
from typing import Dict

def setup_logger(
    name: str,
    log_file: str = None,
    level: int = logging.INFO,
    log_dir: str = "logs",
    max_bytes: int = 1024*1024,  # 1MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Set up a logger that writes to both file and console.
    
    Args:
        name: Logger name (typically __name__)
        log_file: Name of the log file (default: derived from logger name)
        level: Logging level (default: INFO)
        log_dir: Directory to store log files (default: 'logs')
        max_bytes: Max size of each log file before rotation
        backup_count: Number of backup files to keep
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Prevent adding handlers multiple times
    if logger.handlers:
        return logger
        
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Console handler 
    # NOTE: this means we have duplicate logs messages
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    if log_file is None:
        log_file = f"{name.split('.')[-1]}.log"
    
    # Create log directory if it doesn't exist
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)
    
    # write log messages to a file until 1MB, then rotate
    file_handler = logging.handlers.RotatingFileHandler(
        log_path / log_file,
        maxBytes=max_bytes,
        backupCount=backup_count
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger 

def read_rotated_logs(
    logs_dir: Path,
    match_id: str,
    pattern: str,
    backup_count: int = 5
) -> Dict[int, Dict]:
    """
    Read and parse logs from all rotated log files for a given match_id.
    
    Args:
        logs_dir: Directory containing log files
        match_id: ID of the match to read logs for
        pattern: Regex pattern to extract data from log lines
        backup_count: Number of backup files to check (default: 5)
    
    Returns:
        Dictionary mapping event_ids to their first occurrence data
    """
    events_seen = {}  # Dict to track first occurrence of each event
    
    # Get all log files for this match (base + rotated)
    base_log = logs_dir / f"{match_id}.log"
    log_files = [base_log]
    
    # Add rotated files if they exist
    for i in range(1, backup_count + 1):
        rotated = logs_dir / f"{match_id}.log.{i}"
        if rotated.exists():
            log_files.append(rotated)
    
    # Process all log files
    for log_file in log_files:
        if not log_file.exists():
            continue
            
        with open(log_file, 'r') as f:
            for line in f:
                match = re.search(pattern, line)
                if match:
                    timestamp_str, event_id = match.groups()
                    event_id = int(event_id)
                    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
                    
                    # Only keep the first occurrence of each event_id
                    if event_id not in events_seen:
                        events_seen[event_id] = {
                            'timestamp': timestamp,
                            'event_id': event_id
                        }
    
    return events_seen 