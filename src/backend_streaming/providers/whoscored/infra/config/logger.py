import logging
from pathlib import Path
from backend_streaming.providers.whoscored.infra.config.config import paths

def setup_game_logger(game_id: str):
    """Setup logger for a specific game"""    
    logger = logging.getLogger('game')
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - Game - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File Handler with mode='w' to overwrite
    log_file = paths.game_logs_dir / f"{game_id}.log"
    file_handler = logging.FileHandler(str(log_file))
    file_handler.setFormatter(formatter)
    
    # Add only the file handler
    logger.addHandler(file_handler)
    logger.info(f"Game logging initialized. Writing to: {log_file}")
    return logger
    
    