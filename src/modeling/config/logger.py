import logging
import sys
from pathlib import Path
from datetime import datetime
from logging.handlers import RotatingFileHandler

def setup_logger(stage: str):
    # Create logs directory if it does not exist
    log_dir = Path(__file__).resolve().parent.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # Use a single daily log file instead of per-execution files
    date_str = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"modeling_{date_str}.log"

    # Define log format including the 'stage' contextual information
    log_format = '%(asctime)s - %(levelname)s - %(stage)s - %(message)s'

    # Create a rotating file handler (max 10MB, keep 5 backups)
    file_handler = RotatingFileHandler(
        log_file, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format))

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format))

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Clear any existing handlers to avoid duplicates
    root_logger.handlers.clear()
    
    # Add handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Return a LoggerAdapter to inject 'stage' into log records
    return logging.LoggerAdapter(root_logger, {'stage': stage})
