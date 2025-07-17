import logging
import sys
from pathlib import Path
from datetime import datetime

def setup_logger(stage: str):
    # Create logs directory if it does not exist
    log_dir = Path(__file__).resolve().parent.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # Generate timestamp for log filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = log_dir / f"modeling_{timestamp}.log"

    # Define log format including the 'stage' contextual information
    log_format = '%(asctime)s - %(levelname)s - %(stage)s - %(message)s'

    # Configure logging with file and console handlers
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Get the root logger
    logger = logging.getLogger()

    # Return a LoggerAdapter to inject 'stage' into log records
    return logging.LoggerAdapter(logger, {'stage': stage})
