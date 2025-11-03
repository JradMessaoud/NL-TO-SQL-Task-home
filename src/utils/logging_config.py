"""
Configure logging for the application.
"""
import logging
import os
from datetime import datetime

def setup_logging(log_dir: str = "logs") -> None:
    """
    Set up logging configuration for the application.
    
    Args:
        log_dir: Directory to store log files
    """
    # Create logs directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"query_system_{timestamp}.log")
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    # Create logger
    logger = logging.getLogger("QuerySystem")
    logger.setLevel(logging.INFO)
    
    # Log startup message
    logger.info("Logging system initialized")