import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
from datetime import datetime
import uuid

class Logger:
    def __init__(self, name: str = "my_app.log", run_id: str = None, level: str = "INFO"):
        """
        Initialize logger with file creation guarantee
        
        Args:
            name: Logger name (usually __name__)
            log_file: Path to log file
            level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        # Generate run ID if not provided
        self.run_id = run_id or str(uuid.uuid4())[:8]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Create log filename with run_id and timestamp
        self.log_file = os.path.join(
            "logs",
            f"{self.run_id}_{timestamp}_{name}.log"
        )

        # Create formatter with run_id
        formatter = logging.Formatter(
            f'%(asctime)s - {self.run_id} - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File handler with guaranteed file creation
        try:
            # Create directory if needed
            log_path = Path(self.log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create empty file if it doesn't exist
            if not log_path.exists():
                log_path.touch()
            
            # Set up rotating file handler
            file_handler = RotatingFileHandler(
                filename=self.log_file,
                maxBytes=5*1024*1024,  # 5MB
                backupCount=3,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            
            self.logger.debug(f"Log file setup complete at {self.log_file}")
            self.logger.info(f"Logger initialized with log file: {self.log_file}")
        except Exception as e:
            self.logger.error(f"Failed to setup file logging: {str(e)}", exc_info=True)
            raise
    
    def get_logger(self):
        """Get the configured logger instance"""
        return self.logger

# Example usage
"""
if __name__ == "__main__":
    logger = Logger("Energy_connector",12345678).get_logger()
    logger.info("Logging system initialized successfully")
    logger.debug("This is a debug message")
    logger.error("This is an error message")

"""