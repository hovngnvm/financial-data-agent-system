import sys
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler


def get_logger(name: str, level: int = logging.INFO, log_dir: Path | None = None) -> logging.Logger:
    """Returns a configured Python logger instance with standard stream and rotating file handlers."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if not logger.handlers:
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s',
            datefmt='%Y-%m-%dT%H:%M:%S%z'
        )
        
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        if log_dir:
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / "finagent.log"
            file_handler = RotatingFileHandler(str(log_file), maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
    logger.propagate = False
    return logger
