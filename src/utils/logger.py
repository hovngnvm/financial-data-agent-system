import sys
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LOG_DIR = PROJECT_ROOT / "logs"
MAX_LOG_BYTES: int = 10 * 1024 * 1024
LOG_BACKUP_COUNT: int = 5


def get_logger(name: str = __name__, level: int = logging.INFO, log_dir: Path | None = None) -> logging.Logger:
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

        target_dir = log_dir or DEFAULT_LOG_DIR
        target_dir.mkdir(parents=True, exist_ok=True)
        log_file = target_dir / "finagent.log"
        file_handler = RotatingFileHandler(
            str(log_file), maxBytes=MAX_LOG_BYTES, backupCount=LOG_BACKUP_COUNT, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
    logger.propagate = False
    return logger
