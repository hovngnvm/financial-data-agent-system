import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.database import db_manager
from src.vector_db import vector_db_manager
from src.telegram_bot import main as start_bot
from src.utils.logger import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    logger.info("Initializing FinAgent Platform...")
    db_manager.init_db()
    vector_db_manager.init_db()
    start_bot()