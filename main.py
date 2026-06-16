import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))

from src.database import init_relational_database
from src.vector_db import init_vector_database
from src.telegram_bot import main as start_bot
from src.utils.logger import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    logger.info("Initializing FinAgent Platform...")
    init_relational_database()
    init_vector_database()
    start_bot()