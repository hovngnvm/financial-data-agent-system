import sys
from pathlib import Path
import pytest

# Ensure project root is in sys.path for seamless imports
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def anyio_backend():
    return 'asyncio'
