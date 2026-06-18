from typing import Any
from langfuse.langchain import CallbackHandler
from src.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

_langfuse_handler: Any | None = None
_langfuse_initialized: bool = False

def get_langfuse_handler() -> Any | None:
    """Initializes and returns the Langfuse tracing callback handler if valid credentials exist."""
    global _langfuse_handler, _langfuse_initialized
    if not _langfuse_initialized:
        _langfuse_initialized = True
        pub_key = settings.langfuse_public_key
        sec_key = settings.langfuse_secret_key
        host = settings.langfuse_host
        
        if pub_key and sec_key:
            try:
                _langfuse_handler = CallbackHandler(
                    public_key=pub_key,
                    secret_key=sec_key,
                    host=host
                )
                logger.info(f"Langfuse telemetry tracing enabled connected to {host}")
            except Exception as e:
                logger.warning(f"Failed to initialize Langfuse callback handler: {e}")
                _langfuse_handler = None
    return _langfuse_handler
