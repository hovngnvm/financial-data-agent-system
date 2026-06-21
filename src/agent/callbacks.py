import functools
from typing import Any
from src.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

@functools.cache
def get_langfuse_handler() -> Any | None:
    """Initializes and returns the Langfuse tracing callback handler if valid credentials exist."""
    pub_key = settings.langfuse_public_key
    sec_key = settings.langfuse_secret_key
    host = settings.langfuse_host
    
    if pub_key and sec_key:
        try:
            from langfuse.langchain import CallbackHandler
            handler = CallbackHandler(
                public_key=pub_key,
                secret_key=sec_key,
                host=host
            )
            logger.info(f"Langfuse telemetry tracing enabled connected to {host}")
            return handler
        except Exception as e:
            logger.warning(f"Failed to initialize Langfuse callback handler: {e}")
    return None
