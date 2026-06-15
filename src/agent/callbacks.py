import os
from src.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

_langfuse_handler = None

def get_langfuse_handler():
    """Initializes and returns the Langfuse tracing callback handler if valid credentials exist."""
    global _langfuse_handler
    if _langfuse_handler is None:
        pub_key = settings.langfuse_public_key
        sec_key = settings.langfuse_secret_key
        host = settings.langfuse_host
        
        if pub_key and sec_key and pub_key.startswith("pk-"):
            try:
                os.environ["LANGFUSE_PUBLIC_KEY"] = pub_key
                os.environ["LANGFUSE_SECRET_KEY"] = sec_key
                os.environ["LANGFUSE_HOST"] = host
                
                try:
                    from langfuse.callback import CallbackHandler
                    _langfuse_handler = CallbackHandler(
                        public_key=pub_key,
                        secret_key=sec_key,
                        host=host
                    )
                except Exception:
                    from langfuse.langchain import CallbackHandler
                    _langfuse_handler = CallbackHandler()
                
                logger.info(f"Langfuse telemetry tracing enabled connected to {host}")
            except Exception as e:
                logger.warning(f"Failed to initialize Langfuse callback handler: {e}")
                _langfuse_handler = None
    return _langfuse_handler
