from langfuse.callback import CallbackHandler
from src.config import settings

_langfuse_handler = None

def get_langfuse_handler():
    global _langfuse_handler
    if _langfuse_handler is None:
        # Check if Langfuse credentials are valid (avoid default placeholders)
        pub_key = settings.langfuse_public_key
        sec_key = settings.langfuse_secret_key
        host = settings.langfuse_host
        
        if pub_key and "vừa_lấy" not in pub_key:
            try:
                _langfuse_handler = CallbackHandler(
                    public_key=pub_key,
                    secret_key=sec_key,
                    host=host
                )
            except Exception:
                _langfuse_handler = None
    return _langfuse_handler
