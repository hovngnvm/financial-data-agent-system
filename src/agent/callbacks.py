import os
import sys
import types
from src.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Ensure Langchain 1.x module compatibility for Langfuse 2.x callback loader
try:
    import langchain_core.callbacks.base
    import langchain_core.documents
    import langchain_core.agents

    if "langchain.callbacks.base" not in sys.modules:
        lc_callbacks_base = types.ModuleType("langchain.callbacks.base")
        lc_callbacks_base.BaseCallbackHandler = langchain_core.callbacks.base.BaseCallbackHandler
        sys.modules["langchain.callbacks"] = types.ModuleType("langchain.callbacks")
        sys.modules["langchain.callbacks.base"] = lc_callbacks_base

    if "langchain.schema.document" not in sys.modules:
        lc_schema_doc = types.ModuleType("langchain.schema.document")
        lc_schema_doc.Document = langchain_core.documents.Document
        lc_schema_agent = types.ModuleType("langchain.schema.agent")
        lc_schema_agent.AgentAction = langchain_core.agents.AgentAction
        lc_schema_agent.AgentFinish = langchain_core.agents.AgentFinish
        sys.modules["langchain.schema"] = types.ModuleType("langchain.schema")
        sys.modules["langchain.schema.document"] = lc_schema_doc
        sys.modules["langchain.schema.agent"] = lc_schema_agent
except Exception as bridge_err:
    logger.debug(f"Langchain compatibility bridge exception: {bridge_err}")

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
