import asyncio
from typing import Any
from src.agent.state import AgentState
from src.tools import tool_semantic_rag_search
from src.utils.logger import get_logger

logger = get_logger(__name__)


async def node_rag_worker(state: AgentState) -> dict[str, Any]:
    """RAG Agent Node: Explores macro-financial text context from Qdrant Vector DB (Async)"""
    human_messages = [m for m in state.get("messages", []) if m.type == "human"]
    user_msg = human_messages[-1].content if human_messages else ""
    rag_context, error = await asyncio.to_thread(tool_semantic_rag_search, str(user_msg))
    if error:
        logger.warning(f"RAG hybrid retrieval encountered an error: {error}")
        return {"rag_text_output": "", "rag_data_status": "error", "rag_error": error}
    return {"rag_text_output": rag_context, "rag_data_status": "ok" if rag_context else "empty", "rag_error": None}
