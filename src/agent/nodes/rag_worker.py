import asyncio
from typing import Any
from pydantic import BaseModel, Field
from langchain_ollama import ChatOllama, OllamaLLM
from src.agent.state import AgentState
from src.config import settings
from src.tools import tool_semantic_rag_search
from src.agent.prompts import RAG_REWRITE_CHECK_PROMPT, RAG_HYDE_PROMPT
from src.agent.callbacks import get_langfuse_handler
from src.utils.logger import get_logger

logger = get_logger(__name__)

class RAGRewriteCheck(BaseModel):
    need_rewrite: bool = Field(default=False, description="True if query needs HyDE rewrite")

_rag_structured_llm = None
_rag_plain_llm = None

def get_rag_structured_llm():
    global _rag_structured_llm
    if _rag_structured_llm is None:
        _rag_structured_llm = ChatOllama(model=settings.llm_coder_model, temperature=0).with_structured_output(RAGRewriteCheck)
    return _rag_structured_llm

def get_rag_plain_llm():
    global _rag_plain_llm
    if _rag_plain_llm is None:
        _rag_plain_llm = OllamaLLM(model=settings.llm_coder_model, temperature=0)
    return _rag_plain_llm

async def node_rag_worker(state: AgentState) -> dict[str, Any]:
    """RAG Agent Node: Explores macro-financial text context from Qdrant Vector DB (Async)"""
    human_messages = [m for m in state.get("messages", []) if m.type == "human"]
    user_msg = human_messages[-1].content if human_messages else ""
    
    llm_structured = get_rag_structured_llm()
    llm_plain = get_rag_plain_llm()

    handler = get_langfuse_handler()
    call_config = {"callbacks": [handler]} if handler else {}
    
    try:
        check_res: RAGRewriteCheck = await llm_structured.ainvoke(
            f"{RAG_REWRITE_CHECK_PROMPT}\n\nUser Question: {user_msg}", 
            config=call_config
        )
        need_rewrite = check_res.need_rewrite if check_res else False
    except Exception as e:
        logger.warning(f"RAG HyDE rewrite check failed: {e}. Skipping rewrite.")
        need_rewrite = False
        
    if need_rewrite:
        query_to_search = await llm_plain.ainvoke(
            f"{RAG_HYDE_PROMPT}\n\nUser Message: {user_msg}", 
            config=call_config
        )
    else:
        query_to_search = user_msg
    
    rag_context = await asyncio.to_thread(tool_semantic_rag_search, str(query_to_search))
    return {"rag_text_output": rag_context}
