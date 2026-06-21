import functools
from typing import Any
from pydantic import BaseModel, Field
from langchain_ollama import ChatOllama
from src.agent.state import AgentState
from src.config import settings
from src.agent.prompts import SUPERVISOR_MULTI_INTENT_ROUTER_PROMPT
from src.agent.router import semantic_router
from src.agent.callbacks import get_langfuse_handler
from src.utils.logger import get_logger

logger = get_logger(__name__)

class MultiIntentRoutePlan(BaseModel):
    target: str = Field(default="UNKNOWN", description="Target asset ticker in uppercase (e.g. HPG, BTC, ETH)")
    activated_intents: list[str] = Field(
        default=["CHITCHAT"],
        description="List of activated intents: FETCH_PRICE, FETCH_INDICATOR, FETCH_NEWS, RENDER_CHART, CHITCHAT"
    )
    chart_mode: str | None = Field(
        default=None,
        description="Chart mode: comprehensive, price_sma, rsi, macd, volume"
    )

@functools.cache
def get_driver_llm():
    return ChatOllama(model=settings.llm_coder_model, temperature=0).with_structured_output(MultiIntentRoutePlan)

async def node_driver(state: AgentState) -> dict[str, Any]:
    """
    Supervisor Agent: Analyzes user intent using a Hybrid Routing Architecture:
    1. Fast-Path: Semantic Vector Router (< 3ms) via Cosine Similarity on prototype centroids.
    2. Fallback: LLM Multi-Intent Decomposition via Structured Output when confidence is low.
    """
    human_messages = [m for m in state.get("messages", []) if m.type == "human"]
    user_msg = human_messages[-1].content if human_messages else ""

    # Fast-Path: Try Semantic Vector Router first
    try:
        fast_result = semantic_router.fast_route(user_msg)
        if fast_result is not None:
            logger.info(f"Fast-Path Semantic Router matched: {fast_result['activated_intents']} for {fast_result['current_target']}")
            return fast_result
    except Exception as e:
        logger.warning(f"Semantic router fast-path evaluation error: {e}. Falling back to LLM.")

    llm = get_driver_llm()
    handler = get_langfuse_handler()
    call_config = {"callbacks": [handler]} if handler else {}
    
    try:
        parsed: MultiIntentRoutePlan = await llm.ainvoke(
            f"{SUPERVISOR_MULTI_INTENT_ROUTER_PROMPT}\n\nUser Question: {user_msg}",
            config=call_config
        )
        intents = parsed.activated_intents if parsed and parsed.activated_intents else ["CHITCHAT"]
        target = parsed.target.upper() if parsed and parsed.target else "UNKNOWN"
        chart_mode = parsed.chart_mode if parsed else None
        
        is_chitchat = "CHITCHAT" in intents and len(intents) == 1
        
        return {
            "current_target": target,
            "activated_intents": intents,
            "chart_mode": chart_mode,
            "chat": is_chitchat,
            "next_worker": "FINAL_ANALYST" if is_chitchat else "PARALLEL_EXECUTE"
        }
    except Exception as e:
        logger.warning(f"Supervisor LLM parsing failed: {e}. Defaulting to CHITCHAT.")
        return {
            "current_target": "UNKNOWN",
            "activated_intents": ["CHITCHAT"],
            "chart_mode": None,
            "chat": True,
            "next_worker": "FINAL_ANALYST"
        }
