from typing import Any
import re
from pydantic import BaseModel, Field
from langchain_ollama import OllamaLLM
from src.agent.state import AgentState
from src.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

class SecurityCheckResult(BaseModel):
    status: str = Field(default="SAFE", description="SAFE or MALICIOUS")

SQL_INJECTION_REGEX = re.compile(
    r"(\b(DROP|DELETE|ALTER|TRUNCATE|UPDATE)\b\s+\b(TABLE|FROM|DATABASE|KEY)\b)|(--)|(\/\*|\*\/)",
    re.IGNORECASE
)

PROMPT_INJECTION_REGEX = re.compile(
    r"(ignore\s+previous\s+instructions|system\s+prompt|bypass\s+security)",
    re.IGNORECASE
)

async def node_security_shield(state: AgentState) -> dict[str, Any]:
    """Guardrails: Protect the system from Prompt Injection and malicious attacks (Async)"""
    human_messages = [m for m in state["messages"] if m.type == "human"]
    last_user_message = human_messages[-1].content if human_messages else ""
    if not last_user_message:
        return {"security_status": "SAFE", "next_worker": "CONTINUE"}
    
    # Rule-based guard check
    if SQL_INJECTION_REGEX.search(last_user_message) or PROMPT_INJECTION_REGEX.search(last_user_message):
        logger.warning(f"Security Alert: Rule-based regex matched malicious pattern in input: '{last_user_message}'")
        return {"security_status": "MALICIOUS", "next_worker": "FINISH"}
    
    from src.agent.callbacks import get_langfuse_handler
    handler = get_langfuse_handler()
    call_config = {"callbacks": [handler]} if handler else {}
    
    try:
        guard_llm = OllamaLLM(model=settings.llm_guard_model, temperature=0.0)
        raw_res = await guard_llm.ainvoke(
            f"User: {last_user_message}",
            config=call_config
        )
        status = "MALICIOUS" if "unsafe" in str(raw_res).lower() else "SAFE"
    except Exception as e:
        logger.warning(f"Llama-Guard service call failed ({e}). Falling back to Rule-based Sanitizer (Input allowed).")
        status = "SAFE"
        
    if status == "MALICIOUS":
        return {"security_status": "MALICIOUS", "next_worker": "FINISH"}
    else:
        return {"security_status": "SAFE", "next_worker": "CONTINUE"}
