from typing import Any
import re
from pydantic import BaseModel, Field
from langchain_core.messages import AIMessage
from langchain_ollama import OllamaLLM
from src.agent.state import AgentState
from src.config import settings
from src.agent.callbacks import get_langfuse_handler
from src.utils.logger import get_logger

logger = get_logger(__name__)

class SecurityCheckResult(BaseModel):
    status: str = Field(default="SAFE", description="SAFE or MALICIOUS")

SQL_INJECTION_REGEX = re.compile(
    r"(\b(DROP|DELETE|ALTER|TRUNCATE|UPDATE|INSERT|GRANT|REVOKE)\b\s+\b(TABLE|FROM|DATABASE|KEY|INTO)\b)|(--)|(\/\*|\*\/)|(\bUNION\b\s+\bSELECT\b)|(;\s*\b(DROP|DELETE|ALTER)\b)",
    re.IGNORECASE
)

PROMPT_INJECTION_REGEX = re.compile(
    r"(ignore\s+(all\s+)?(previous|prior)\s+(instructions|directives|prompts)|system\s+prompt|bypass\s+security|jailbreak|act\s+as\s+(dan|an\s+unfiltered)|disregard\s+(all\s+)?(instructions|rules))",
    re.IGNORECASE
)

SECURITY_REFUSAL_MESSAGE: str = (
    "Cảnh báo an ninh: Yêu cầu của bạn chứa cú pháp không hợp lệ hoặc tiềm ẩn nguy cơ an toàn thông tin. "
    "Hệ thống đã từ chối xử lý để bảo vệ an ninh hệ thống."
)

_guard_llm = None

def get_guard_llm():
    global _guard_llm
    if _guard_llm is None:
        _guard_llm = OllamaLLM(model=settings.llm_guard_model, temperature=0.0)
    return _guard_llm

async def node_security_shield(state: AgentState) -> dict[str, Any]:
    """Guardrails: Protect the system from Prompt Injection and malicious attacks (Async)"""
    human_messages = [m for m in state.get("messages", []) if m.type == "human"]
    last_user_message = human_messages[-1].content if human_messages else ""
    if not last_user_message:
        return {"security_status": "SAFE", "next_worker": "CONTINUE", "chart_file_path": None}
    
    # Rule-based guard check
    if SQL_INJECTION_REGEX.search(last_user_message) or PROMPT_INJECTION_REGEX.search(last_user_message):
        logger.warning(f"Security Alert: Rule-based regex matched malicious pattern in input: '{last_user_message}'")
        return {
            "security_status": "MALICIOUS",
            "next_worker": "FINISH",
            "chart_file_path": None,
            "messages": [AIMessage(content=SECURITY_REFUSAL_MESSAGE)]
        }
    
    handler = get_langfuse_handler()
    call_config = {"callbacks": [handler]} if handler else {}
    
    try:
        guard_llm = get_guard_llm()
        raw_res = await guard_llm.ainvoke(
            f"User: {last_user_message}",
            config=call_config
        )
        status = "MALICIOUS" if "unsafe" in str(raw_res).lower() else "SAFE"
    except Exception as e:
        logger.warning(f"Llama-Guard service call failed ({e}). Falling back to Rule-based Sanitizer (Input allowed).")
        status = "SAFE"
        
    if status == "MALICIOUS":
        return {
            "security_status": "MALICIOUS",
            "next_worker": "FINISH",
            "chart_file_path": None,
            "messages": [AIMessage(content=SECURITY_REFUSAL_MESSAGE)]
        }
    else:
        return {"security_status": "SAFE", "next_worker": "CONTINUE", "chart_file_path": None}
