import re
from typing import Any
from langchain_core.messages import AIMessage
from src.agent.state import AgentState
from src.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

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

async def node_security_shield(state: AgentState) -> dict[str, Any]:
    """Guardrails: Deterministic regex and length-based input security check (Async)"""
    human_messages = [m for m in state.get("messages", []) if m.type == "human"]
    last_user_message = human_messages[-1].content if human_messages else ""
    if not last_user_message:
        return {"security_status": "SAFE", "next_worker": "CONTINUE", "chart_file_path": None}

    if len(last_user_message) > settings.max_input_length or SQL_INJECTION_REGEX.search(last_user_message) or PROMPT_INJECTION_REGEX.search(last_user_message):
        logger.warning(f"Security Alert: Blocked input: '{last_user_message[:100]}...'")
        return {
            "security_status": "MALICIOUS",
            "next_worker": "FINISH",
            "chart_file_path": None,
            "messages": [AIMessage(content=SECURITY_REFUSAL_MESSAGE)]
        }

    return {"security_status": "SAFE", "next_worker": "CONTINUE", "chart_file_path": None}
