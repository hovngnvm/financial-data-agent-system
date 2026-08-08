import re
from pydantic import BaseModel, Field
from langchain_ollama import ChatOllama
from src.agent.state import AgentState
from src.logger import get_logger
from src.agent.prompts import SECURITY_SHIELD_PROMPT

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

async def node_security_shield(state: AgentState):
    """Guardrails: Protect the system from Prompt Injection and malicious attacks (Async)"""
    human_messages = [m for m in state["messages"] if m.type == "human"]
    last_user_message = human_messages[-1].content if human_messages else ""
    if not last_user_message:
        return {"security_status": "SAFE", "next_worker": "CONTINUE"}
    
    # 1. Immediate Rule-based check
    if SQL_INJECTION_REGEX.search(last_user_message) or PROMPT_INJECTION_REGEX.search(last_user_message):
        logger.warning(f"Security Alert: Rule-based regex matched malicious pattern in input: '{last_user_message}'")
        return {"security_status": "MALICIOUS", "next_worker": "FINISH"}
    
    # 2. LLM-based Llama-Guard Check with Pydantic Structured Output
    llm_json = ChatOllama(model="llama-guard3:1b-q5_K_S", temperature=0.0).with_structured_output(SecurityCheckResult)
    
    from src.agent.callbacks import get_langfuse_handler
    handler = get_langfuse_handler()
    callbacks = [handler] if handler else []
    
    try:
        response: SecurityCheckResult = await llm_json.ainvoke(
            [{"role": "user", "content": f"{SECURITY_SHIELD_PROMPT}\nInput: {last_user_message}"}], 
            config={"callbacks": callbacks}
        )
        status = response.status.upper() if response and response.status else "SAFE"
    except Exception as e:
        logger.warning(f"Llama-Guard service call failed ({e}). Falling back to Rule-based Sanitizer (Input allowed).")
        status = "SAFE"
        
    if status == "MALICIOUS":
        return {"security_status": "MALICIOUS", "next_worker": "FINISH"}
    else:
        return {"security_status": "SAFE", "next_worker": "CONTINUE"}
