from pydantic import BaseModel, Field
from langchain_ollama import ChatOllama
from src.agent.state import AgentState
from src.config import settings
from src.agent.prompts import SUPERVISOR_DRIVER_PROMPT

class DriverOutput(BaseModel):
    target: str = Field(default="UNKNOWN", description="Target asset symbol in uppercase")
    mode: str = Field(default="CHITCHAT", description="CHITCHAT or INVESTMENT")

async def node_driver(state: AgentState):
    """Supervisor Agent: Analyzes user intent and orchestrates Multi-Agent workflows (Async)"""
    human_messages = [m for m in state["messages"] if m.type == "human"]
    user_msg = human_messages[-1].content if human_messages else ""
    
    llm = ChatOllama(model=settings.llm_coder_model, temperature=0).with_structured_output(DriverOutput)
    
    from src.agent.callbacks import get_langfuse_handler
    handler = get_langfuse_handler()
    callbacks = [handler] if handler else []
    
    try:
        parsed: DriverOutput = await llm.ainvoke(
            f"{SUPERVISOR_DRIVER_PROMPT}\n\nUser Question: {user_msg}",
            config={"callbacks": callbacks}
        )
        mode = parsed.mode.upper() if parsed and parsed.mode else "CHITCHAT"
        target = parsed.target.upper() if parsed and parsed.target else "UNKNOWN"
        return {
            "current_target": target,
            "chat": True if mode == "CHITCHAT" else False,
            "next_worker": "FINAL_ANALYST" if mode == "CHITCHAT" else "PARALLEL_EXECUTE"
        }
    except Exception as e:
        return {"current_target": "UNKNOWN", "chat": True, "next_worker": "FINAL_ANALYST"}
