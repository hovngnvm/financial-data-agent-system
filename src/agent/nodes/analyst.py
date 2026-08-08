import json
from langchain_ollama import OllamaLLM
from src.agent.state import AgentState
from src.config import settings
from src.agent.prompts import ANALYST_CHITCHAT_PROMPT, ANALYST_INVESTMENT_PROMPT

async def node_final_analyst(state: AgentState):
    """Analyst Agent: Synthesizes quantitative data, RAG context, and chart status (Async)"""
    llm_analyst = OllamaLLM(model=settings.llm_analyst_model, temperature=0.3)
    
    conversation_history = state["messages"]
    user_msg = conversation_history[-1].content if conversation_history else ""
        
    if state.get("chat", False):
        system_prompt = ANALYST_CHITCHAT_PROMPT
        prompt_payload = f"=== USER QUESTION ===\n{user_msg}"
    else:
        target = state.get("current_target", "UNKNOWN")
        sql_numbers = state.get("sql_data_output", [])
        rag_news = state.get("rag_text_output", "No related macro news found.")
        chart_info = state.get("chart_status_msg", "No chart generated.")

        prompt_payload = (
            "TARGET ASSET PROFILE:\n"
            f"Target: {target}\n"
            "=== QUANTITATIVE HISTORICAL DATA (ClickHouse) ===\n"
            f"{json.dumps(sql_numbers, indent=2) if sql_numbers else 'No time-series data available.'}\n\n"
            "=== QUALITATIVE NEWS CONTEXT (Qdrant RAG) ===\n"
            f"{rag_news}\n\n"
            "=== VISUALIZATION STATUS ===\n"
            f"{chart_info}\n\n"
            "=== CHAT HISTORY ===\n"
            f"{conversation_history}"
        )
        system_prompt = ANALYST_INVESTMENT_PROMPT
    
    from src.agent.callbacks import get_langfuse_handler
    handler = get_langfuse_handler()
    callbacks = [handler] if handler else []
    
    final_response = await llm_analyst.ainvoke(
        f"{system_prompt}\n\n{prompt_payload}",
        config={"callbacks": callbacks}
    )
    return {"messages": [{"role": "assistant", "content": final_response}], "next_worker": "PURGE"}

async def node_purge_state(state: AgentState):
    """State Purger: Resets transient execution errors while preserving conversation context across turns"""
    return {
        "security_status": "SAFE",
        "chat": False,
        "error_log": "",
        "retry_count": 0,
        "next_worker": "FINISH"
    }
