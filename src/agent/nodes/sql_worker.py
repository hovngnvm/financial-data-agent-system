from pydantic import BaseModel, Field
from langchain_ollama import ChatOllama
from src.agent.state import AgentState
from src.config import settings
from src.tools import tool_get_ticker_prices, tool_get_ticker_indicators
from src.agent.prompts import SQL_WORKER_PROMPT

class SQLWorkerParams(BaseModel):
    action: str = Field(default="get_prices", description="get_prices or get_indicators")
    ticker: str = Field(default="", description="Target symbol in uppercase")
    limit: int = Field(default=30, description="Number of records to retrieve (1-30)")

async def node_sql_worker(state: AgentState):
    """SQL Agent Node: Retrieves quantitative data from ClickHouse using a Semantic Layer (Async)"""
    human_messages = [m for m in state["messages"] if m.type == "human"]
    user_msg = human_messages[-1].content if human_messages else ""
    ticker = state.get("current_target", "")
    retries = state.get("retry_count", 0)
    error = state.get("error_log", "")
    
    llm = ChatOllama(model=settings.llm_coder_model, temperature=0).with_structured_output(SQLWorkerParams)

    prompt_text = f"{SQL_WORKER_PROMPT}\n\nUser Question: {user_msg}\nTarget Ticker: {ticker}"
    if error:
        prompt_text += f"\n\n[CRITICAL WARNING - FIX REQUIRED]: Previous attempt failed with: '{error}'."

    from src.agent.callbacks import get_langfuse_handler
    handler = get_langfuse_handler()
    callbacks = [handler] if handler else []
    
    try:
        params: SQLWorkerParams = await llm.ainvoke(prompt_text, config={"callbacks": callbacks})
        action = params.action if params and params.action else "get_prices"
        target_ticker = (params.ticker or ticker).upper()
        limit = params.limit if params and 1 <= params.limit <= 30 else 30
    except Exception as e:
        action = "get_prices"
        target_ticker = ticker.upper()
        limit = 30

    if action == "get_indicators":
        clean_data, error_msg = tool_get_ticker_indicators(target_ticker, limit)
    else:
        clean_data, error_msg = tool_get_ticker_prices(target_ticker, limit)
    
    if error_msg:
        if retries + 1 <= 2:
            return {"error_log": error_msg, "retry_count": retries + 1, "next_worker": "SQL_RETRY"}
        else:
            return {"error_log": f"ClickHouse failure: {error_msg}", "sql_data_output": [], "next_worker": "JOIN_BARRIER"}
    
    return {"sql_data_output": clean_data, "error_log": "", "retry_count": 0, "next_worker": "JOIN_BARRIER"}
