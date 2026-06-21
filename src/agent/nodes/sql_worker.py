from typing import Any
from src.agent.state import AgentState
from src.tools import tool_get_ticker_prices, tool_get_ticker_indicators

DEFAULT_SQL_RECORD_LIMIT: int = 30
MAX_SQL_RETRY_ATTEMPTS: int = 2

async def node_sql_worker(state: AgentState) -> dict[str, Any]:
    """SQL Agent Node: Retrieves quantitative data from ClickHouse using a Semantic Layer (Async)"""
    ticker = state.get("current_target", "UNKNOWN").upper()
    retries = state.get("retry_count", 0)
    intents = state.get("activated_intents", [])

    if "FETCH_INDICATOR" in intents:
        clean_data, error_msg = tool_get_ticker_indicators(ticker, DEFAULT_SQL_RECORD_LIMIT)
    else:
        clean_data, error_msg = tool_get_ticker_prices(ticker, DEFAULT_SQL_RECORD_LIMIT)
    
    if error_msg:
        if retries + 1 <= MAX_SQL_RETRY_ATTEMPTS:
            return {"retry_count": retries + 1, "next_worker": "SQL_RETRY"}
        return {
            "sql_data_output": [],
            "next_worker": "FINAL_ANALYST"
        }
    
    return {"sql_data_output": clean_data, "retry_count": 0}
