import asyncio
from typing import Any
from src.agent.state import AgentState
from src.tools import tool_get_ticker_prices, tool_get_ticker_indicators
from src.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

MAX_SQL_RETRY_ATTEMPTS: int = 2
SQL_RETRY_BACKOFF_SECONDS: float = 0.5


async def node_sql_worker(state: AgentState) -> dict[str, Any]:
    """SQL Agent Node: Retrieves quantitative data from ClickHouse using a Semantic Layer (Async)"""
    ticker = state.get("current_target", "UNKNOWN").upper()
    if ticker == "UNKNOWN":
        return {"sql_data_output": []}

    intents = state.get("activated_intents", [])
    fetch_tool = tool_get_ticker_indicators if "FETCH_INDICATOR" in intents else tool_get_ticker_prices
    limit = settings.default_prices_limit

    for attempt in range(MAX_SQL_RETRY_ATTEMPTS + 1):
        clean_data, error_msg = await asyncio.to_thread(fetch_tool, ticker, limit)
        if not error_msg:
            return {"sql_data_output": clean_data}
        logger.warning(
            f"ClickHouse query attempt {attempt + 1}/{MAX_SQL_RETRY_ATTEMPTS + 1} for '{ticker}' failed: {error_msg}"
        )
        if attempt < MAX_SQL_RETRY_ATTEMPTS:
            await asyncio.sleep(SQL_RETRY_BACKOFF_SECONDS)

    logger.error(f"Exhausted all retries querying ClickHouse for ticker '{ticker}'.")
    return {"sql_data_output": []}
