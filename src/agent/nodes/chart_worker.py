import asyncio
import uuid
from pathlib import Path
from typing import Any
from langchain_core.messages import ToolMessage
from src.agent.state import AgentState
from src.agent.router import semantic_router
from src.tools import tool_generate_market_chart
from src.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

VALID_CHART_MODES: set[str] = {"comprehensive", "price_sma", "rsi", "macd", "volume"}

async def node_chart_worker(state: AgentState) -> dict[str, Any]:
    """
    Chart Agent Node: Dynamically determines appropriate chart type and renders multi-panel visualizations.
    """
    ticker = state.get("current_target", "")
    human_messages = [m for m in state.get("messages", []) if m.type == "human"]
    user_text = human_messages[-1].content if human_messages else ""
    
    predefined_mode = state.get("chart_mode")
    if predefined_mode in VALID_CHART_MODES:
        chart_type = predefined_mode
    else:
        chart_type = semantic_router.detect_chart_mode(user_text)
        
    session_uid = uuid.uuid4().hex[:8]
    clean_ticker = ticker.upper().strip() if ticker else "UNKNOWN"
    chart_dir = Path(settings.chart_file_path).parent
    output_chart_file = str(chart_dir / f"chart_{clean_ticker}_{session_uid}.png")
    logger.info(f"Rendering {chart_type} chart for target '{clean_ticker}' -> {output_chart_file}")

    chart_result = await asyncio.to_thread(
        tool_generate_market_chart,
        ticker=ticker,
        chart_type=chart_type,
        output_path=output_chart_file
    )
    
    return {
        "chart_status_msg": f"Rendered {chart_type} chart: {chart_result}",
        "chart_file_path": output_chart_file if "successfully" in chart_result else None,
        "next_worker": "FINAL_ANALYST",
        "messages": [ToolMessage(content=chart_result, name="chart_generator", tool_call_id="chart_generator_call")]
    }
