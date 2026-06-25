import asyncio
import uuid
from pathlib import Path
from typing import Any, get_args
from src.agent.state import AgentState, ChartMode
from src.agent.router import semantic_router
from src.tools import tool_generate_market_chart
from src.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

VALID_CHART_MODES: set[str] = set(get_args(ChartMode))

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
        output_path=output_chart_file,
        prefetched_data=state.get("sql_data_output")
    )
    
    success = "successfully" in chart_result
    return {
        "chart_status_msg": f"Rendered {chart_type} chart: {chart_result}",
        "chart_data_status": "ok" if success else "error",
        "chart_file_path": output_chart_file if success else None,
        "next_worker": "FINAL_ANALYST"
    }
