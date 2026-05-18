from typing import Any
from langchain_core.messages import ToolMessage
from src.agent.state import AgentState
from src.tools import tool_generate_market_chart

async def node_chart_worker(state: AgentState) -> dict[str, Any]:
    """
    Chart Agent Node: Dynamically determines appropriate chart type and renders multi-panel visualizations.
    """
    ticker = state.get("current_target", "")
    
    human_messages = [m for m in state.get("messages", []) if m.type == "human"]
    user_text = human_messages[-1].content.lower() if human_messages else ""
    
    predefined_mode = state.get("chart_mode")
    if predefined_mode in ["comprehensive", "price_sma", "rsi", "macd", "volume"]:
        chart_type = predefined_mode
    elif any(kw in user_text for kw in ["rsi", "quá mua", "quá bán", "overbought", "oversold"]):
        chart_type = "rsi"
    elif any(kw in user_text for kw in ["macd", "phân kỳ", "divergence", "động lượng", "momentum"]):
        chart_type = "macd"
    elif any(kw in user_text for kw in ["volume", "khối lượng", "thanh khoản"]):
        chart_type = "volume"
    elif any(kw in user_text for kw in ["sma", "đường trung bình", "moving average", "ma5", "ma20"]):
        chart_type = "price_sma"
    else:
        chart_type = "comprehensive"
        
    chart_result = tool_generate_market_chart(ticker=ticker, chart_type=chart_type)
    
    return {
        "chart_status_msg": f"Rendered {chart_type} chart: {chart_result}", 
        "next_worker": "FINAL_ANALYST",
        "messages": [ToolMessage(content=chart_result, name="chart_generator", tool_call_id="chart_generator_call")]
    }
