from langchain_core.messages import ToolMessage
from src.agent.state import AgentState
from src.tools import tool_generate_market_chart

async def node_chart_worker(state: AgentState):
    """Chart Agent Node: Generates and exports price fluctuation charts (Async)"""
    ticker = state.get("current_target", "")
    chart_result = tool_generate_market_chart(ticker)
    
    return {
        "chart_status_msg": f"Success. {chart_result}", 
        "next_worker": "FINAL_ANALYST",
        "messages": [ToolMessage(content=chart_result, name="chart_generator", tool_call_id="chart_generator_call")]
    }
