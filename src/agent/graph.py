from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

try:
    from langgraph.checkpoint.redis.aio import AsyncRedisSaver
except (ImportError, ModuleNotFoundError):
    AsyncRedisSaver = None

from redis import Redis

from src.config import settings
from src.agent.state import AgentState
from src.agent.nodes.security import node_security_shield
from src.agent.nodes.driver import node_driver
from src.agent.nodes.sql_worker import node_sql_worker
from src.agent.nodes.rag_worker import node_rag_worker
from src.agent.nodes.chart_worker import node_chart_worker
from src.agent.nodes.analyst import node_final_analyst, node_purge_state

def node_join_barrier(state: AgentState) -> dict[str, str]:
    """Barrier Node (Join): Synchronizes parallel agent execution branches before routing to the next step"""
    intents = state.get("activated_intents", [])
    has_chart_intent = "RENDER_CHART" in intents
    
    human_messages = [m for m in state.get("messages", []) if m.type == "human"]
    user_msg = human_messages[-1].content.lower() if human_messages else ""
    chart_keywords = ["biểu đồ", "đồ thị", "vẽ", "chart", "graph", "visualize", "draw"]
    needs_chart = has_chart_intent or any(kw in user_msg for kw in chart_keywords)
    
    if needs_chart:
        return {"next_worker": "Chart_Agent"}
    else:
        return {"next_worker": "FINAL_ANALYST"}

workflow = StateGraph(AgentState)

workflow.add_node("security_shield", node_security_shield)
workflow.add_node("driver", node_driver)
workflow.add_node("sql_worker", node_sql_worker)
workflow.add_node("rag_worker", node_rag_worker)
workflow.add_node("join_barrier", node_join_barrier)
workflow.add_node("chart_worker", node_chart_worker)
workflow.add_node("final_analyst", node_final_analyst)
workflow.add_node("state_purger", node_purge_state)

workflow.set_entry_point("security_shield")

# Router 1: Gateway Security Check
workflow.add_conditional_edges(
    "security_shield",
    lambda state: "blocked" if state["next_worker"] == "FINISH" else "pass",
    {"blocked": "final_analyst", "pass": "driver"}
)

def supervisor_fork_router(state: AgentState) -> list[str]:
    """Dynamically routes execution based on activated multi-intents."""
    if state.get("chat", False) or state.get("next_worker") != "PARALLEL_EXECUTE":
        return ["call_analyst_direct"]
        
    intents = state.get("activated_intents", [])
    branches = []
    
    # Check SQL worker requirement (price, indicators, or chart data)
    if not intents or any(i in intents for i in ["FETCH_PRICE", "FETCH_INDICATOR", "RENDER_CHART"]):
        branches.append("call_sql")
        
    # Check RAG worker requirement (qualitative news context)
    if not intents or "FETCH_NEWS" in intents:
        branches.append("call_rag")
        
    return branches if branches else ["call_analyst_direct"]

workflow.add_conditional_edges(
    "driver",
    supervisor_fork_router,
    {
        "call_sql": "sql_worker",
        "call_rag": "rag_worker",
        "call_analyst_direct": "final_analyst"
    }
)

# Self-Correction Loop for SQL Worker
workflow.add_conditional_edges(
    "sql_worker",
    lambda state: "self_correction" if state["next_worker"] == "SQL_RETRY" else "to_barrier",
    {
        "self_correction": "sql_worker",
        "to_barrier": "join_barrier"
    }
)

# Route RAG worker directly to the join barrier after completion
workflow.add_edge("rag_worker", "join_barrier")

# Router 3: Join Barrier Node (Fan-in Check)
workflow.add_conditional_edges(
    "join_barrier",
    lambda state: "call_chart" if state["next_worker"] == "Chart_Agent" else "call_analyst",
    {
        "call_chart": "chart_worker",
        "call_analyst": "final_analyst"
    }
)

# Route after chart generation is complete
workflow.add_edge("chart_worker", "final_analyst")

# End of processing lifecycle
workflow.add_edge("final_analyst", "state_purger")
workflow.add_edge("state_purger", END)

redis_url = f"redis://{settings.redis_host}:{settings.redis_port}"
if AsyncRedisSaver is not None:
    try:
        redis_checkpointer = AsyncRedisSaver(redis_url=redis_url)
    except Exception:
        redis_checkpointer = MemorySaver()
else:
    redis_checkpointer = MemorySaver()

app = workflow.compile(checkpointer=redis_checkpointer)
