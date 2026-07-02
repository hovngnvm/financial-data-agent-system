from langgraph.graph import StateGraph, END
from langgraph.checkpoint.redis import RedisSaver
from redis import Redis

from src.config import settings
from src.agent.state import AgentState
from src.agent.nodes.security import node_security_shield
from src.agent.nodes.driver import node_driver
from src.agent.nodes.sql_worker import node_sql_worker
from src.agent.nodes.rag_worker import node_rag_worker
from src.agent.nodes.chart_worker import node_chart_worker
from src.agent.nodes.analyst import node_final_analyst, node_purge_state

def node_join_barrier(state: AgentState):
    """Barrier Node (Join): Synchronizes parallel agent execution branches before routing to the next step"""
    human_messages = [m for m in state["messages"] if m.type == "human"]
    user_msg = human_messages[-1].content.lower() if human_messages else ""
    
    # Check if the user explicitly wants to generate or visualize a chart
    chart_keywords = ["biểu đồ", "đồ thị", "vẽ", "chart", "graph", "visualize", "draw"]
    needs_chart = any(kw in user_msg for kw in chart_keywords)
    
    if needs_chart:
        return {"next_worker": "Chart_Agent"}
    else:
        return {"next_worker": "FINAL_ANALYST"}

# =====================================================================
# SETUP PARALLEL MULTI-AGENT LANGGRAPH WORKFLOW
# =====================================================================
workflow = StateGraph(AgentState)

# Declare all Agent Nodes
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

# Router 2: Supervisor Forking Router (Intelligent Branching)
def supervisor_fork_router(state: AgentState):
    next_step = state["next_worker"]
    if next_step == "PARALLEL_EXECUTE":
        # Return a list of nodes to activate parallel execution (fan-out)
        return ["call_sql", "call_rag"] 
    else:
        return ["call_analyst_direct"]

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

# 1. Initialize a persistent Redis client
redis_client = Redis(host=settings.redis_host, port=settings.redis_port)

# 2. Wrap client in LangGraph RedisSaver
redis_checkpointer = RedisSaver(redis_client)

# 3. Compile the graph using Redis as external memory instead of in-memory RAM
app = workflow.compile(checkpointer=redis_checkpointer)
