from src.agent.prompts import (
    SUPERVISOR_MULTI_INTENT_ROUTER_PROMPT,
    SQL_WORKER_PROMPT,
    RAG_REWRITE_CHECK_PROMPT,
    RAG_HYDE_PROMPT,
    ANALYST_CHITCHAT_PROMPT,
    ANALYST_INVESTMENT_PROMPT,
    ANALYST_MACRO_NEWS_PROMPT,
)
from src.agent.nodes.driver import node_driver
from src.agent.nodes.analyst import node_final_analyst, node_purge_state
from src.agent.nodes.sql_worker import node_sql_worker
from src.agent.nodes.rag_worker import node_rag_worker
from src.agent.nodes.security import node_security_shield
from src.agent.graph import workflow

def test_prompts_and_nodes_integrity():
    assert "[ROLE & OBJECTIVE]" in SUPERVISOR_MULTI_INTENT_ROUTER_PROMPT
    assert "[ROLE & OBJECTIVE]" in SQL_WORKER_PROMPT
    assert "[ROLE & OBJECTIVE]" in RAG_REWRITE_CHECK_PROMPT
    assert "[ROLE & OBJECTIVE]" in RAG_HYDE_PROMPT
    assert "TIẾNG VIỆT" in ANALYST_CHITCHAT_PROMPT.upper()
    assert "TIẾNG VIỆT" in ANALYST_INVESTMENT_PROMPT.upper()
    assert "TIẾNG VIỆT" in ANALYST_MACRO_NEWS_PROMPT.upper()
    assert workflow is not None
