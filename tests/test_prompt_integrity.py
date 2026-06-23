import pytest
from src.agent.prompts import (
    SUPERVISOR_MULTI_INTENT_ROUTER_PROMPT,
    ANALYST_CHITCHAT_PROMPT,
    ANALYST_INVESTMENT_PROMPT,
    ANALYST_MACRO_NEWS_PROMPT,
)
from src.agent.graph import workflow

@pytest.mark.parametrize("prompt", [
    SUPERVISOR_MULTI_INTENT_ROUTER_PROMPT,
])
def test_structured_prompts_have_role_and_objective(prompt):
    assert "[ROLE & OBJECTIVE]" in prompt

@pytest.mark.parametrize("analyst_prompt", [
    ANALYST_CHITCHAT_PROMPT,
    ANALYST_INVESTMENT_PROMPT,
    ANALYST_MACRO_NEWS_PROMPT,
])
def test_analyst_prompts_enforce_vietnamese_language(analyst_prompt):
    assert "TIẾNG VIỆT" in analyst_prompt.upper()

def test_workflow_graph_compiled():
    assert workflow is not None
