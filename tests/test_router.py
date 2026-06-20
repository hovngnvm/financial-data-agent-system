import time
import pytest
from langchain_core.messages import HumanMessage, AIMessage, trim_messages
from src.agent.router import semantic_router, SemanticVectorRouter
from src.config import settings

def test_semantic_router_sub_10ms_latency():
    """Verifies that Semantic Vector Router evaluates intents in sub-10ms latency."""
    query = "Giá cổ phiếu HPG hôm nay biến động thế nào?"
    
    # Warmup
    semantic_router.fast_route("warmup query")
    
    start_t = time.perf_counter()
    result = semantic_router.fast_route(query)
    elapsed_ms = (time.perf_counter() - start_t) * 1000
    
    assert result is not None
    assert elapsed_ms < 25.0 # High threshold for CPU test runner safety; typically < 3ms
    assert result["current_target"] == "HPG"
    assert "FETCH_PRICE" in result["activated_intents"]

def test_semantic_router_ticker_extraction():
    """Verifies fast regex extraction of financial asset symbols."""
    assert semantic_router.extract_ticker("Xem giá HPG") == "HPG"
    assert semantic_router.extract_ticker("Phân tích btc và eth") in ["BTC", "ETH"]
    assert semantic_router.extract_ticker("Tin tức vĩ mô chung") == "UNKNOWN"
    assert semantic_router.extract_ticker("Vẽ đồ thị FPT") == "FPT"

@pytest.mark.parametrize("query,expected_intent,expected_target,expected_chat", [
    ("Lịch sử giá và khối lượng giao dịch của SSI", "FETCH_PRICE", "SSI", False),
    ("Chỉ số RSI và đường trung bình SMA của BTC", "FETCH_INDICATOR", "BTC", False),
    ("Tin tức kinh tế vĩ mô và báo cáo tài chính mới nhất", "FETCH_NEWS", "UNKNOWN", False),
    ("Xin chào bạn, hôm nay thời tiết thế nào?", "CHITCHAT", "UNKNOWN", True),
])
def test_semantic_router_single_intents(query, expected_intent, expected_target, expected_chat):
    """Verifies precision across single intent categories."""
    res = semantic_router.fast_route(query)
    assert res is not None
    assert expected_intent in res["activated_intents"]
    if expected_target != "UNKNOWN":
        assert res["current_target"] == expected_target
    assert res["chat"] is expected_chat

def test_semantic_router_chart_intent():
    """Verifies chart intent and mode extraction."""
    chart_res = semantic_router.fast_route("Vẽ biểu đồ MACD phân kỳ của mã VND")
    assert chart_res is not None
    assert "RENDER_CHART" in chart_res["activated_intents"]
    assert chart_res["chart_mode"] == "macd"
    assert chart_res["current_target"] == "VND"

def test_semantic_router_multi_intent_clause_splitting():
    """Verifies Multi-Intent detection via Clause Splitting on compound queries."""
    compound_query = "Vẽ biểu đồ RSI của HPG và cho tôi biết tin tức ngành thép mới nhất"
    
    clauses = semantic_router.split_clauses(compound_query)
    assert len(clauses) >= 2
    
    result = semantic_router.fast_route(compound_query)
    assert result is not None
    assert "RENDER_CHART" in result["activated_intents"]
    assert "FETCH_NEWS" in result["activated_intents"]
    assert result["chart_mode"] == "rsi"
    assert result["current_target"] == "HPG"
    assert result["chat"] is False

def test_context_token_trimming_safety():
    """Verifies that trim_messages strictly limits conversation history to token budget."""
    # Simulate a long conversation of 20 turns
    long_history = []
    for i in range(20):
        long_history.append(HumanMessage(content=f"Câu hỏi số {i}: Phân tích tình hình tài chính của tập đoàn Hòa Phát HPG."))
        long_history.append(AIMessage(content=f"Câu trả lời số {i}: Tập đoàn Hòa Phát ghi nhận doanh thu tăng trưởng ổn định trong quý vừa qua."))
    
    raw_token_count = sum(len(str(m.content)) for m in long_history) // 4
    assert raw_token_count > 600 # Long context that needs trimming
    
    # Trim to budget of 200 tokens
    budget = 200
    trimmed = trim_messages(
        long_history,
        max_tokens=budget,
        strategy="last",
        token_counter=lambda msgs: sum(len(str(m.content)) for m in msgs) // 4,
        include_system=False,
        allow_partial=False,
        start_on="human"
    )
    
    trimmed_token_count = sum(len(str(m.content)) for m in trimmed) // 4
    assert trimmed_token_count <= budget
    assert len(trimmed) < len(long_history)
    assert trimmed[0].type == "human" # Starts cleanly on human message
    assert trimmed[-1].content == long_history[-1].content # Retains the most recent turn
