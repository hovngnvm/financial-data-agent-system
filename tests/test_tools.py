from pathlib import Path
import pytest
import pandas as pd
from unittest.mock import MagicMock
from src.tools import (
    tool_calculate_technical_indicators,
    tool_get_ticker_prices,
    tool_get_ticker_indicators,
    tool_generate_market_chart,
    tool_semantic_rag_search
)

def test_tool_calculate_technical_indicators():
    data = {
        "timestamp": pd.date_range(start="2026-01-01", periods=30, freq="D"),
        "symbol": ["BTC"] * 30,
        "price": [100.0 + i * 2.0 for i in range(30)]
    }
    df = pd.DataFrame(data)
    result_df = tool_calculate_technical_indicators(df)
    
    assert "SMA_5" in result_df.columns
    assert "SMA_20" in result_df.columns
    assert "RSI" in result_df.columns
    assert "MACD" in result_df.columns
    assert "MACD_Signal" in result_df.columns
    assert len(result_df) == 30
    assert not result_df["SMA_5"].isna().any()

def test_tool_get_ticker_prices_success(mocker):
    mock_df = pd.DataFrame({
        "timestamp": [pd.Timestamp("2026-01-01 10:00:00")],
        "symbol": ["HPG"],
        "price": [28000.0],
        "volume": [10000.0]
    })
    mock_client = MagicMock()
    mock_client.query_df.return_value = mock_df
    mocker.patch("src.tools.db_manager._client", mock_client)
    
    records, error = tool_get_ticker_prices("HPG", limit=10)
    assert error is None
    assert len(records) == 1
    assert records[0]["symbol"] == "HPG"
    assert records[0]["price"] == 28000.0

def test_tool_get_ticker_indicators_success(mocker):
    mock_df = pd.DataFrame({
        "timestamp": [pd.Timestamp("2026-01-01 10:00:00")],
        "symbol": ["HPG"],
        "price": [28000.0],
        "volume": [10000.0],
        "SMA_5": [27500.0],
        "SMA_20": [27000.0],
        "RSI": [55.0],
        "MACD": [120.0],
        "MACD_Signal": [100.0]
    })
    mock_client = MagicMock()
    mock_client.query_df.return_value = mock_df
    mocker.patch("src.tools.db_manager._client", mock_client)
    
    records, error = tool_get_ticker_indicators("HPG", limit=10)
    assert error is None
    assert len(records) == 1
    assert records[0]["SMA_5"] == 27500.0
    assert records[0]["RSI"] == 55.0

@pytest.mark.parametrize("chart_type", ["comprehensive", "price_sma", "rsi", "macd", "volume"])
def test_tool_generate_market_chart_modes(mocker, chart_type, tmp_path):
    mock_df = pd.DataFrame({
        "timestamp": pd.date_range(start="2026-01-01", periods=10, freq="D"),
        "symbol": ["HPG"] * 10,
        "price": [28000.0 + i * 100 for i in range(10)],
        "volume": [50000.0] * 10,
        "SMA_5": [28000.0] * 10,
        "SMA_20": [27500.0] * 10,
        "RSI": [60.0] * 10,
        "MACD": [150.0] * 10,
        "MACD_Signal": [140.0] * 10
    })
    mock_client = MagicMock()
    mock_client.query_df.return_value = mock_df
    mocker.patch("src.tools.db_manager._client", mock_client)
    
    test_chart_file = str(tmp_path / f"test_chart_{chart_type}.png")
    mocker.patch("src.tools.OUTPUT_CHART_PATH", test_chart_file)
    
    msg = tool_generate_market_chart("HPG", chart_type=chart_type, limit=10)
    assert f"Rendered {chart_type} chart successfully" in msg
    assert Path(test_chart_file).exists()

def test_tool_generate_market_chart_unknown_ticker():
    msg = tool_generate_market_chart("UNKNOWN")
    assert "No valid ticker specified" in msg

def test_tool_semantic_rag_search_empty(mocker):
    mocker.patch("src.tools.qdrant_client.query_points", return_value=MagicMock(points=[]))
    mocker.patch("src.tools.get_embedding_model")
    mocker.patch("src.tools._text_to_sparse_vector", return_value={"indices": [], "values": []})
    
    result = tool_semantic_rag_search("Macroeconomic forecast")
    assert "No related macro-financial documents found" in result
