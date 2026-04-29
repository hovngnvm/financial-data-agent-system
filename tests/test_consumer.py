import json
import pytest
from unittest.mock import AsyncMock, MagicMock
import pandas as pd
from aiokafka import ConsumerRecord

from src.consumer import process_market_batch
from src.config import settings
from src.database import db_manager

@pytest.fixture
def anyio_backend():
    return 'asyncio'

@pytest.fixture(autouse=True)
def mock_db_client(mocker):
    mock_client = MagicMock()
    # Mock return value of query_df to be empty DataFrame by default
    mock_client.query_df.return_value = pd.DataFrame()
    # Patch get_client and db_manager._client
    mocker.patch("clickhouse_connect.get_client", return_value=mock_client)
    db_manager._client = mock_client
    yield mock_client
    db_manager._client = None

@pytest.mark.anyio
async def test_process_market_batch_success(mocker):
    mock_ingest = mocker.patch("src.consumer.ingest_data_to_db")
    mocker.patch("src.consumer.tool_calculate_technical_indicators", side_effect=lambda df: df)
    
    mock_producer = AsyncMock()
    
    valid_payload = {
        "ingest_timestamp": "2026-07-18 11:00:00.000",
        "payload": {
            "symbol": "BTCUSDT",
            "price": 60000.0,
            "volume": 1.5
        }
    }
    
    msg = ConsumerRecord(
        topic="finagent_bronze_market",
        partition=0,
        offset=12,
        timestamp=1000,
        timestamp_type=0,
        key=None,
        value=json.dumps(valid_payload).encode("utf-8"),
        checksum=None,
        serialized_key_size=0,
        serialized_value_size=len(json.dumps(valid_payload)),
        headers=[]
    )
    
    await process_market_batch([msg], mock_producer)
    
    mock_ingest.assert_called_once()
    mock_producer.send_and_wait.assert_not_called()


@pytest.mark.anyio
async def test_process_market_batch_malformed_goes_to_dlq(mocker):
    mock_ingest = mocker.patch("src.consumer.ingest_data_to_db")
    mock_producer = AsyncMock()
    
    bad_msg = ConsumerRecord(
        topic="finagent_bronze_market",
        partition=0,
        offset=13,
        timestamp=1000,
        timestamp_type=0,
        key=None,
        value=b"invalid-json",
        checksum=None,
        serialized_key_size=0,
        serialized_value_size=12,
        headers=[]
    )
    
    await process_market_batch([bad_msg], mock_producer)
    
    mock_ingest.assert_not_called()
    mock_producer.send_and_wait.assert_called_once_with(settings.topic_market_dlq, mocker.ANY)
    
    args, kwargs = mock_producer.send_and_wait.call_args
    dlq_payload = args[1]
    assert dlq_payload["offset"] == 13
    assert dlq_payload["original_topic"] == "finagent_bronze_market"
    assert "invalid-json" in dlq_payload["raw_payload"]


@pytest.mark.anyio
async def test_process_market_batch_db_error_raises_exception(mocker):
    mocker.patch("src.consumer.tool_calculate_technical_indicators", side_effect=lambda df: df)
    # Mock DB ingest to throw an error
    mock_ingest = mocker.patch("src.consumer.ingest_data_to_db", side_effect=Exception("DB Connection Timeout"))
    
    mock_producer = AsyncMock()
    
    valid_payload = {
        "ingest_timestamp": "2026-07-18 11:00:00.000",
        "payload": {
            "symbol": "BTCUSDT",
            "price": 60000.0,
            "volume": 1.5
        }
    }
    msg = ConsumerRecord(
        topic="finagent_bronze_market",
        partition=0,
        offset=14,
        timestamp=1000,
        timestamp_type=0,
        key=None,
        value=json.dumps(valid_payload).encode("utf-8"),
        checksum=None,
        serialized_key_size=0,
        serialized_value_size=len(json.dumps(valid_payload)),
        headers=[]
    )
    
    with pytest.raises(Exception) as exc_info:
        await process_market_batch([msg], mock_producer)
    assert "DB Connection Timeout" in str(exc_info.value)


@pytest.mark.anyio
async def test_price_cache_manager_in_memory_fallback(mock_db_client, mocker):
    from src.consumer import PriceCacheManager
    from datetime import datetime
    
    cache = PriceCacheManager()
    cache.use_redis = False  # Force in-memory fallback
    
    # Mock ClickHouse history query to return a single historical record
    mock_history_df = pd.DataFrame([{
        "timestamp": datetime.strptime("2026-07-18 10:00:00.000000", "%Y-%m-%d %H:%M:%S.%f"),
        "symbol": "ETHUSDT",
        "price": 3000.0,
        "volume": 10.0
    }])
    mock_db_client.query_df.return_value = mock_history_df
    
    new_records = [
        {"timestamp": "2026-07-18 10:01:00.000000", "symbol": "ETHUSDT", "price": 3010.0, "volume": 5.0},
        {"timestamp": "2026-07-18 10:02:00.000000", "symbol": "ETHUSDT", "price": 3020.0, "volume": 8.0}
    ]
    
    # First call: should trigger cache miss and load the historical record
    window = await cache.get_and_update_window("ETHUSDT", new_records)
    
    # Window should contain the 1 historical record + 2 new records = 3 records total
    assert len(window) == 3
    assert window[0]["price"] == 3000.0
    assert window[1]["price"] == 3010.0
    assert window[2]["price"] == 3020.0
    
    # Second call with another new record: should NOT hit database (query_df should not be called again)
    mock_db_client.query_df.reset_mock()
    
    more_records = [
        {"timestamp": "2026-07-18 10:03:00.000000", "symbol": "ETHUSDT", "price": 3030.0, "volume": 2.0}
    ]
    window2 = await cache.get_and_update_window("ETHUSDT", more_records)
    
    assert len(window2) == 4
    assert window2[-1]["price"] == 3030.0
    mock_db_client.query_df.assert_not_called()
