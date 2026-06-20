import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from telegram.ext import Application
from src.telegram_bot import post_init, _send_and_cleanup_chart
from src.agent.nodes.analyst import node_purge_state
from src.agent.graph import get_checkpointer
from langgraph.checkpoint.memory import MemorySaver


@pytest.mark.anyio
async def test_post_init_with_memory_saver(mocker):
    """Verifies post_init runs smoothly without AttributeError when checkpointer is MemorySaver."""
    mocker.patch("src.telegram_bot.redis_checkpointer", MemorySaver())
    mock_app = MagicMock(spec=Application)

    # Should not raise AttributeError
    await post_init(mock_app)


@pytest.mark.anyio
async def test_post_init_with_async_checkpointer(mocker):
    """Verifies post_init invokes asetup() when checkpointer has an async setup method."""
    mock_saver = MagicMock()
    mock_saver.asetup = AsyncMock()
    mocker.patch("src.telegram_bot.redis_checkpointer", mock_saver)
    mock_app = MagicMock(spec=Application)

    await post_init(mock_app)
    mock_saver.asetup.assert_awaited_once()


@pytest.mark.anyio
async def test_chart_file_path_preserved_through_state_purger():
    """Verifies node_purge_state does not wipe chart_file_path from state outputs."""
    sample_state = {
        "security_status": "SAFE",
        "chat": False,
        "sql_data_output": [{"ticker": "HPG", "price": 28500}],
        "rag_text_output": "Financial report context",
        "chart_status_msg": "Rendered successfully",
        "chart_file_path": "data/exports/session_chart.png",
        "activated_intents": ["RENDER_CHART"],
        "chart_mode": "candlestick",
        "error_log": "",
        "retry_count": 0,
        "next_worker": "PURGE"
    }

    purged_dict = await node_purge_state(sample_state)

    # chart_file_path must NOT be overwritten with None in purged state dict
    assert "chart_file_path" not in purged_dict
    assert purged_dict["next_worker"] == "FINISH"
    assert purged_dict["sql_data_output"] == []


@pytest.mark.anyio
async def test_send_and_cleanup_chart(tmp_path):
    """Verifies _send_and_cleanup_chart delivers photo and unlinks the temporary file."""
    test_file = tmp_path / "temp_chart.png"
    test_file.write_bytes(b"dummy_image_data")
    assert test_file.exists()

    mock_context = MagicMock()
    mock_context.bot.send_chat_action = AsyncMock()
    mock_context.bot.send_photo = AsyncMock()

    await _send_and_cleanup_chart(
        context=mock_context,
        chat_id=123456,
        chart_path=str(test_file),
        caption="Test chart"
    )

    mock_context.bot.send_chat_action.assert_awaited_once_with(chat_id=123456, action="upload_photo")
    mock_context.bot.send_photo.assert_awaited_once()
    assert not test_file.exists(), "Temporary chart file should be deleted after delivery"


def test_get_checkpointer_fallback(mocker):
    """Verifies get_checkpointer safely returns MemorySaver when Redis is unreachable."""
    mocker.patch("socket.create_connection", side_effect=ConnectionRefusedError("Connection refused"))
    checkpointer = get_checkpointer()
    assert isinstance(checkpointer, MemorySaver)
