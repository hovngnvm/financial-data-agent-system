import pytest
import pandas as pd
from unittest.mock import MagicMock, PropertyMock
from src.database import DatabaseManager

def test_database_manager_init(mocker):
    # Verify manager attributes are loaded from settings
    db_mgr = DatabaseManager()
    assert db_mgr.host is not None
    assert db_mgr.port is not None
    assert db_mgr.user is not None
    assert db_mgr.database is not None

def test_database_manager_ingest_df_append(mocker):
    mock_client = MagicMock()
    mocker.patch("clickhouse_connect.get_client", return_value=mock_client)
    
    db_mgr = DatabaseManager()
    mocker.patch.object(DatabaseManager, 'client', new_callable=PropertyMock, return_value=mock_client)
    
    df = pd.DataFrame({"symbol": ["HPG"], "price": [28000.0]})
    db_mgr.ingest_df(df, table_name="prices", mode="append")
    
    mock_client.insert_df.assert_called_once()
    args, kwargs = mock_client.insert_df.call_args
    assert args[0] == "prices"
    assert isinstance(args[1], pd.DataFrame)
    assert args[1]['symbol'].iloc[0] == "HPG"

def test_database_manager_ingest_df_replace(mocker):
    mock_client = MagicMock()
    mocker.patch("clickhouse_connect.get_client", return_value=mock_client)
    
    db_mgr = DatabaseManager()
    mocker.patch.object(DatabaseManager, 'client', new_callable=PropertyMock, return_value=mock_client)
    
    df = pd.DataFrame({"symbol": ["HPG"], "price": [28000.0]})
    db_mgr.ingest_df(df, table_name="prices", mode="replace")
    
    mock_client.command.assert_called_once_with("TRUNCATE TABLE prices")
    mock_client.insert_df.assert_called_once()

def test_database_manager_ingest_df_empty(mocker):
    mock_client = MagicMock()
    db_mgr = DatabaseManager()
    mocker.patch.object(DatabaseManager, 'client', new_callable=PropertyMock, return_value=mock_client)
    
    db_mgr.ingest_df(pd.DataFrame(), table_name="prices")
    mock_client.insert_df.assert_not_called()
