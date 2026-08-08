import os
import pandas as pd
import clickhouse_connect
from src.config import settings
from src.logger import get_logger

logger = get_logger(__name__)

class DatabaseManager:
    def __init__(self):
        self.host = settings.db_host
        self.port = settings.db_port
        self.user = settings.db_user
        self.password = settings.db_password
        self.database = settings.db_name
        self._client = None

    @property
    def client(self):
        """Lazy-loaded ClickHouse Connect Client"""
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                database=self.database
            )
        return self._client

    def init_db(self):
        """Initializes database and tables in ClickHouse using ReplacingMergeTree"""
        temp_client = None
        try:
            # Connect without database first to ensure db exists
            temp_client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password
            )
            temp_client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        except Exception as e:
            logger.warning(f"Failed to pre-create database {self.database}: {e}")
        finally:
            if temp_client:
                temp_client.close()

        # Connect to target database and create table with ReplacingMergeTree
        client = self.client
        client.command("""
            CREATE TABLE IF NOT EXISTS prices (
                timestamp DateTime,
                symbol LowCardinality(String),
                price Float64,
                volume Float64,
                SMA_5 Float64,
                SMA_20 Float64,
                RSI Float64,
                MACD Float64,
                MACD_Signal Float64
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (symbol, timestamp);
        """)
        logger.info(f"-> [ClickHouse]: Database schema successfully initialized at {self.host}:{self.port}")

    def ingest_df(self, dataframe, table_name="prices", mode="append"):
        """
        Ingests a Pandas DataFrame into the ClickHouse database.
        """
        if dataframe is None or dataframe.empty:
            return
        dataframe = dataframe.copy()
        dataframe.columns = dataframe.columns.str.lower()

        # Format timestamp to correct timezone-naive datetime
        if 'timestamp' in dataframe.columns:
            dataframe['timestamp'] = pd.to_datetime(dataframe['timestamp'])
        
        # ClickHouse truncation before append if mode is 'replace'
        if mode == "replace":
            self.client.command(f"TRUNCATE TABLE {table_name}")
        
        # Align column names to exact casing in ClickHouse schema
        column_mapping = {
            'timestamp': 'timestamp',
            'symbol': 'symbol',
            'price': 'price',
            'volume': 'volume',
            'sma_5': 'SMA_5',
            'sma_20': 'SMA_20',
            'rsi': 'RSI',
            'macd': 'MACD',
            'macd_signal': 'MACD_Signal'
        }
        dataframe = dataframe.rename(columns={k: v for k, v in column_mapping.items() if k in dataframe.columns})
        
        # Write to ClickHouse
        self.client.insert_df(table_name, dataframe)
        logger.info(f"-> [ClickHouse]: Ingested {len(dataframe)} rows into table '{table_name}'")

# Initialize global DatabaseManager instance for shared workspace usage
db_manager = DatabaseManager()

# Backward-compatibility wrappers
def init_relational_database():
    db_manager.init_db()

def ingest_data_to_db(dataframe, table_name="prices", mode="append"):
    db_manager.ingest_df(dataframe, table_name=table_name, mode=mode)