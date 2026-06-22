import pandas as pd
import clickhouse_connect
from src.config import settings
from src.utils.logger import get_logger

COLUMN_MAPPING: dict[str, str] = {
    'sma_5': 'SMA_5',
    'sma_20': 'SMA_20',
    'rsi': 'RSI',
    'macd': 'MACD',
    'macd_signal': 'MACD_Signal'
}

logger = get_logger(__name__)

class DatabaseManager:
    """Manages ClickHouse database connections, table migrations, and bulk DataFrame ingestion."""

    def __init__(self):
        self.host = settings.db_host
        self.port = settings.db_port
        self.user = settings.db_user
        self.password = settings.db_password
        self.database = settings.db_name
        self._client = None

    @property
    def client(self):
        """Lazy-loaded ClickHouse Connect Client."""
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                database=self.database
            )
        return self._client

    def init_db(self) -> None:
        """Initializes database and tables in ClickHouse using ReplacingMergeTree."""
        try:
            with clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password
            ) as temp_client:
                temp_client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        except Exception as e:
            logger.warning(f"Failed to pre-create database {self.database}: {e}")

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
        logger.info(f"ClickHouse database schema successfully initialized at {self.host}:{self.port}")

    def ingest_df(self, dataframe: pd.DataFrame, table_name: str = "prices", mode: str = "append") -> None:
        """Ingests a Pandas DataFrame into the ClickHouse database."""
        if dataframe is None or dataframe.empty:
            return
        dataframe = dataframe.copy()
        dataframe.columns = dataframe.columns.str.lower()

        if 'timestamp' in dataframe.columns:
            dataframe['timestamp'] = pd.to_datetime(dataframe['timestamp'])
        
        if mode == "replace":
            self.client.command(f"TRUNCATE TABLE {table_name}")
        
        dataframe = dataframe.rename(columns={k: v for k, v in COLUMN_MAPPING.items() if k in dataframe.columns})
        self.client.insert_df(table_name, dataframe)
        logger.info(f"Ingested {len(dataframe)} rows into ClickHouse table '{table_name}'")

db_manager = DatabaseManager()