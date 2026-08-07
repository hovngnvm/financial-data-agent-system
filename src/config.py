import os
from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    # Database Settings
    db_host: str = Field("localhost", env="DB_HOST")
    db_port: int = Field(8123, env="DB_PORT")
    db_user: str = Field("finuser", env="DB_USER")
    db_password: str = Field("finpassword", env="DB_PASSWORD")
    db_name: str = Field("finance_db", env="DB_NAME")

    # Qdrant Settings
    qdrant_host: str = Field("localhost", env="QDRANT_HOST")
    qdrant_port: int = Field(6333, env="QDRANT_PORT")
    qdrant_collection: str = "financial_reports"

    # Redis Settings
    redis_host: str = Field("localhost", env="REDIS_HOST")
    redis_port: int = Field(6379, env="REDIS_PORT")

    # Telegram Settings
    telegram_bot_token: str = Field(None, env="TELEGRAM_BOT_TOKEN")

    # Kafka Settings
    kafka_broker: str = "localhost:9092"
    topic_market: str = "finagent_bronze_market"
    topic_news: str = "finagent_bronze_news"
    topic_market_dlq: str = "finagent_market_dlq"
    topic_news_dlq: str = "finagent_news_dlq"

    # LLM Settings
    llm_coder_model: str = "qwen2.5-coder:3b-instruct-q5_K_S"
    llm_analyst_model: str = "qwen3.5:4b-q4_K_M"

    # File Paths
    chart_file_path: str = "data/exports/market_chart.png"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"

# Global settings instance initialization
settings = Settings()
