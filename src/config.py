from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field

# Project root directory for consistent cross-platform absolute path resolutions
PROJECT_ROOT = Path(__file__).resolve().parent.parent

class Settings(BaseSettings):
    # Database Settings (ClickHouse)
    db_host: str = Field("localhost", env="DB_HOST")
    db_port: int = Field(8123, env="DB_PORT")
    db_user: str = Field("finuser", env="DB_USER")
    db_password: str = Field("finpassword", env="DB_PASSWORD")
    db_name: str = Field("finance_db", env="DB_NAME")

    # Qdrant Vector DB Settings
    qdrant_host: str = Field("localhost", env="QDRANT_HOST")
    qdrant_port: int = Field(6333, env="QDRANT_PORT")
    qdrant_collection: str = "financial_reports"
    embedding_model_name: str = "all-MiniLM-L6-v2"
    rerank_model_name: str = "BAAI/bge-reranker-v2-m3"

    # Redis Cache & Checkpointer Settings
    redis_host: str = Field("localhost", env="REDIS_HOST")
    redis_port: int = Field(6379, env="REDIS_PORT")

    # Langfuse Telemetry Settings
    langfuse_public_key: str = Field("", env="LANGFUSE_PUBLIC_KEY")
    langfuse_secret_key: str = Field("", env="LANGFUSE_SECRET_KEY")
    langfuse_host: str = Field("http://localhost:3000", env="LANGFUSE_HOST")

    # Telegram Bot Settings
    telegram_bot_token: str | None = Field(None, env="TELEGRAM_BOT_TOKEN")

    # Kafka Streaming Settings
    kafka_broker: str = "localhost:9092"
    topic_market: str = "finagent_bronze_market"
    topic_news: str = "finagent_bronze_news"
    topic_market_dlq: str = "finagent_market_dlq"
    topic_news_dlq: str = "finagent_news_dlq"

    # LLM Model Settings (Sub-2B lightweight defaults)
    llm_coder_model: str = Field("qwen2.5-coder:1.5b-instruct", env="LLM_CODER_MODEL")
    llm_analyst_model: str = Field("qwen2.5:1.5b-instruct", env="LLM_ANALYST_MODEL")
    llm_guard_model: str = Field("llama-guard3:1b-q5_K_S", env="LLM_GUARD_MODEL")

    # Analyst Multi-Provider Settings (Local / Cloud Switcher)
    analyst_llm_provider: str = Field("local", env="ANALYST_LLM_PROVIDER")
    openai_api_key: str = Field("", env="OPENAI_API_KEY")
    gemini_api_key: str = Field("", env="GEMINI_API_KEY")
    deepseek_api_key: str = Field("", env="DEEPSEEK_API_KEY")
    groq_api_key: str = Field("", env="GROQ_API_KEY")
    analyst_api_model: str = Field("gpt-4o-mini", env="ANALYST_API_MODEL")
    analyst_api_base_url: str | None = Field(None, env="ANALYST_API_BASE_URL")

    # Semantic Router & Token Budget Settings
    max_history_tokens: int = Field(800, env="MAX_HISTORY_TOKENS")
    semantic_router_threshold: float = Field(0.55, env="SEMANTIC_ROUTER_THRESHOLD")

    # File Paths
    chart_file_path: str = str(PROJECT_ROOT / "data" / "exports" / "market_chart.png")

    class Config:
        env_file = str(PROJECT_ROOT / ".env")
        env_file_encoding = "utf-8"
        extra = "ignore"

# Auto-bootstrap runtime directories
(PROJECT_ROOT / "data" / "exports").mkdir(parents=True, exist_ok=True)

settings = Settings()
