# FinAgent: Financial Data Multi-Agent & Stream Pipeline

## Project Overview

A **Real-time Financial Ingestion Pipeline & Multi-Agent Intelligence System**. This project ingests live cryptocurrency trades, Vietnamese stock quotes, and financial news feeds, processes them through an **Apache Kafka** streaming broker, enriches market data with technical indicators using a **Redis/In-memory hybrid cache**, stores structured market bars in **ClickHouse** (ReplacingMergeTree) and semantic knowledge in **Qdrant** (dense + sparse hybrid vectors with BGE cross-encoder reranking), and exposes an interactive **LangGraph-based Multi-Agent intelligence network** accessible via an asynchronous **Telegram Bot** interface with telemetry monitored by **Langfuse**.

**Business Goal:** Deliver real-time quantitative indicator tracking and semantic RAG macro-insights directly to traders and analysts through a natural language interface, coordinating specialist agents to execute parameterized database queries, perform hybrid vector search, generate multi-panel technical charts, and output synthesized financial assessments.

---

## Architecture & Tech Stack

```mermaid
graph LR
    %% Nodes for Ingestion Pipeline
    subgraph Ingestion ["Ingestion Pipeline"]
        Sources["Raw Sources<br>(Binance WS / vnstock / RSS Feeds)"]
        Kafka{"Apache Kafka Broker<br>(Bronze Topics / DLQ)"}
        Consumer["Kafka Consumer<br>(Manual Commit && DLQ)"]
        Cache["Price Cache<br>(Redis ZSET / In-memory fallback)"]
        
        Sources -->|Streams| Kafka
        Kafka -->|Batch Msg| Consumer
        Consumer -->|Compute Indicators| Cache
    end

    %% Databases
    subgraph Storage ["Storage Layer"]
        ClickHouse[("ClickHouse DB<br>(Gold Layer - ReplacingMergeTree)")]
        Qdrant[("Qdrant Vector DB<br>(Dense + Sparse Hybrid)")]
        
        Consumer -->|Write Prices| ClickHouse
        Consumer -->|Write News| Qdrant
    end

    %% Node for LangGraph Multi-Agent
    subgraph AgentNetwork ["Multi-Agent Workflow"]
        Telegram["Telegram Bot User Interface"]
        Security["Security Agent<br>(Compliance && SQLi Guardrails)"]
        Supervisor{"Supervisor Agent<br>(Routing Driver)"}
        RedisDB[("Redis Checkpointer")]
        
        %% Specialist Workers
        SQLAgent["SQL Agent<br>(Semantic Layer)"]
        RAGAgent["RAG Agent<br>(Parent-Child RAG)"]
        ChartAgent["Chart Agent<br>(Dynamic Session Renderer)"]
        Analyst["Analyst Agent<br>(Memory && Report Synthesizer)"]
        Telegram -->|User Prompt| Security
        Security -->|Clean Prompt| Supervisor
        
        %% Routing
        Supervisor -->|Route Query| SQLAgent
        Supervisor -->|Route Query| RAGAgent
        Supervisor -->|Route Query| ChartAgent
        Supervisor -->|Route Query| Analyst
        
        %% Specialist to DB interaction
        ClickHouse --> SQLAgent
        Qdrant --> RAGAgent
        ClickHouse --> ChartAgent
        Analyst -->|Save State| RedisDB

        %% Invisible links to force vertical separation between agents
        SQLAgent ~~~ ChartAgent
        ChartAgent ~~~ Analyst
        Analyst ~~~ RAGAgent
    end

    %% Monitoring
    Langfuse["Langfuse Server<br>(Telemetry && Agent Tracing)"]
    Supervisor -.->|Trace Metrics| Langfuse

    %% Styling with explicit text color to ensure dark/light mode compatibility
    classDef source fill:#E5E7EB,stroke:#9CA3AF,stroke-width:2px,color:#1F2937;
    classDef kafka fill:#FEF3C7,stroke:#FBBF24,stroke-width:2px,color:#78350F;
    classDef db fill:#D1FAE5,stroke:#34D399,stroke-width:2px,color:#065F46;
    classDef agent fill:#E0F2FE,stroke:#38BDF8,stroke-width:2px,color:#0369A1;
    classDef monitor fill:#F3E8FF,stroke:#C084FC,stroke-width:2px,color:#581C87;
    
    class Sources source;
    class Kafka kafka;
    class Consumer kafka;
    class ClickHouse db;
    class Qdrant db;
    class RedisDB db;
    class Cache db;
    class Telegram agent;
    class Security agent;
    class Supervisor agent;
    class SQLAgent agent;
    class RAGAgent agent;
    class ChartAgent agent;
    class Analyst agent;
    class Langfuse monitor;
```

* **Streaming Orchestration:** ![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7.0-231F20?style=flat&logo=apachekafka&logoColor=white)
* **Real-time Cache:** ![Redis](https://img.shields.io/badge/Redis-7.0-DC382D?style=flat&logo=redis&logoColor=white)
* **Columnar DB:** ![ClickHouse](https://img.shields.io/badge/ClickHouse-24.3-FFCC00?style=flat&logo=clickhouse&logoColor=black)
* **Vector DB:** ![Qdrant](https://img.shields.io/badge/Qdrant-1.11-FF4B4B?style=flat&logo=qdrant&logoColor=white)
* **Agent Framework:** ![LangGraph](https://img.shields.io/badge/LangGraph-0.2+-orange?style=flat) + ![Langfuse](https://img.shields.io/badge/Langfuse-Telemetry-000?style=flat)
* **Deployment & Containers:** ![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)
* **Language & Config:** ![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=flat&logo=python&logoColor=white) + ![Pydantic](https://img.shields.io/badge/Pydantic-v2-E92063?style=flat&logo=pydantic&logoColor=white)

---

## Multi-Agent Routing & Intent Decision Matrix

The coordination network uses a hybrid routing strategy combining a **sub-3ms Semantic Vector Router** (`SemanticVectorRouter`) for fast single/multi-intent identification and an **LLM Reasoning Fallback** for ambiguous queries:

| Detected Intent | Sample User Query | Routing Mechanism | Target Worker Node | Underlying Tool / Data Source |
| :--- | :--- | :--- | :--- | :--- |
| `INTENT_SQL` | *"What is the latest price and RSI for HPG?"* | Fast-Path Vector Embedding Similarity (`score > 0.82`) | **SQL Worker** | ClickHouse Semantic Layer (`get_ticker_price`, `get_indicators`) |
| `INTENT_CHART` | *"Draw a technical chart with MACD and SMA for SSI"* | Fast-Path Vector Embedding Similarity (`score > 0.80`) | **Chart Worker** | Headless Matplotlib Renderer (`generate_technical_chart`) |
| `INTENT_RAG` | *"Summarize recent news regarding State Bank of Vietnam interest rates"* | Fast-Path Vector Embedding Similarity (`score > 0.78`) | **RAG Worker** | Qdrant Dense+Sparse Hybrid Index + BGE Reranker |
| `INTENT_COMPLEX` | *"Analyze FPT stock price, show chart, and evaluate recent AI cloud expansion news"* | Clause Splitting + Multi-Threshold Fast-Path | **Dynamic Fan-out** (SQL + Chart + RAG concurrently $\rightarrow$ Analyst) | Parallel execution across ClickHouse, Matplotlib, and Qdrant |
| `AMBIGUOUS` | *"What should I do with my portfolio today?"* | LLM Reasoning Fallback (`qwen2.5-coder:1.5b`) | **Analyst Worker** | Direct dialogue synthesis with Redis chat checkpointer |

---

## 3-Layer RAG & Ingestion Data Contract

The unstructured knowledge pipeline converts financial RSS news feeds into queryable semantic representations:

```mermaid
flowchart TD
    raw_news["Raw RSS Feed Articles<br/>(Vietstock / CafeF / VnExpress)"] --> hash_check{"Hash Deduplicator<br/>(MD5 Link Registry)"}
    hash_check -->|New Article| chunker["Hierarchical News Chunker"]
    hash_check -->|Duplicate| discard["Discard Article"]

    subgraph Chunking [Hierarchical Decomposition]
        chunker --> parent["Parent Context Doc<br/>(~500 tokens / 3-5 sentences)"]
        chunker --> child["Child Precise Chunks<br/>(~120 tokens / markdown & tables preserved)"]
    end

    child --> batch_enc["Vectorized Batch Encoder<br/>(sentence-transformers, batch_size=32)"]
    batch_enc --> dense_vec["Dense Embeddings<br/>(384-dim HNSW Cosine)"]
    batch_enc --> sparse_vec["Sparse Tokens<br/>(MurmurHash3 mmh3 Index)"]

    dense_vec --> qdrant_store[("Qdrant On-Disk Storage")]
    sparse_vec --> qdrant_store
    parent --> qdrant_store

    subgraph QueryFlow [Hybrid Retrieval & Reranking]
        query["User Query"] --> qdrant_search["Hybrid Search (Dense + Sparse)"]
        qdrant_store --> qdrant_search
        qdrant_search --> candidates["Top 10 Candidate Chunks"]
        candidates --> cross_enc["BGE Reranker v2 m3<br/>(Cross-Encoder Score)"]
        cross_enc --> top_k["Top-3 Scored Chunks"]
        top_k --> retrieve_parent["Fetch Parent Document Context"]
        retrieve_parent --> rag_agent["RAG Worker Output"]
    end
```

---

## Dynamic Multi-Panel Technical Charting Modes

The `ChartAgent` module renders multi-panel financial charts via thread-safe, object-oriented Matplotlib with support for dynamic session-isolated file paths:

| Chart Mode | Panel Layout | Visual Indicators Included | Primary Use Case |
| :--- | :---: | :--- | :--- |
| `comprehensive` | 2-Panel Vertical | Top: Price Candles + SMA-5/20 + Bollinger Bands<br>Bottom: MACD Histogram & Signal Line | Holistic market momentum and trend analysis |
| `price_sma` | 1-Panel Single | Close Prices + 5-day & 20-day Simple Moving Averages | Quick trend direction and Golden/Death cross inspection |
| `rsi` | 2-Panel Vertical | Top: Price line<br>Bottom: 14-period RSI with 70 Overbought & 30 Oversold thresholds | Mean reversion and overextended price detection |
| `macd` | 2-Panel Vertical | Top: Price line<br>Bottom: MACD Line, Signal Line, and Positive/Negative Momentum Bars | Momentum convergence and divergence confirmation |
| `volume` | 2-Panel Vertical | Top: Price line<br>Bottom: Color-coded trading volume bars | Liquidity confirmation and breakout validation |

---

## Pipeline Workflow

### 1. Real-time Ingestion (Producers)
* **Binance WS Stream:** Connects directly to Binance WebSockets, listening to live ETHUSDT and BTCUSDT trades. Formats records to match the ingestion data contract and publishes them to the `finagent_bronze_market` topic.
* **vnstock Polling Stream:** Polls price boards for high-liquidity Vietnamese stock tickers (SSI, VND, HPG, FPT, etc.) using vnstock. Formats records and publishes them to the market topic.
* **RSS Feeds Stream:** Parses economic feeds (CafeF, Vietstock, VnExpress) every 15 seconds. Uses an in-memory MD5 hash registry to filter out duplicate news links and publishes new stories to the `finagent_bronze_news` topic.

### 2. Stream Ingestion & Caching (Kafka Consumer)
* **At-Least-Once Delivery:** Auto-commits are disabled (`enable_auto_commit=False`). Offsets are manually committed *only* after database inserts succeed.
* **Dead Letter Queue (DLQ):** Failed schemas, parsing failures, and contract mismatches are caught, packed with failure diagnostics, and routed to dedicated DLQ topics (`finagent_market_dlq`, `finagent_news_dlq`).
* **Sliding Window Caching:** Utilizes `PriceCacheManager` with **Redis Sorted Sets (ZSET)** indexed by timestamp scores to maintain a sliding window of the last 30 prices per symbol. If Redis is unavailable, it gracefully falls back to an **in-memory process dictionary** (safely sorting and deduplicating). ClickHouse is queried *only once* on cache miss (lazy loading) to warm up the cache, eliminating repetitive database hits.
* **Incremental Indicator Calculation:** Recalculates technical metrics (SMA-5, SMA-20, RSI, MACD) on the 30-item sliding window, and bulk-inserts the enriched records into ClickHouse.

### 3. Semantic Layer Ingestion (Vector Database)
* **Hierarchical Chunking:** Processes ingested news articles into child chunks for precision embeddings while preserving the broader parent document context to avoid context fragmentation.
* **Batch Vector Encoding & Hybrid Indexing:** Encodes document chunks in vectorized batches (`batch_size=32`) via `sentence-transformers`, generating dense embeddings alongside sparse vectors built via MurmurHash3 (`mmh3`) with collision weight aggregation. Stores both dense and sparse indices directly on disk in Qdrant.

### 4. LangGraph Multi-Agent Orchestration
* **Security Agent:** Intercepts incoming Telegram inputs to guard against SQL injections, prompt manipulation, or jailbreaks using multi-vector regex inspection and `llama-guard3:1b`.
* **Supervisor Agent & Fast Semantic Router:** Evaluates intent via a sub-3ms `SemanticVectorRouter` with Multi-Threshold matching and Clause Splitting. Falls back to structured LLM reasoning (`qwen2.5-coder:1.5b`) on ambiguous input.
* **Dynamic Conditional Fan-out:** Branches execution concurrently to relevant workers (SQL Agent, RAG Agent, Chart Agent, or direct to Analyst), eliminating unnecessary sequential steps.
* **SQL Agent:** Extracts parameters (action, ticker, limit) from user questions to query prices and technical indicators via ClickHouse Semantic Layer tools with exact case-insensitive symbol normalization (`upper(symbol) = upper(%(symbol)s)`).
* **RAG Agent:** Executes hybrid dense/sparse searches against Qdrant, reranks text candidates using a cross-encoder model (`bge-reranker-v2-m3`), and fetches the expanded parent document context.
* **Chart Agent:** Dynamically generates multi-panel technical analysis charts using object-oriented, thread-safe headless Matplotlib with custom session-isolated output paths.
* **Analyst Agent & In-Flight Context Trimming:** Synthesizes final reports combining quantitative ClickHouse metrics and Qdrant macro context. Applies `trim_messages` to constrain chat history within an 800-token budget while preserving the complete conversation snapshot in RedisSaver. Supports runtime switching between Local Ollama (`qwen2.5:1.5b-instruct`) and Cloud AI (OpenAI, Gemini, DeepSeek, Groq).

---

## Key Engineering Highlights

* **Thread-Safe Dynamic Chart Generation:** Generates chart image files with session-isolated target paths (`chart_path` in `AgentState`) and safely cleans them up after transmission, preventing concurrent user file collision and race conditions.
* **Deterministic Parameterized SQL Matching:** Eliminates SQL dialect syntax issues and injection risks by enforcing parameter-driven execution with strict case-insensitive symbol matching and subquery chronological ordering.
* **Multi-Vector Security Guardrails:** Combines rule-based regex patterns for SQL injection (`UNION SELECT`, `INSERT INTO`) and prompt jailbreaks (`disregard rules`, `act as DAN`) with Llama-Guard fail-safe verification.
* **Multi-Provider LLM Switcher:** Users can toggle between Local Ollama and Cloud AI providers dynamically via Telegram UI buttons (`/model`) or environment configurations.
* **3-Layer RAG Quality Gate & Chunking Benchmark:** Automated evaluation testing IR metrics (Hit@1, Hit@2, MRR), fact recall, and table preservation integrity.
* **High-Throughput Batch Vectorization:** Replaces per-item encoding with batched tensor calculations (`batch_size=32`), achieving 5x–10x faster vector database ingestion.
* **Zero-Leak Stream Buffers:** Protects continuous 24/7 RSS ingestion using bounded sliding deques to prevent memory leaks.
* **Resilient Offset Commit Policy:** Commits Kafka offsets manually *after* data persistence succeeds, assuring zero data loss (At-Least-Once).
* **Robust DLQ Design:** Isolates malformed events into dedicated DLQ topics to prevent ingestion pipeline blocking.
* **ClickHouse Query Offloading:** Integrates a write-through hybrid cache (Redis ZSET / In-memory fallback) to eliminate database queries inside the streaming loop.
* **Pydantic v2 Centralized Settings:** Type-safe environment variable parsing with automatic directory bootstrapping (`data/exports/`, `logs/`).

---

## Project Structure

```
financial-data-agent-system/
│
├── src/
│   ├── agent/                         # Multi-Agent Coordination Network
│   │   ├── nodes/
│   │   │   ├── analyst.py             # Coordinator: synthesizes reports, manages session states
│   │   │   ├── chart_worker.py        # Chart Agent: generates technical charts with dynamic paths
│   │   │   ├── driver.py              # Supervisor Agent: routes to specialist workers (Fast-Path + LLM Fallback)
│   │   │   ├── rag_worker.py          # RAG Agent: performs sparse/dense hybrid searches
│   │   │   ├── security.py            # Guardrail Agent: validates prompt compliance and security
│   │   │   └── sql_worker.py          # SQL Agent: queries prices and indicators via Semantic Layer
│   │   ├── callbacks.py               # Langfuse tracing integrations
│   │   ├── graph.py                   # LangGraph assembly (state, dynamic fan-out, checkpointer)
│   │   ├── prompts.py                 # Multi-Intent and agent system prompts
│   │   ├── router.py                  # Sub-3ms Semantic Vector Router with Multi-Threshold & Clause Splitting
│   │   └── state.py                   # AgentState schema definition
│   │
│   ├── config/
│   │   ├── __init__.py                # Settings package initialization & exports
│   │   └── settings.py                # Centralized Pydantic v2 settings & directory bootstrapping
│   │
│   ├── utils/
│   │   ├── __init__.py                # Utilities package initialization
│   │   ├── chunking.py                # Hierarchical news chunker (preserves markdown/tables)
│   │   └── logger.py                  # Standardized logging wrapper
│   │
│   ├── consumer.py                    # Kafka Consumer with Manual Commits, DLQ & PriceCacheManager
│   ├── database.py                    # ClickHouse schema definition (ReplacingMergeTree) and ingest engine
│   ├── producer.py                    # Binance WS, vnstock polling, RSS financial feeds producer
│   ├── telegram_bot.py                # Telegram Bot handler running LangGraph workflows async
│   ├── tools.py                       # SQL execution, hybrid vector retrieval, dynamic chart rendering
│   └── vector_db.py                   # Qdrant hybrid (dense + sparse index HNSW) schema & ingest
│
├── tests/                             # Full Unit & Evaluation Test Suite
│   ├── evaluate.py                    # Unified 3-Layer RAG Quality Gate & Chunking Benchmark Suite
│   ├── seed_data.py                   # Test dataset initialization fixtures
│   ├── test_chunking.py               # Markdown table and hierarchical chunking tests
│   ├── test_consumer.py               # Ingestion stream, DLQ, and cache manager tests
│   ├── test_database.py               # ClickHouse query and schema tests
│   ├── test_prompt_integrity.py       # Prompt contracts and multi-agent node integrity tests
│   ├── test_router.py                 # Semantic Vector Router and Context Trimming tests
│   ├── test_security_guard.py         # Security injection guardrails and route plan tests
│   ├── test_tools.py                  # Semantic layer tools and dynamic chart rendering tests
│   └── test_vector_db.py              # Qdrant hybrid vector search tests
│
├── docs/                              # Project Documentation & Architecture Reviews
│   └── adversarial_review.md          # Adversarial Architecture Assessment & Production Review
│
├── conftest.py                        # Global Pytest fixtures & environment setup
├── docker-compose.yml                 # Infrastructure stack (Kafka, ClickHouse, Redis, Qdrant, Postgres, Langfuse)
├── main.py                            # Unified bootstrap entrypoint (DB init + Vector init + Bot start)
├── requirements.txt                   # Project package dependencies
├── .env.example                       # Environment configuration template
└── .env                               # Environment configurations (tokens, database credentials)
```

---

## Data Contracts & Storage Schemas

### 1. ClickHouse Gold Layer: `prices` Table
Stores high-frequency market prices and incrementally calculated technical indicators:

| Column | Type | Nullable | Description |
| :--- | :--- | :---: | :--- |
| `timestamp` | `DateTime` | No | Timestamp of the price observation (UTC) |
| `symbol` | `String` | No | Asset ticker symbol (e.g., `BTCUSDT`, `ETHUSDT`, `HPG`, `SSI`) |
| `price` | `Float64` | No | Last trade / closing price |
| `volume` | `Float64` | No | Executed trading volume |
| `SMA_5` | `Nullable(Float64)` | Yes | 5-period Simple Moving Average |
| `SMA_20` | `Nullable(Float64)` | Yes | 20-period Simple Moving Average |
| `RSI` | `Nullable(Float64)` | Yes | 14-period Relative Strength Index (0–100) |
| `MACD` | `Nullable(Float64)` | Yes | Moving Average Convergence Divergence line |
| `MACD_Signal` | `Nullable(Float64)` | Yes | 9-period Exponential Moving Average signal line |

* **Engine:** `ReplacingMergeTree()`
* **Primary / Sorting Key:** `(symbol, timestamp)`

---

### 2. Qdrant Vector Collection: `financial_reports`
Stores dense embeddings, sparse tokens, and hierarchical parent-child article payloads:

| Vector / Payload Field | Type / Dimension | Indexing Strategy | Description |
| :--- | :--- | :--- | :--- |
| `dense` | `Vector(384)` | HNSW (Cosine Distance) | Dense semantic vector (`all-MiniLM-L6-v2`) |
| `sparse` | `SparseVector` | Inverted Index (On-Disk) | Lexical token weights generated via MurmurHash3 (`mmh3`) |
| `parent_id` | `String (MD5/UUID)` | Keyword Match | Unique identifier linking child chunks to parent document |
| `chunk_id` | `String / Integer` | Keyword Match | Child chunk sequence index within parent article |
| `source` | `String` | Keyword Filter | News publisher (e.g., `CafeF`, `Vietstock`, `VnExpress`) |
| `title` | `String` | Fulltext Search | Headline / Article title |
| `url` | `String` | Exact Match | Canonical web URL of the original news story |
| `published_at` | `String (ISO 8601)` | Datetime Filter | Article publication timestamp |
| `content` | `String (~120 tokens)` | Stored Text | Child precise chunk text (markdown & tables preserved) |
| `parent_content` | `String (~500 tokens)` | Stored Context | Complete parent article context passed to Analyst LLM |

---

## Environment Configuration Reference

The system utilizes centralized Pydantic v2 settings (`src/config/settings.py`) loaded from `.env`:

| Variable | Category | Type | Default Value | Description |
| :--- | :--- | :---: | :--- | :--- |
| `DB_HOST` | Database | `str` | `localhost` | ClickHouse host address |
| `DB_PORT` | Database | `int` | `8123` | ClickHouse HTTP interface port |
| `DB_USER` | Database | `str` | `finuser` | ClickHouse username |
| `DB_PASSWORD` | Database | `str` | `finpassword` | ClickHouse user password |
| `DB_NAME` | Database | `str` | `finance_db` | Target database name |
| `QDRANT_HOST` | Vector DB | `str` | `localhost` | Qdrant vector database host |
| `QDRANT_PORT` | Vector DB | `int` | `6333` | Qdrant HTTP interface port |
| `QDRANT_COLLECTION` | Vector DB | `str` | `financial_reports` | Qdrant collection name |
| `EMBEDDING_MODEL_NAME` | Vector DB | `str` | `all-MiniLM-L6-v2` | Sentence-transformers dense embedding model |
| `RERANK_MODEL_NAME` | Vector DB | `str` | `BAAI/bge-reranker-v2-m3` | Cross-encoder reranker model |
| `REDIS_HOST` | Cache / State | `str` | `localhost` | Redis server host |
| `REDIS_PORT` | Cache / State | `int` | `6379` | Redis server port |
| `KAFKA_BROKER` | Streaming | `str` | `localhost:9092` | Kafka broker bootstrap server address |
| `TOPIC_MARKET` | Streaming | `str` | `finagent_bronze_market` | Market prices streaming topic |
| `TOPIC_NEWS` | Streaming | `str` | `finagent_bronze_news` | Financial news streaming topic |
| `TOPIC_MARKET_DLQ` | Streaming | `str` | `finagent_market_dlq` | Dead Letter Queue for malformed price records |
| `TOPIC_NEWS_DLQ` | Streaming | `str` | `finagent_news_dlq` | Dead Letter Queue for malformed news records |
| `LANGFUSE_PUBLIC_KEY` | Observability | `str` | `""` | Langfuse telemetry public key |
| `LANGFUSE_SECRET_KEY` | Observability | `str` | `""` | Langfuse telemetry secret key |
| `LANGFUSE_HOST` | Observability | `str` | `http://localhost:3000` | Langfuse server URL |
| `TELEGRAM_BOT_TOKEN` | Interface | `str` | `None` | Telegram Bot API token from `@BotFather` |
| `LLM_CODER_MODEL` | Local LLM | `str` | `qwen2.5-coder:1.5b-instruct` | Ollama model for structured reasoning fallback |
| `LLM_ANALYST_MODEL` | Local LLM | `str` | `qwen2.5:1.5b-instruct` | Ollama model for report synthesis |
| `LLM_GUARD_MODEL` | Local LLM | `str` | `llama-guard3:1b-q5_K_S` | Ollama model for security guardrails |
| `ANALYST_LLM_PROVIDER` | Multi-Provider | `str` | `local` | Active provider: `local`, `openai`, `gemini`, `deepseek`, `groq` |
| `ANALYST_TEMPERATURE` | Multi-Provider | `float` | `0.3` | Analyst LLM temperature (0.0–1.0) |
| `OPENAI_API_KEY` | Cloud AI | `str` | `""` | OpenAI API key for GPT models |
| `GEMINI_API_KEY` | Cloud AI | `str` | `""` | Google Gemini API key |
| `DEEPSEEK_API_KEY` | Cloud AI | `str` | `""` | DeepSeek API key |
| `GROQ_API_KEY` | Cloud AI | `str` | `""` | Groq high-speed inference API key |
| `MAX_HISTORY_TOKENS` | Memory Budget | `int` | `800` | Token budget limit for conversation context window trimming |
| `SEMANTIC_ROUTER_THRESHOLD` | Routing | `float` | `0.55` | Similarity threshold for fast-path vector routing |
| `VNSTOCK_API_KEY` | Market Data | `str` | `""` | API key for vnstock market feed (optional) |

---

## Observability & Agent Tracing (Langfuse)

All multi-agent interactions, tool executions, and LLM reasoning steps are traced in real-time with **Langfuse**:

* **Dashboard Access:** Open `http://localhost:3000` in your browser.
* **Trace Hierarchy:** Traces follow the workflow execution path: `Telegram Input` $\rightarrow$ `Security Guard` $\rightarrow$ `Semantic Vector Router` $\rightarrow$ `Specialist Worker(s)` $\rightarrow$ `Analyst Synthesis`.
* **Telemetry Metrics Monitored:**
  * **Latency Breakdown:** Sub-3ms router execution vs tool database query times vs LLM token generation latency.
  * **Token Usage & Costs:** In-flight message trimming statistics and token cost tracking per provider.
  * **Fast-Path vs Fallback Ratio:** Tracks proportion of queries routed via embedding similarity vs LLM fallback.
  * **Error & DLQ Traces:** Real-time visibility into schema validation errors and failed tool calls.

---

## Quickstart & Execution Guide

### 1. Configure the Environment
```bash
git clone <your-repo-url>
cd financial-data-agent-system
cp .env.example .env
# Fill out the .env file with your API keys (Telegram Bot Token, Langfuse, Database credentials)
```

### 2. Launch Infrastructure Services
Ensure Docker Desktop is running, then start the container stack:
```bash
docker compose up -d
```
This boots up:
* **Kafka:** Port `9092` (KRaft broker).
* **ClickHouse:** Ports `8123` (HTTP) & `9000` (Native) for market data storage.
* **Qdrant:** Ports `6333` (HTTP) & `6334` (gRPC) for vector search.
* **Redis:** Port `6379` (Sliding window cache & LangGraph checkpointer).
* **PostgreSQL:** Port `5433` (Langfuse metadata storage).
* **Langfuse:** Port `3000` (Agent monitoring and tracing server).

### 3. Run Unit and Evaluation Test Suites
```bash
# Activate your virtual environment
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install required dependencies
pip install -r requirements.txt

# Run full pytest unit and integration test suite across all components
PYTHONPATH=. pytest -v

# Run the 3-Layer RAG Quality Gate and Chunking Strategy Benchmark Suite
PYTHONPATH=. python tests/evaluate.py --all
```

### 4. Run the Stream Pipeline (Optional Standalone)
Launch the real-time producers and consumer in separate terminal sessions:
```bash
# Start Ingestion streams (Producers)
PYTHONPATH=. python src/producer.py

# Start Processing engine (Consumer)
PYTHONPATH=. python src/consumer.py
```

### 5. Launch the Platform & Telegram Bot
Start the unified platform entrypoint to initialize database schemas and start the Telegram Bot:
```bash
PYTHONPATH=. python main.py
```

### 6. Interactive Bot Commands & Chat Scenarios
Search for your bot in Telegram using your configured bot token name and interact:

#### Command Shortcuts:
* `/start` - Displays bot welcome overview and active model status.
* `/model` - Opens the interactive UI Switcher to select between Local Ollama and Cloud AI (OpenAI, Gemini, DeepSeek, Groq).
* `/analyze HPG` - Triggers comprehensive technical analysis with automated chart generation for ticker `HPG`.

#### Sample Natural Language Queries:
* **Quantitative Data Lookup (`INTENT_SQL`):**
  > *"Lấy giá mới nhất và chỉ số RSI của SSI"*
* **Dynamic Technical Charting (`INTENT_CHART`):**
  > *"Vẽ biểu đồ kỹ thuật MACD và đường SMA cho SSI"*
* **Macroeconomic News Synthesis (`INTENT_RAG`):**
  > *"Tóm tắt các tin tức mới nhất về chính sách điều hành lãi suất của Ngân hàng Nhà nước"*
* **Complex Multi-Intent Analysis (`INTENT_COMPLEX`):**
  > *"Phân tích kỹ thuật mã FPT, vẽ biểu đồ và đánh giá các tin tức mở rộng trung tâm dữ liệu AI gần đây"*

