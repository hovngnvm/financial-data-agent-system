from collections.abc import Sequence
from typing import Annotated, Any, Literal, TypedDict
from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages

Intent = Literal["FETCH_PRICE", "FETCH_INDICATOR", "FETCH_NEWS", "RENDER_CHART", "CHITCHAT"]
ChartMode = Literal["comprehensive", "price_sma", "rsi", "macd", "volume"]
SecurityStatus = Literal["SAFE", "MALICIOUS"]
NextWorker = Literal[
    "START", "CONTINUE", "PARALLEL_EXECUTE", "CHART_WORKER", "FINAL_ANALYST", "PURGE", "FINISH"
]
AnalystProvider = Literal["local", "openai", "gemini", "deepseek", "groq"]

class AgentState(TypedDict):
    messages: Annotated[Sequence[AnyMessage], add_messages]
    security_status: SecurityStatus
    current_target: str
    chat: bool
        
    sql_data_output: list[dict[str, Any]]
    rag_text_output: str
    chart_status_msg: str
    chart_file_path: str | None
    
    analyst_provider: AnalystProvider
    
    activated_intents: list[Intent]
    chart_mode: ChartMode | None
    
    next_worker: NextWorker
