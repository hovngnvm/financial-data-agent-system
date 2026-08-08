from typing import Annotated, Sequence, TypedDict
from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages

class AgentState(TypedDict):
    messages: Annotated[Sequence[AnyMessage], add_messages]
    security_status: str
    current_target: str
    chat: bool
        
    sql_data_output: list[dict[str, any]]
    rag_text_output: str
    chart_status_msg: str
    
    error_log: str
    retry_count: int
    
    next_worker: str
