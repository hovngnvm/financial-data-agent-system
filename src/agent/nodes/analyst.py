from typing import Any
import os
import json
from langchain_core.messages import AIMessage, trim_messages
from langchain_ollama import OllamaLLM
from src.agent.state import AgentState
from src.config import settings
from src.utils.logger import get_logger
from src.agent.prompts import ANALYST_CHITCHAT_PROMPT, ANALYST_INVESTMENT_PROMPT, ANALYST_MACRO_NEWS_PROMPT

logger = get_logger(__name__)

def get_analyst_llm(provider: str | None = None) -> Any:
    """
    Factory function returning the configured LLM instance.
    Supports Local Ollama and Cloud APIs (OpenAI, Gemini, DeepSeek, Groq) with automatic fallback.
    """
    active_provider = (provider or settings.analyst_llm_provider or "local").lower().strip()
    
    if active_provider == "local":
        return OllamaLLM(model=settings.llm_analyst_model, temperature=0.3)
        
    try:
        if active_provider == "openai":
            from langchain_openai import ChatOpenAI
            api_key = settings.openai_api_key or os.getenv("OPENAI_API_KEY")
            return ChatOpenAI(
                model=settings.analyst_api_model or "gpt-4o-mini",
                api_key=api_key,
                base_url=settings.analyst_api_base_url,
                temperature=0.3
            )
        elif active_provider == "gemini":
            try:
                from langchain_google_genai import ChatGoogleGenerativeAI
                api_key = settings.gemini_api_key or os.getenv("GEMINI_API_KEY")
                model_name = settings.analyst_api_model if "gemini" in settings.analyst_api_model else "gemini-1.5-flash"
                return ChatGoogleGenerativeAI(
                    model=model_name,
                    google_api_key=api_key,
                    temperature=0.3
                )
            except ImportError:
                from langchain_openai import ChatOpenAI
                return ChatOpenAI(
                    model="gemini-1.5-flash",
                    api_key=settings.gemini_api_key or os.getenv("GEMINI_API_KEY"),
                    base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
                    temperature=0.3
                )
        elif active_provider == "deepseek":
            from langchain_openai import ChatOpenAI
            api_key = settings.deepseek_api_key or os.getenv("DEEPSEEK_API_KEY")
            base_url = settings.analyst_api_base_url or "https://api.deepseek.com"
            return ChatOpenAI(
                model="deepseek-chat",
                api_key=api_key,
                base_url=base_url,
                temperature=0.3
            )
        elif active_provider == "groq":
            from langchain_openai import ChatOpenAI
            api_key = settings.groq_api_key or os.getenv("GROQ_API_KEY")
            base_url = settings.analyst_api_base_url or "https://api.groq.com/openai/v1"
            return ChatOpenAI(
                model="llama-3.3-70b-versatile",
                api_key=api_key,
                base_url=base_url,
                temperature=0.3
            )
        else:
            logger.warning(f"Unknown provider '{active_provider}'. Defaulting to Local Ollama.")
            return OllamaLLM(model=settings.llm_analyst_model, temperature=0.3)
    except Exception as e:
        logger.error(f"Failed to initialize LLM provider '{active_provider}': {e}. Falling back to Local Ollama.")
        return OllamaLLM(model=settings.llm_analyst_model, temperature=0.3)

async def node_final_analyst(state: AgentState) -> dict[str, Any]:
    """Final Synthesis Agent: Analyzes quantitative and qualitative outputs (Async)"""
    selected_provider = state.get("analyst_provider", settings.analyst_llm_provider)
    llm_analyst = get_analyst_llm(selected_provider)
    
    raw_messages = list(state.get("messages", []))
    user_msg = raw_messages[-1].content if raw_messages else ""
    
    # In-Flight Context Trimming: Constrain chat history to configured token budget
    trimmed_messages = trim_messages(
        raw_messages,
        max_tokens=settings.max_history_tokens,
        strategy="last",
        token_counter=lambda msgs: sum(len(str(m.content)) for m in msgs) // 4,
        include_system=False,
        allow_partial=False,
        start_on="human"
    )
    
    history_transcript = "\n".join(
        f"{'User' if m.type == 'human' else 'Assistant'}: {m.content}"
        for m in trimmed_messages
    ) if trimmed_messages else "No prior conversation history."
        
    if state.get("chat", False):
        system_prompt = ANALYST_CHITCHAT_PROMPT
        prompt_payload = f"=== USER QUESTION ===\n{user_msg}"
    else:
        target = state.get("current_target", "UNKNOWN")
        sql_numbers = state.get("sql_data_output", [])
        rag_news = state.get("rag_text_output", "No related macro news found.")
        chart_info = state.get("chart_status_msg", "No chart generated.")

        if target != "UNKNOWN":
            # Scenario 1: Specific Asset Deep-Dive (Quantitative + Indicators + Chart + Asset News)
            system_prompt = ANALYST_INVESTMENT_PROMPT
            prompt_payload = (
                f"=== USER REQUEST ===\n{user_msg}\n\n"
                f"TARGET ASSET: {target}\n\n"
                "=== QUANTITATIVE HISTORICAL DATA (ClickHouse) ===\n"
                f"{json.dumps(sql_numbers, indent=2) if sql_numbers else 'No time-series data available.'}\n\n"
                "=== QUALITATIVE NEWS CONTEXT (Qdrant RAG) ===\n"
                f"{rag_news}\n\n"
                "=== VISUALIZATION STATUS ===\n"
                f"The technical analysis chart ({chart_info}) has already been generated by the backend and attached to the user UI. Focus your text analysis purely on interpreting the numbers and indicators above.\n\n"
                "=== CHAT HISTORY (TRIMMED CONTEXT) ===\n"
                f"{history_transcript}"
            )
        else:
            # Scenario 2: Macroeconomic & Market-Wide News Roundup (Qualitative RAG Only)
            system_prompt = ANALYST_MACRO_NEWS_PROMPT
            prompt_payload = (
                f"=== USER REQUEST ===\n{user_msg}\n\n"
                "=== QUALITATIVE NEWS CONTEXT (Qdrant RAG) ===\n"
                f"{rag_news}\n\n"
                "=== CHAT HISTORY (TRIMMED CONTEXT) ===\n"
                f"{history_transcript}"
            )
    
    from src.agent.callbacks import get_langfuse_handler
    handler = get_langfuse_handler()
    call_config = {"callbacks": [handler]} if handler else {}
    
    try:
        raw_response = await llm_analyst.ainvoke(
            f"{system_prompt}\n\n{prompt_payload}",
            config=call_config
        )
        final_response = raw_response.content if hasattr(raw_response, "content") else str(raw_response)
    except Exception as e:
        logger.error(f"Error during LLM inference with provider '{selected_provider}': {e}. Retrying with local Ollama.")
        fallback_llm = OllamaLLM(model=settings.llm_analyst_model, temperature=0.3)
        raw_response = await fallback_llm.ainvoke(
            f"{system_prompt}\n\n{prompt_payload}",
            config=call_config
        )
        final_response = raw_response.content if hasattr(raw_response, "content") else str(raw_response)

    return {"messages": [AIMessage(content=final_response)], "next_worker": "PURGE"}

async def node_purge_state(state: AgentState) -> dict[str, Any]:
    """State Purger: Resets transient execution errors while preserving conversation context across turns"""
    return {
        "security_status": "SAFE",
        "chat": False,
        "error_log": "",
        "retry_count": 0,
        "next_worker": "FINISH"
    }
