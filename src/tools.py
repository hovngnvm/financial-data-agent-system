import os
import json
import pandas as pd
import matplotlib.pyplot as plt
from langchain_ollama import OllamaLLM
from qdrant_client.models import Prefetch, QueryRequest

from src.database import db_manager
from src.vector_db import qdrant_client, COLLECTION_NAME, get_embedding_model, get_reranking_model, _text_to_sparse_vector
from src.config import settings
from src.logger import get_logger

logger = get_logger(__name__)

# Lazy loading LLM instance specifically for Context Engineering tasks
_rag_llm = None

def get_rag_llm():
    global _rag_llm
    if _rag_llm is None:
        _rag_llm = OllamaLLM(model=settings.llm_coder_model, temperature=0.1)
    return _rag_llm

def tool_get_ticker_prices(ticker: str, limit: int = 30):
    """Semantic Layer Tool: Safely retrieves price history (timestamp, price, volume) for a specific symbol from ClickHouse."""
    try:
        df = db_manager.client.query_df(
            "SELECT timestamp, symbol, price, volume FROM prices WHERE symbol = %(symbol)s ORDER BY timestamp DESC LIMIT %(limit)s",
            parameters={"symbol": ticker, "limit": limit}
        )
        if df.empty:
            return [], None
        # Convert timestamp to string format for serialization
        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        return df.to_dict(orient='records'), None
    except Exception as e:
        logger.error(f"Error in tool_get_ticker_prices: {e}")
        return [], str(e)

def tool_get_ticker_indicators(ticker: str, limit: int = 30):
    """Semantic Layer Tool: Safely retrieves technical indicators (SMA_5, SMA_20, RSI, MACD, MACD_Signal) for a specific symbol from ClickHouse."""
    try:
        df = db_manager.client.query_df(
            "SELECT timestamp, symbol, price, SMA_5, SMA_20, RSI, MACD, MACD_Signal FROM prices WHERE symbol = %(symbol)s ORDER BY timestamp DESC LIMIT %(limit)s",
            parameters={"symbol": ticker, "limit": limit}
        )
        if df.empty:
            return [], None
        # Convert timestamp to string format for serialization
        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        return df.to_dict(orient='records'), None
    except Exception as e:
        logger.error(f"Error in tool_get_ticker_indicators: {e}")
        return [], str(e)

def tool_semantic_rag_search(processed_query: str) -> str:
    try:
        dense_vector = get_embedding_model().encode(processed_query).tolist()
        sparse_vector = _text_to_sparse_vector(processed_query)
        
        prefetch_dense = Prefetch(vector=dense_vector, limit=10)
        prefetch_sparse = Prefetch(vector={"name": "text-sparse", "vector": sparse_vector}, limit=10)
        
        hybrid_results = qdrant_client.query_batch_points(
            collection_name=COLLECTION_NAME,
            requests=[QueryRequest(prefetch=[prefetch_dense, prefetch_sparse], limit=10)]
        )
        search_results = hybrid_results[0].points if hybrid_results else []
        
        if not search_results:
            return "Không tìm thấy tài liệu phân tích vĩ mô liên quan trong Vector DB."
            
        # SEMANTIC RERANKING BASED ON CHILD CHUNK TO ENSURE HIGH PRECISION
        rerank_pairs = [(processed_query, res.payload.get('text', '')) for res in search_results]
        scores = get_reranking_model().predict(rerank_pairs)
        
        ranked_docs = []
        for idx, score in enumerate(scores):
            ranked_docs.append({
                # ARCHITECTURAL DECISION: Fetch Parent Text instead of Child Text
                "parent_context": search_results[idx].payload.get('parent_text', search_results[idx].payload.get('text', '')),
                "source": search_results[idx].payload.get('source', 'N/A'),
                "rerank_score": float(score)
            })
        ranked_docs.sort(key=lambda x: x["rerank_score"], reverse=True)
        
        seen_contexts = set()
        final_context_list = []
        
        for doc in ranked_docs:
            if doc["rerank_score"] < 0.1: 
                continue
            # Filter duplicates on the Parent Context layer
            context_hash = doc["parent_context"][:150]
            if context_hash not in seen_contexts:
                seen_contexts.add(context_hash)
                final_context_list.append(
                    f"Context: {doc['parent_context']} (Source: {doc['source']} | Score: {doc['rerank_score']:.2f})"
                )
            if len(final_context_list) >= 2: # Limit to 2 Parent contexts to avoid token dilution
                break

        return "\n\n".join(final_context_list)
    except Exception as e:
        return f"Lỗi trong hệ thống Hybrid Parent-Child RAG: {str(e)}"
    
OUTPUT_CHART_PATH = settings.chart_file_path

def tool_generate_market_chart(ticker: str) -> str:
    """Renders price trend chart adapted to the ClickHouse dataset structure"""
    if not ticker or ticker in ["UNKNOWN", "NONE"]:
        return "Không có mã cụ thể để vẽ biểu đồ."
        
    try:
        df = db_manager.client.query_df(
            "SELECT timestamp, price, volume FROM prices WHERE symbol = %(symbol)s ORDER BY timestamp ASC",
            parameters={"symbol": ticker}
        )
        
        if df.empty:
            return f"Không có dữ liệu trong Database để vẽ biểu đồ cho mã {ticker}."
            
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df = df.dropna(subset=['timestamp'])
        
        plt.figure(figsize=(10, 5))
        plt.plot(df['timestamp'], df['price'], marker='o', color='b', linestyle='-', linewidth=2, label='Giá trị (USD)')
        
        plt.title(f"BIỂU ĐỒ BIẾN ĐỘNG GIÁ TỰ ĐỘNG - THỰC THỂ: {ticker}", fontsize=14, fontweight='bold')
        plt.xlabel("Thời gian (Timestamp)", fontsize=10)
        plt.ylabel("Giá (USD)", fontsize=10)
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.legend()
        plt.xticks(rotation=15)
        plt.tight_layout()
        
        os.makedirs(os.path.dirname(OUTPUT_CHART_PATH), exist_ok=True)
        plt.savefig(OUTPUT_CHART_PATH)
        return f"Path: '{OUTPUT_CHART_PATH}'."
    except Exception as e:
        return f"Lỗi trong quá trình vẽ biểu đồ: {str(e)}"
    finally:
        plt.close('all')

def tool_calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Data Engineering Module: Computes technical indicators over a sliding window"""
    if df.empty: 
        return df
        
    df = df.copy()
    df['price'] = pd.to_numeric(df['price'], errors='coerce').astype(float)
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp', 'symbol', 'price'])
    
    df = df.sort_values(['symbol', 'timestamp']).reset_index(drop=True)

    df['SMA_5'] = df.groupby('symbol')['price'].transform(lambda x: x.rolling(5, min_periods=1).mean())
    df['SMA_20'] = df.groupby('symbol')['price'].transform(lambda x: x.rolling(20, min_periods=1).mean())
    
    def calc_rsi(series):
        if len(series) < 14: 
            return 50.0
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(14, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14, min_periods=1).mean()
        rs = gain / (loss + 1e-9)
        return 100 - (100 / (1 + rs))

    df['RSI'] = df.groupby('symbol')['price'].transform(calc_rsi)
    
    df['MACD'] = df.groupby('symbol')['price'].transform(lambda x: x.ewm(span=12, adjust=False).mean() - x.ewm(span=26, adjust=False).mean())
    df['MACD_Signal'] = df.groupby('symbol')['MACD'].transform(lambda x: x.ewm(span=9, adjust=False).mean())
    
    df = df.bfill().ffill()
    return df