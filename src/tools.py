import os
import matplotlib
matplotlib.use('Agg')  # Headless backend for server environments
import matplotlib.pyplot as plt
import pandas as pd
from qdrant_client.models import Prefetch, FusionQuery, Fusion

from src.database import db_manager
from src.vector_db import qdrant_client, COLLECTION_NAME, get_embedding_model, get_reranking_model, _text_to_sparse_vector
from src.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)


def tool_get_ticker_prices(ticker: str, limit: int = 30) -> tuple[list[dict], str | None]:
    """Semantic Layer Tool: Safely retrieves price history (timestamp, price, volume) for a specific symbol from ClickHouse."""
    try:
        df = db_manager.client.query_df(
            "SELECT timestamp, symbol, price, volume FROM prices WHERE symbol = %(symbol)s OR symbol LIKE %(symbol_pattern)s ORDER BY timestamp DESC LIMIT %(limit)s",
            parameters={"symbol": ticker, "symbol_pattern": f"{ticker}%", "limit": limit}
        )
        if df.empty:
            return [], None
        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        return df.to_dict(orient='records'), None
    except Exception as e:
        logger.error(f"Error in tool_get_ticker_prices: {e}")
        return [], str(e)

def tool_get_ticker_indicators(ticker: str, limit: int = 30) -> tuple[list[dict], str | None]:
    """Semantic Layer Tool: Safely retrieves technical indicators (SMA_5, SMA_20, RSI, MACD, MACD_Signal) for a specific symbol from ClickHouse."""
    try:
        df = db_manager.client.query_df(
            "SELECT timestamp, symbol, price, volume, SMA_5, SMA_20, RSI, MACD, MACD_Signal FROM prices WHERE symbol = %(symbol)s OR symbol LIKE %(symbol_pattern)s ORDER BY timestamp DESC LIMIT %(limit)s",
            parameters={"symbol": ticker, "symbol_pattern": f"{ticker}%", "limit": limit}
        )
        if df.empty:
            return [], None
        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        return df.to_dict(orient='records'), None
    except Exception as e:
        logger.error(f"Error in tool_get_ticker_indicators: {e}")
        return [], str(e)

def tool_semantic_rag_search(processed_query: str) -> str:
    """Performs hybrid dense-sparse vector search and cross-encoder reranking on Qdrant knowledge base."""
    try:
        dense_vector = get_embedding_model().encode(processed_query).tolist()
        sparse_vector = _text_to_sparse_vector(processed_query)
        
        prefetch_dense = Prefetch(query=dense_vector, limit=10)
        prefetch_sparse = Prefetch(query=sparse_vector, using="text-sparse", limit=10)
        
        hybrid_response = qdrant_client.query_points(
            collection_name=COLLECTION_NAME,
            prefetch=[prefetch_dense, prefetch_sparse],
            query=FusionQuery(fusion=Fusion.RRF),
            limit=10
        )
        search_results = hybrid_response.points if hybrid_response else []
        
        if not search_results:
            return "No related macro-financial documents found in Vector DB."
            
        rerank_pairs = [(processed_query, res.payload.get('text', '')) for res in search_results]
        scores = get_reranking_model().predict(rerank_pairs)
        
        ranked_docs = []
        for idx, score in enumerate(scores):
            ranked_docs.append({
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
            context_hash = doc["parent_context"][:150]
            if context_hash not in seen_contexts:
                seen_contexts.add(context_hash)
                final_context_list.append(
                    f"Context: {doc['parent_context']} (Source: {doc['source']} | Score: {doc['rerank_score']:.2f})"
                )
            if len(final_context_list) >= 2:
                break

        return "\n\n".join(final_context_list)
    except Exception as e:
        logger.error(f"Error during hybrid RAG retrieval: {e}")
        return f"Error during hybrid RAG retrieval: {e}"

OUTPUT_CHART_PATH = settings.chart_file_path

def tool_generate_market_chart(ticker: str, chart_type: str = "comprehensive", limit: int = 30) -> str:
    """
    Dynamic Financial Chart Generator: Renders multi-panel technical analysis charts.
    Supported chart_type:
    - 'price_sma': Price trend with 5-period & 20-period Moving Averages
    - 'rsi': Price chart + Relative Strength Index oscillator (30/70 thresholds)
    - 'macd': Price chart + MACD & Signal Line + Histogram momentum
    - 'volume': Price chart + Trading Volume bars
    - 'comprehensive': 2-panel professional view with Price, SMAs, Volume, and RSI
    """
    if not ticker or ticker in ["UNKNOWN", "NONE"]:
        return "No valid ticker specified for chart generation."
        
    chart_type = chart_type.lower() if chart_type else "comprehensive"
    fig = None
    
    try:
        df = db_manager.client.query_df(
            "SELECT timestamp, price, volume, SMA_5, SMA_20, RSI, MACD, MACD_Signal FROM prices WHERE symbol = %(symbol)s OR symbol LIKE %(symbol_pattern)s ORDER BY timestamp ASC LIMIT %(limit)s",
            parameters={"symbol": ticker, "symbol_pattern": f"{ticker}%", "limit": limit}
        )
        
        if df.empty:
            return f"No price data available in database to render chart for {ticker}."
            
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df = df.dropna(subset=['timestamp'])
        
        if df.empty:
            return f"Invalid timestamp records for {ticker}."

        os.makedirs(os.path.dirname(OUTPUT_CHART_PATH), exist_ok=True)
        
        # 1. RSI Chart Mode (2 Subplots)
        if chart_type == "rsi":
            fig, (ax_price, ax_rsi) = plt.subplots(2, 1, figsize=(10, 7), sharex=True, gridspec_kw={'height_ratios': [2, 1]})
            
            ax_price.plot(df['timestamp'], df['price'], marker='o', color='#1f77b4', linewidth=2, label='Price')
            ax_price.set_title(f"Technical Analysis (Price & RSI) - {ticker}", fontsize=13, fontweight='bold')
            ax_price.set_ylabel("Price (USD)", fontsize=10)
            ax_price.grid(True, linestyle='--', alpha=0.5)
            ax_price.legend(loc='upper left')
            
            if 'RSI' in df.columns and not df['RSI'].isna().all():
                ax_rsi.plot(df['timestamp'], df['RSI'], color='#9467bd', linewidth=1.8, label='RSI (14)')
                ax_rsi.axhline(70, color='#d62728', linestyle='--', alpha=0.7, label='Overbought (70)')
                ax_rsi.axhline(30, color='#2ca02c', linestyle='--', alpha=0.7, label='Oversold (30)')
                ax_rsi.fill_between(df['timestamp'], 30, 70, color='#9467bd', alpha=0.1)
                ax_rsi.set_ylabel("RSI", fontsize=10)
                ax_rsi.set_ylim(0, 100)
                ax_rsi.grid(True, linestyle='--', alpha=0.5)
                ax_rsi.legend(loc='upper right', fontsize=8)
            ax_rsi.set_xlabel("Time", fontsize=10)
            
        # 2. MACD Chart Mode (2 Subplots)
        elif chart_type == "macd":
            fig, (ax_price, ax_macd) = plt.subplots(2, 1, figsize=(10, 7), sharex=True, gridspec_kw={'height_ratios': [2, 1]})
            
            ax_price.plot(df['timestamp'], df['price'], marker='o', color='#1f77b4', linewidth=2, label='Price')
            ax_price.set_title(f"Technical Analysis (Price & MACD Momentum) - {ticker}", fontsize=13, fontweight='bold')
            ax_price.set_ylabel("Price (USD)", fontsize=10)
            ax_price.grid(True, linestyle='--', alpha=0.5)
            ax_price.legend(loc='upper left')
            
            if 'MACD' in df.columns and 'MACD_Signal' in df.columns:
                ax_macd.plot(df['timestamp'], df['MACD'], color='#1f77b4', linewidth=1.5, label='MACD')
                ax_macd.plot(df['timestamp'], df['MACD_Signal'], color='#ff7f0e', linewidth=1.5, linestyle='--', label='Signal')
                macd_hist = df['MACD'] - df['MACD_Signal']
                colors = ['#2ca02c' if val >= 0 else '#d62728' for val in macd_hist]
                ax_macd.bar(df['timestamp'], macd_hist, color=colors, alpha=0.5, label='Histogram', width=0.03)
                ax_macd.axhline(0, color='gray', linestyle='-', linewidth=0.8)
                ax_macd.set_ylabel("MACD", fontsize=10)
                ax_macd.grid(True, linestyle='--', alpha=0.5)
                ax_macd.legend(loc='upper right', fontsize=8)
            ax_macd.set_xlabel("Time", fontsize=10)

        # 3. Volume Chart Mode (2 Subplots)
        elif chart_type == "volume":
            fig, (ax_price, ax_vol) = plt.subplots(2, 1, figsize=(10, 6), sharex=True, gridspec_kw={'height_ratios': [2, 1]})
            
            ax_price.plot(df['timestamp'], df['price'], marker='o', color='#1f77b4', linewidth=2, label='Price')
            ax_price.set_title(f"Price & Trading Volume - {ticker}", fontsize=13, fontweight='bold')
            ax_price.set_ylabel("Price (USD)", fontsize=10)
            ax_price.grid(True, linestyle='--', alpha=0.5)
            ax_price.legend(loc='upper left')
            
            ax_vol.bar(df['timestamp'], df['volume'], color='#2ca02c', alpha=0.6, label='Volume')
            ax_vol.set_ylabel("Volume", fontsize=10)
            ax_vol.grid(True, linestyle='--', alpha=0.5)
            ax_vol.legend(loc='upper left', fontsize=8)
            ax_vol.set_xlabel("Time", fontsize=10)

        # 4. Price + SMA Trend Chart Mode (1 Subplot)
        elif chart_type == "price_sma":
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.plot(df['timestamp'], df['price'], marker='o', color='#1f77b4', linewidth=2, label='Price')
            if 'SMA_5' in df.columns and not df['SMA_5'].isna().all():
                ax.plot(df['timestamp'], df['SMA_5'], color='#ff7f0e', linestyle='--', linewidth=1.5, label='SMA-5')
            if 'SMA_20' in df.columns and not df['SMA_20'].isna().all():
                ax.plot(df['timestamp'], df['SMA_20'], color='#2ca02c', linestyle='-.', linewidth=1.5, label='SMA-20')
            ax.set_title(f"Moving Average Trend (SMA) - {ticker}", fontsize=13, fontweight='bold')
            ax.set_xlabel("Time", fontsize=10)
            ax.set_ylabel("Price (USD)", fontsize=10)
            ax.grid(True, linestyle='--', alpha=0.5)
            ax.legend(loc='upper left')

        # 5. Comprehensive Professional View (Default - 2 Subplots with Price/SMAs/Volume + RSI)
        else:
            fig, (ax_price, ax_lower) = plt.subplots(2, 1, figsize=(10, 7), sharex=True, gridspec_kw={'height_ratios': [2, 1]})
            
            # Top Panel: Price + SMA-5 + SMA-20
            ax_price.plot(df['timestamp'], df['price'], marker='o', color='#1f77b4', linewidth=2, label='Price')
            if 'SMA_5' in df.columns and not df['SMA_5'].isna().all():
                ax_price.plot(df['timestamp'], df['SMA_5'], color='#ff7f0e', linestyle='--', linewidth=1.5, label='SMA-5')
            if 'SMA_20' in df.columns and not df['SMA_20'].isna().all():
                ax_price.plot(df['timestamp'], df['SMA_20'], color='#2ca02c', linestyle='-.', linewidth=1.5, label='SMA-20')
                
            ax_price.set_title(f"Comprehensive Market Analysis - {ticker}", fontsize=13, fontweight='bold')
            ax_price.set_ylabel("Price (USD)", fontsize=10)
            ax_price.grid(True, linestyle='--', alpha=0.5)
            ax_price.legend(loc='upper left', fontsize=9)
            
            # Bottom Panel: RSI
            if 'RSI' in df.columns and not df['RSI'].isna().all():
                ax_lower.plot(df['timestamp'], df['RSI'], color='#9467bd', linewidth=1.8, label='RSI (14)')
                ax_lower.axhline(70, color='#d62728', linestyle='--', alpha=0.7, label='Overbought (70)')
                ax_lower.axhline(30, color='#2ca02c', linestyle='--', alpha=0.7, label='Oversold (30)')
                ax_lower.fill_between(df['timestamp'], 30, 70, color='#9467bd', alpha=0.1)
                ax_lower.set_ylabel("RSI", fontsize=10)
                ax_lower.set_ylim(0, 100)
                ax_lower.grid(True, linestyle='--', alpha=0.5)
                ax_lower.legend(loc='upper right', fontsize=8)
            else:
                ax_lower.bar(df['timestamp'], df['volume'], color='#7f7f7f', alpha=0.6, label='Volume')
                ax_lower.set_ylabel("Volume", fontsize=10)
                ax_lower.grid(True, linestyle='--', alpha=0.5)
                ax_lower.legend(loc='upper left', fontsize=8)
                
            ax_lower.set_xlabel("Time", fontsize=10)

        fig.autofmt_xdate(rotation=15)
        fig.tight_layout()
        fig.savefig(OUTPUT_CHART_PATH, dpi=120)
        return f"Rendered {chart_type} chart successfully at '{OUTPUT_CHART_PATH}'."
        
    except Exception as e:
        logger.error(f"Failed to render chart for {ticker}: {e}")
        return f"Error during chart rendering: {e}"
    finally:
        if fig is not None:
            plt.close(fig)

def tool_calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Data Engineering Module: Computes technical indicators over a sliding window."""
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