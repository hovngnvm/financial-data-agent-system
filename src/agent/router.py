import re
from typing import Any
import numpy as np
from sentence_transformers import SentenceTransformer
from src.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Standard Financial Ticker symbols for fast regex lookup
TICKER_REGEX = re.compile(
    r"\b(BTC|ETH|HPG|SSI|VND|FPT|VIC|VNM|MWG|VCB|MBB|TCB|ACB|STB|VPB|VHM|VRE|GAS|MSN|PLX|SAB|BID|CTG|POW|GVR|SHB|EIB|TPB|KDH|KBC|DGC|VHC|PVD|PVS|HSG|NKG)\b",
    re.IGNORECASE
)

# Clause splitting conjunctions (explicit compound connectors)
SPLIT_CONJUNCTIONS_REGEX = re.compile(r"\s*(?:và|đồng thời|cùng với)\s*", re.IGNORECASE)

# Canonical intent prototypes for vector centroid calculation
INTENT_UTTERANCES: dict[str, list[str]] = {
    "FETCH_PRICE": [
        "giá cổ phiếu hôm nay bao nhiêu",
        "lịch sử giá giao dịch",
        "cho xem khối lượng volume giao dịch",
        "giá mở cửa giá đóng cửa",
        "biến động giá hôm nay",
        "stock price quote history volume",
        "giá thị trường hiện tại"
    ],
    "FETCH_INDICATOR": [
        "chỉ số rsi đang ở mức nào",
        "đường trung bình sma 20 ngày",
        "tín hiệu macd phân kỳ",
        "chỉ báo kỹ thuật quá mua quá bán",
        "xu hướng kỹ thuật technical indicators",
        "rsi macd moving average signals",
        "tín hiệu đường ma"
    ],
    "FETCH_NEWS": [
        "tin tức kinh tế vĩ mô mới nhất",
        "báo cáo tài chính doanh nghiệp",
        "triển vọng ngành thép kinh doanh",
        "chính sách lãi suất ngân hàng nhà nước fed",
        "dự án dung quất thông tin sự kiện",
        "macroeconomic financial news report outlook"
    ],
    "RENDER_CHART": [
        "vẽ biểu đồ giá",
        "vẽ đồ thị rsi",
        "vẽ hình kỹ thuật macd",
        "hiển thị chart phân tích",
        "plot chart visualize graph",
        "vẽ đồ thị phân tích kỹ thuật"
    ],
    "CHITCHAT": [
        "xin chào bạn",
        "chào bot",
        "bạn là ai",
        "hôm nay thế nào",
        "hôm nay thời tiết thế nào",
        "cảm ơn bạn nhé",
        "tạm biệt bot",
        "hello hi there how are you",
        "bạn có thể làm được những gì",
        "trò chuyện tâm sự một chút",
        "chúc một ngày tốt lành"
    ]
}

class SemanticVectorRouter:
    """
    Sub-3ms Fast-Path Intent Router:
    Uses precomputed dense vector centroids from all-MiniLM-L6-v2 to evaluate Cosine Similarity
    with Multi-Threshold support and Clause Splitting for compound queries.
    """
    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self.model = SentenceTransformer(model_name)
        self.intent_centroids: dict[str, np.ndarray] = {}
        self._precompute_centroids()

    def _precompute_centroids(self) -> None:
        """Precomputes and normalizes unit vector centroids for all canonical intent clusters."""
        for intent_name, utterances in INTENT_UTTERANCES.items():
            embeddings = self.model.encode(utterances, convert_to_numpy=True, normalize_embeddings=True)
            centroid = np.mean(embeddings, axis=0)
            # Normalize centroid vector to unit length for direct dot product cosine similarity
            norm = np.linalg.norm(centroid)
            if norm > 0:
                centroid = centroid / norm
            self.intent_centroids[intent_name] = centroid

    def extract_ticker(self, text: str) -> str:
        """Extracts and normalizes the target ticker symbol from input text."""
        match = TICKER_REGEX.search(text)
        return match.group(1).upper() if match else "UNKNOWN"

    def detect_chart_mode(self, text: str) -> str:
        """Determines the specific multi-panel chart visualization mode."""
        lower_text = text.lower()
        if any(kw in lower_text for kw in ["rsi", "quá mua", "quá bán", "overbought", "oversold"]):
            return "rsi"
        if any(kw in lower_text for kw in ["macd", "phân kỳ", "divergence", "động lượng", "momentum"]):
            return "macd"
        if any(kw in lower_text for kw in ["volume", "khối lượng", "thanh khoản"]):
            return "volume"
        if any(kw in lower_text for kw in ["sma", "đường trung bình", "moving average", "ma5", "ma20"]):
            return "price_sma"
        return "comprehensive"

    def split_clauses(self, query: str) -> list[str]:
        """Splits compound sentences into standalone sub-clauses for multi-intent evaluation."""
        clauses = [c.strip() for c in SPLIT_CONJUNCTIONS_REGEX.split(query) if len(c.strip()) > 2]
        return clauses if clauses else [query.strip()]

    def fast_route(self, query: str, threshold: float | None = None) -> dict[str, Any] | None:
        """
        Fast-Path Intent Evaluation:
        Computes cosine similarities for all sub-clauses against intent centroids.
        Returns a structured routing dictionary if confident, or None to fallback to LLM.
        """
        active_threshold = threshold if threshold is not None else settings.semantic_router_threshold
        target_ticker = self.extract_ticker(query)
        clauses = self.split_clauses(query)
        
        detected_intents: set[str] = set()
        max_overall_score = 0.0
        
        for clause in clauses:
            clause_vector = self.model.encode(clause, convert_to_numpy=True, normalize_embeddings=True)
            clause_scores = {
                intent_name: float(np.dot(clause_vector, centroid))
                for intent_name, centroid in self.intent_centroids.items()
            }
            best_intent = max(clause_scores, key=clause_scores.get)
            best_score = clause_scores[best_intent]
            max_overall_score = max(max_overall_score, best_score)
            
            if best_score >= active_threshold:
                detected_intents.add(best_intent)
                
            # Check secondary high-affinity intents in same clause (e.g. chart + indicator)
            for intent_name, score in clause_scores.items():
                if intent_name != best_intent and intent_name != "CHITCHAT" and score >= max(active_threshold, best_score - 0.06):
                    detected_intents.add(intent_name)

        # Fallback condition: ambiguous query or low confidence
        if not detected_intents or max_overall_score < (active_threshold - 0.10):
            return None

        # Clean chitchat isolation
        if "CHITCHAT" in detected_intents:
            if target_ticker == "UNKNOWN" and any(
                clause_scores.get("CHITCHAT", 0.0) >= max(clause_scores.get("FETCH_PRICE", 0.0), clause_scores.get("FETCH_NEWS", 0.0))
                for clause in clauses
            ):
                detected_intents = {"CHITCHAT"}
            elif len(detected_intents) > 1:
                detected_intents.remove("CHITCHAT")

        is_chitchat = "CHITCHAT" in detected_intents and len(detected_intents) == 1
        has_chart = "RENDER_CHART" in detected_intents
        chart_mode = self.detect_chart_mode(query) if has_chart else None

        # If user asks for chart without explicit indicator, add FETCH_INDICATOR or FETCH_PRICE
        if has_chart and not any(i in detected_intents for i in ["FETCH_PRICE", "FETCH_INDICATOR"]):
            detected_intents.add("FETCH_INDICATOR" if chart_mode in ["rsi", "macd", "price_sma"] else "FETCH_PRICE")

        activated_list = sorted(list(detected_intents))

        return {
            "current_target": target_ticker,
            "activated_intents": activated_list,
            "chart_mode": chart_mode,
            "chat": is_chitchat,
            "next_worker": "FINAL_ANALYST" if is_chitchat else "PARALLEL_EXECUTE",
            "routing_source": "SEMANTIC_VECTOR_FAST_PATH"
        }

# Global singleton router instance
semantic_router = SemanticVectorRouter(settings.embedding_model_name)
