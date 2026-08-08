import os
import uuid
import mmh3
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, SparseVectorParams, SparseIndexParams, PointStruct
from src.config import settings
from src.logger import get_logger

logger = get_logger(__name__)

class VectorDBManager:
    def __init__(self):
        # Initialize Qdrant Client connection from settings config
        self.client = QdrantClient(host=settings.qdrant_host, port=settings.qdrant_port)
        self.collection_name = settings.qdrant_collection
        self._embedding_model = None
        self._reranking_model = None

    def get_embedding_model(self):
        if self._embedding_model is None:
            from sentence_transformers import SentenceTransformer
            self._embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
        return self._embedding_model

    def get_reranking_model(self):
        if self._reranking_model is None:
            from sentence_transformers import CrossEncoder
            self._reranking_model = CrossEncoder("BAAI/bge-reranker-v2-m3")
        return self._reranking_model

    def init_db(self):
        """Initializes a collection supporting Hybrid Search (Dense + Sparse/BM25) in Qdrant"""
        collections = self.client.get_collections().collections
        exists = any(c.name == self.collection_name for c in collections)
        
        if not exists:
            self.client.create_collection(
                collection_name=self.collection_name,
                # 1. Configuration for Dense Semantic Vector
                vectors_config=VectorParams(
                    size=384, 
                    distance=Distance.COSINE
                ),
                # 2. Configuration for exact keyword matching (Sparse / BM25)
                sparse_vectors_config={
                    "text-sparse": SparseVectorParams(
                        index=SparseIndexParams(
                            on_disk=True # Optimize RAM usage by storing HNSW sparse indexes on disk
                        )
                    )
                }
            )
            logger.info(f"-> [Qdrant Enterprise]: Successfully initialized Hybrid Search Collection: {self.collection_name}")
        else:
            logger.info(f"-> [Qdrant Enterprise]: Collection '{self.collection_name}' already exists.")

    def text_to_sparse_vector(self, text_content: str) -> dict:
        """
        Hashes raw text tokens consistently into a Qdrant Sparse Vector format (Term Frequency) using mmh3.
        """
        words = text_content.lower().split()
        frequency = {}
        for word in words:
            # Strip basic special characters around the token
            clean_word = "".join(ch for ch in word if ch.isalnum())
            if clean_word:
                frequency[clean_word] = frequency.get(clean_word, 0.0) + 1.0
                
        # Map to indices and values formats
        indices = []
        values = []
        for word, count in frequency.items():
            indices.append(abs(mmh3.hash(word)) % 1000000) 
            values.append(float(count))
            
        return {"indices": indices, "values": values}

    def ingest_data(self, chunks_data: list[dict]):
        """Ingests hierarchical Parent-Child chunks into the Qdrant database"""
        if not chunks_data:
            return
            
        points = []
        for idx, chunk in enumerate(chunks_data):
            text_block = chunk["text"] # Child text used for vector distance calculations
            
            dense_emb = self.get_embedding_model().encode(text_block).tolist()
            sparse_emb = self.text_to_sparse_vector(text_block)
            
            # Generate deterministic UUID v5 based on document URL and hierarchy to prevent duplicates on re-ingestion
            unique_str = f"{chunk['source']}_{chunk['chunk_hierarchy']}_{idx}_{text_block}"
            point_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_str))
            
            points.append(
                PointStruct(
                    id=point_uuid,
                    vector={
                        "": dense_emb,
                        "text-sparse": sparse_emb
                    },
                    payload={
                        "text": text_block,
                        "parent_text": chunk.get("parent_text", text_block), # DIRECTLY STORE PARENT TEXT IN PAYLOAD
                        "source": chunk["source"],
                        "timestamp": chunk.get("timestamp", ""),
                        "hierarchy": chunk["chunk_hierarchy"]
                    }
                )
            )
            
        self.client.upsert(collection_name=self.collection_name, points=points)
        logger.info(f"-> [Qdrant Ingestion]: Successfully ingested {len(points)} Parent-Child chunks.")

# Initialize global VectorDBManager instance for shared usage
vector_db_manager = VectorDBManager()
qdrant_client = vector_db_manager.client
COLLECTION_NAME = vector_db_manager.collection_name

# Backward-compatibility wrappers
def get_embedding_model():
    return vector_db_manager.get_embedding_model()

def get_reranking_model():
    return vector_db_manager.get_reranking_model()

def init_vector_database():
    vector_db_manager.init_db()

def _text_to_sparse_vector(text_content: str) -> dict:
    return vector_db_manager.text_to_sparse_vector(text_content)

def ingest_data_to_qdrant(chunks_data: list[dict]):
    vector_db_manager.ingest_data(chunks_data)