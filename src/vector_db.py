import uuid
import zlib
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, SparseVectorParams, SparseIndexParams, PointStruct
from sentence_transformers import SentenceTransformer, CrossEncoder
from src.config import settings
from src.utils.logger import get_logger

DENSE_VECTOR_DIM: int = 384
SPARSE_BUCKET_SIZE: int = 1_000_000
DEFAULT_EMBEDDING_BATCH_SIZE: int = 32

logger = get_logger(__name__)

class VectorDBManager:
    """Manages Qdrant vector database operations, hybrid dense-sparse indexing, and batch ingestion."""

    def __init__(self):
        self.client = QdrantClient(host=settings.qdrant_host, port=settings.qdrant_port)
        self.collection_name = settings.qdrant_collection
        self._embedding_model = None
        self._reranking_model = None

    def get_embedding_model(self):
        """Lazy-loaded dense embedding model."""
        if self._embedding_model is None:
            self._embedding_model = SentenceTransformer(settings.embedding_model_name)
        return self._embedding_model

    def get_reranking_model(self):
        """Lazy-loaded cross-encoder reranking model."""
        if self._reranking_model is None:
            self._reranking_model = CrossEncoder(settings.rerank_model_name)
        return self._reranking_model

    def init_db(self) -> None:
        """Initializes a collection supporting Hybrid Search (Dense + Sparse/BM25) in Qdrant."""
        collections = self.client.get_collections().collections
        exists = any(c.name == self.collection_name for c in collections)
        
        if not exists:
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(
                    size=DENSE_VECTOR_DIM, 
                    distance=Distance.COSINE
                ),
                sparse_vectors_config={
                    "text-sparse": SparseVectorParams(
                        index=SparseIndexParams(
                            on_disk=True  # Optimize RAM usage by storing HNSW sparse indices on disk
                        )
                    )
                }
            )
            logger.info(f"Successfully initialized Qdrant Hybrid Collection: {self.collection_name}")
        else:
            logger.info(f"Qdrant Hybrid Collection '{self.collection_name}' already exists.")

    def text_to_sparse_vector(self, text_content: str) -> dict[str, list]:
        """
        Hashes raw text tokens consistently into a Qdrant Sparse Vector format (Term Frequency) using zlib.crc32.
        Aggregates frequency weights if hash collisions occur within the bucket space.
        """
        words = text_content.lower().split()
        frequency = {}
        for word in words:
            clean_word = "".join(ch for ch in word if ch.isalnum())
            if clean_word:
                frequency[clean_word] = frequency.get(clean_word, 0.0) + 1.0
                
        sparse_map = {}
        for word, count in frequency.items():
            idx = zlib.crc32(word.encode('utf-8')) % SPARSE_BUCKET_SIZE
            sparse_map[idx] = sparse_map.get(idx, 0.0) + float(count)
            
        return {
            "indices": list(sparse_map.keys()),
            "values": list(sparse_map.values())
        }

    def ingest_data(self, chunks_data: list[dict]) -> None:
        """
        Ingests hierarchical Parent-Child chunks into the Qdrant database using batch vectorization.
        """
        if not chunks_data:
            return
            
        texts = [chunk["text"] for chunk in chunks_data]
        dense_embeddings = self.get_embedding_model().encode(texts, batch_size=DEFAULT_EMBEDDING_BATCH_SIZE).tolist()
        
        points = []
        for idx, (chunk, dense_emb) in enumerate(zip(chunks_data, dense_embeddings)):
            text_block = chunk["text"]
            sparse_emb = self.text_to_sparse_vector(text_block)
            
            # Generate deterministic UUID v5 to prevent duplicates on re-ingestion
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
                        "parent_text": chunk.get("parent_text", text_block),
                        "source": chunk["source"],
                        "timestamp": chunk.get("timestamp", ""),
                        "hierarchy": chunk["chunk_hierarchy"]
                    }
                )
            )
            
        self.client.upsert(collection_name=self.collection_name, points=points)
        logger.info(f"Successfully ingested {len(points)} Parent-Child chunks to Qdrant.")

# Global instance for shared usage
vector_db_manager = VectorDBManager()