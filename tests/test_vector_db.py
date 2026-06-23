import pytest
from unittest.mock import MagicMock
from src.vector_db import VectorDBManager

def test_vector_db_manager_init(mocker):
    mock_qdrant = mocker.patch("src.vector_db.QdrantClient")
    
    vdb_mgr = VectorDBManager()
    
    assert vdb_mgr.collection_name == "financial_reports"
    mock_qdrant.assert_called_once()

def test_vector_db_manager_sparse_vector(mocker):
    mocker.patch("src.vector_db.QdrantClient")
    vdb_mgr = VectorDBManager()
    
    sparse1 = vdb_mgr.text_to_sparse_vector("HPG")
    sparse2 = vdb_mgr.text_to_sparse_vector("HPG")
    
    # Verify deterministic zlib.crc32 output
    assert sparse1 == sparse2
    assert "indices" in sparse1
    assert "values" in sparse1
    assert len(sparse1["indices"]) == 1
    assert sparse1["values"][0] == 1.0

def test_vector_db_manager_sparse_vector_collision_deduplication(mocker):
    mocker.patch("src.vector_db.QdrantClient")
    vdb_mgr = VectorDBManager()
    
    # Mock zlib.crc32 to return identical index to verify weight aggregation
    mocker.patch("zlib.crc32", return_value=42)
    sparse = vdb_mgr.text_to_sparse_vector("apple orange banana")
    
    # All 3 words map to bucket 42; their weights should be summed (3.0) without duplicate indices
    assert sparse["indices"] == [42]
    assert sparse["values"] == [3.0]

def test_vector_db_manager_ingest_data_batch(mocker):
    mock_qdrant_instance = MagicMock()
    mocker.patch("src.vector_db.QdrantClient", return_value=mock_qdrant_instance)
    
    # Mock encoder batch output (2D list for batch encoding)
    mock_encoder = MagicMock()
    mock_encoder.encode.return_value.tolist.return_value = [[0.1] * 384]
    
    vdb_mgr = VectorDBManager()
    mocker.patch.object(vdb_mgr, "get_embedding_model", return_value=mock_encoder)
    
    chunks = [
        {
            "text": "Ngành thép tăng trưởng mạnh HPG.",
            "source": "Báo cáo 2026",
            "chunk_hierarchy": "p0-c0"
        }
    ]
    
    vdb_mgr.ingest_data(chunks)
    
    # Verify batch encode call
    mock_encoder.encode.assert_called_once_with(["Ngành thép tăng trưởng mạnh HPG."], batch_size=32)
    
    # Verify upsert call
    mock_qdrant_instance.upsert.assert_called_once()
    call_args = mock_qdrant_instance.upsert.call_args
    assert call_args[1]["collection_name"] == vdb_mgr.collection_name
    points = call_args[1]["points"]
    assert len(points) == 1
    assert points[0].payload["source"] == "Báo cáo 2026"
    assert points[0].payload["hierarchy"] == "p0-c0"
