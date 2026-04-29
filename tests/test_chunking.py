import pytest
from src.utils.chunking import preserve_markdown_tables, advanced_parent_child_chunker

def test_preserve_markdown_tables():
    text = (
        "Introductory paragraph.\n\n"
        "| Header 1 | Header 2 |\n"
        "|----------|----------|\n"
        "| Value 1  | Value 2  |\n\n"
        "Concluding paragraph."
    )
    placeholder_text, tables = preserve_markdown_tables(text)
    
    assert len(tables) == 1
    assert "Header 1" in tables[0]
    assert "__TABLE_PLACEHOLDER_0__" in placeholder_text
    assert "Header 1" not in placeholder_text

def test_preserve_multiple_identical_tables():
    table = (
        "| A | B |\n"
        "|---|---|\n"
        "| 1 | 2 |"
    )
    text = f"{table}\n\nMiddle text\n\n{table}"
    placeholder_text, tables = preserve_markdown_tables(text)
    
    assert len(tables) == 2
    assert "__TABLE_PLACEHOLDER_0__" in placeholder_text
    assert "__TABLE_PLACEHOLDER_1__" in placeholder_text

def test_advanced_parent_child_chunker():
    text = (
        "Hoa Phat Group (HPG) expects strong revenue growth. "
        "This is an additional sentence to evaluate sentence splitting logic. "
        "| Column A | Column B |\n"
        "|---|---|\n"
        "| X | Y |"
    )
    chunks = advanced_parent_child_chunker(text, source_link="http://test.com", parent_size=100, child_size=30)
    
    assert len(chunks) > 0
    # Verify source mapping
    assert chunks[0]["source"] == "http://test.com"
    # Verify required parent context and hierarchy metadata
    assert "parent_text" in chunks[0]
    assert "chunk_hierarchy" in chunks[0]
    assert "Column A" in chunks[0]["parent_text"]

def test_advanced_parent_child_chunker_empty():
    chunks = advanced_parent_child_chunker("", source_link="http://test.com")
    assert chunks == []
