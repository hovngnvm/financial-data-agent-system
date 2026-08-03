import re

# Precompiled regex patterns to eliminate per-call compilation overhead
TABLE_REGEX = re.compile(r'(?:\|[^\n]+\|\n\|(?:[\s]*:?-+:?[\s]*\|)+\n(?:\|[^\n]+\|\n*)+)')

SENTENCE_END_REGEX = re.compile(
    r'(?<!\b(?:VND|Inc|Ltd|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.)'
    r'(?<!\bU\.S\.)'
    r'(?<!\bCorp\.)'
    r'(?<!\bvs\.)'
    r'(?<=[.!?])\s+'
)

def preserve_markdown_tables(text: str) -> tuple[str, list[str]]:
    """
    Identifies and isolates Markdown tables.
    Ensures financial numeric tables are not split arbitrarily during chunking.
    """
    tables = TABLE_REGEX.findall(text)
    placeholder_text = text
    for idx, table in enumerate(tables):
        placeholder_text = placeholder_text.replace(table, f"__TABLE_PLACEHOLDER_{idx}__", 1)
        
    return placeholder_text, tables

def advanced_parent_child_chunker(text: str, source_link: str, parent_size: int = 1200, child_size: int = 250) -> list[dict]:
    """
    Hierarchical text splitter (Parent-Child Chunking) preserving sentence boundaries and Markdown tables.
    """
    # Isolate financial tables
    processed_text, preserved_tables = preserve_markdown_tables(text)
    
    # Split text into paragraphs
    paragraphs = [p.strip() for p in processed_text.split("\n\n") if p.strip()]
    
    parent_chunks = []
    current_parent = []
    current_length = 0
    
    # Group small paragraphs into larger Parent Context blocks (~1200 characters)
    for para in paragraphs:
        current_parent.append(para)
        current_length += len(para)
        
        if current_length >= parent_size:
            parent_chunks.append("\n\n".join(current_parent))
            current_parent = []
            current_length = 0
            
    if current_parent:
        parent_chunks.append("\n\n".join(current_parent))
        
    final_prepared_payloads = []
    
    # Decompose Parent blocks into Child Chunks (~250 characters)
    for p_idx, parent_content in enumerate(parent_chunks):
        actual_parent_text = parent_content
        for t_idx, table_content in enumerate(preserved_tables):
            actual_parent_text = actual_parent_text.replace(f"__TABLE_PLACEHOLDER_{t_idx}__", table_content)
            
        sentences = SENTENCE_END_REGEX.split(parent_content)
        
        current_child = []
        current_child_len = 0
        
        for sentence in sentences:
            current_child.append(sentence)
            current_child_len += len(sentence)
            
            if current_child_len >= child_size:
                child_text_raw = " ".join(current_child).strip()
                if child_text_raw:
                    child_text = child_text_raw
                    for t_idx, table_content in enumerate(preserved_tables):
                        child_text = child_text.replace(f"__TABLE_PLACEHOLDER_{t_idx}__", table_content)
                        
                    final_prepared_payloads.append({
                        "text": child_text,
                        "parent_text": actual_parent_text,
                        "source": source_link,
                        "chunk_hierarchy": f"p{p_idx}-c{len(final_prepared_payloads)}"
                    })
                current_child = []
                current_child_len = 0
                
        # Handle the remaining trailing child sequence in this parent block
        if current_child:
            child_text_raw = " ".join(current_child).strip()
            if child_text_raw:
                child_text = child_text_raw
                for t_idx, table_content in enumerate(preserved_tables):
                    child_text = child_text.replace(f"__TABLE_PLACEHOLDER_{t_idx}__", table_content)
                    
                final_prepared_payloads.append({
                    "text": child_text,
                    "parent_text": actual_parent_text,
                    "source": source_link,
                    "chunk_hierarchy": f"p{p_idx}-tail"
                })
                
    return final_prepared_payloads
