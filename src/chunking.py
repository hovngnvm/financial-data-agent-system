import re

def preserve_markdown_tables(text: str) -> list:
    """
    Identifies and isolates Markdown tables.
    Ensures financial numeric tables are not split arbitrarily.
    """
    # Regex to identify standard Markdown table structures
    table_regex = re.compile(r'(?:\|[^\n]+\|\n\|(?:[\s]*:?-+:?[\s]*\|)+\n(?:\|[^\n]+\|\n*)+)')
    
    tables = table_regex.findall(text)
    # Temporarily replace tables with a unique identifier to avoid parsing side effects during chunk splitting
    placeholder_text = text
    for idx, table in enumerate(tables):
        # Use count=1 to prevent replacing identical tables all at once which shifts indices
        placeholder_text = placeholder_text.replace(table, f"__TABLE_PLACEHOLDER_{idx}__", 1)
        
    return placeholder_text, tables

def advanced_parent_child_chunker(text: str, source_link: str, parent_size: int = 1200, child_size: int = 250) -> list[dict]:
    """
    Configures a hierarchical text splitter (Parent-Child Chunking)
    to preserve sentence boundaries and financial tables.
    """
    # Step 1: Isolate financial tables
    processed_text, preserved_tables = preserve_markdown_tables(text)
    
    # Step 2: Split text into paragraphs based on double line breaks
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
    
    # Avoid splitting sentences at common abbreviations in financial reports
    sentence_end = re.compile(
        r'(?<!\b(?:VND|Inc|Ltd|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.)'
        r'(?<!\bU\.S\.)'
        r'(?<!\bCorp\.)'
        r'(?<!\bvs\.)'
        r'(?<=[.!?])\s+'
    )
    
    # Step 3: Decompose each Parent block into smaller Child Chunks (~250 characters) to compute vector embeddings
    for p_idx, parent_content in enumerate(parent_chunks):
        # Reconstruct the parent text by restoring the actual table structures
        actual_parent_text = parent_content
        for t_idx, table_content in enumerate(preserved_tables):
            actual_parent_text = actual_parent_text.replace(f"__TABLE_PLACEHOLDER_{t_idx}__", table_content)
            
        # Split child chunks based on sentence endings or whitespace from parent_content placeholders
        # to ensure table layout structure is not corrupted
        sentences = sentence_end.split(parent_content)
        
        current_child = []
        current_child_len = 0
        
        for sentence in sentences:
            current_child.append(sentence)
            current_child_len += len(sentence)
            
            if current_child_len >= child_size:
                child_text_raw = " ".join(current_child).strip()
                if child_text_raw:
                    # Restore table in child_text
                    child_text = child_text_raw
                    for t_idx, table_content in enumerate(preserved_tables):
                        child_text = child_text.replace(f"__TABLE_PLACEHOLDER_{t_idx}__", table_content)
                        
                    final_prepared_payloads.append({
                        "text": child_text,                 # Used to generate Dense/Sparse Vector (Child)
                        "parent_text": actual_parent_text,   # Context fed into the LLM prompt (Parent)
                        "source": source_link,
                        "chunk_hierarchy": f"p{p_idx}-c{len(final_prepared_payloads)}"
                    })
                current_child = []
                current_child_len = 0
                
        # Handle the remaining trailing child sequence in this parent block
        if current_child:
            child_text_raw = " ".join(current_child).strip()
            if child_text_raw:
                # Restore table in child_text
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