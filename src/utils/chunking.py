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

def _restore_tables(text: str, tables: list[str]) -> str:
    restored = text
    for idx, table_content in enumerate(tables):
        restored = restored.replace(f"__TABLE_PLACEHOLDER_{idx}__", table_content)
    return restored

def preserve_markdown_tables(text: str) -> tuple[str, list[str]]:
    """
    Identifies and isolates Markdown tables.
    Ensures financial numeric tables are not split arbitrarily during chunking.
    """
    tables = []
    def _repl(m: re.Match) -> str:
        idx = len(tables)
        tables.append(m.group(0))
        return f"__TABLE_PLACEHOLDER_{idx}__"

    return TABLE_REGEX.sub(_repl, text), tables

def advanced_parent_child_chunker(text: str, source_link: str, parent_size: int = 1200, child_size: int = 250) -> list[dict]:
    """
    Hierarchical text splitter (Parent-Child Chunking) preserving sentence boundaries and Markdown tables.
    """
    processed_text, preserved_tables = preserve_markdown_tables(text)
    paragraphs = [p.strip() for p in processed_text.split("\n\n") if p.strip()]
    
    parent_chunks = []
    current_parent = []
    current_length = 0
    
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
    
    for p_idx, parent_content in enumerate(parent_chunks):
        actual_parent_text = _restore_tables(parent_content, preserved_tables)
        sentences = SENTENCE_END_REGEX.split(parent_content)
        
        current_child = []
        current_child_len = 0

        def _add_child(parts: list[str], suffix: str | None = None) -> None:
            raw = " ".join(parts).strip()
            if raw:
                final_prepared_payloads.append({
                    "text": _restore_tables(raw, preserved_tables),
                    "parent_text": actual_parent_text,
                    "source": source_link,
                    "chunk_hierarchy": f"p{p_idx}-{suffix or f'c{len(final_prepared_payloads)}'}"
                })
        
        for sentence in sentences:
            current_child.append(sentence)
            current_child_len += len(sentence)
            
            if current_child_len >= child_size:
                _add_child(current_child)
                current_child = []
                current_child_len = 0
                
        if current_child:
            _add_child(current_child, "tail")
                
    return final_prepared_payloads

