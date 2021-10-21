def make_page_id(segment_id: int, page_id: int) -> int:
    return (segment_id << 48) | page_id

def get_segment_id(page_id: int) -> int:
    return page_id >> 48

def get_segment_page_id(page_id: int) -> int:
    return page_id & ((1 << 48) - 1)