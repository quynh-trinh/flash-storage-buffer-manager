from src.buffer.buffer_frame import BufferFrame
from src.buffer.replacement.abstract_replacer import AbstractReplacer

class BufferManager():
    def __init__(self, frame_count: int, page_size: int, replacer: AbstractReplacer):
        self._frame_count = frame_count
        self._page_size = page_size
        self._replacer = replacer
        self._frames = [BufferFrame(i, self._page_size) for i in range(self._frame_count)]
    
    def fix_page(self, page_id: int, exclusive: bool):
        pass

    def unfix_page(self, page_id: int, is_dirty: bool):
        pass

    def _lock_frame(self, page_id: int):
        pass

    def _unlock_frame(self, page_id: int):
        pass

    def _read_frame(self, frame_id: int):
        pass

    def _write_frame(self, frame_id: int):
        pass