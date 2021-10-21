from __future__ import annotations
import mmap

class BufferFrame():
    def __init__(self, frame_id: int, page_size: int):
        self._dirty = False
        self._page_id = -1
        self._frame_id = frame_id
        self._page_size = page_size
        self._exclusive = False
        self._data = mmap.mmap(-1, self._page_size)
    
    def __del__(self):
        self._data.close()

    @property
    def dirty(self) -> bool:
        return self._dirty
    
    @dirty.setter
    def dirty(self, is_dirty: bool):
        self._dirty = is_dirty

    @property
    def page_id(self) -> int:
        return self._page_id
    
    @page_id.setter
    def page_id(self, page_id: bool):
        self._page_id = page_id

    @property
    def frame_id(self) -> int:
        return self._frame_id
    
    @frame_id.setter
    def frame_id(self, frame_id: bool):
        self._frame_id = frame_id
    
    @property
    def exclusive(self) -> bool:
        return self._exclusive
    
    @exclusive.setter
    def exclusive(self, is_exclusive: bool):
        self._exclusive = is_exclusive

    @property
    def data(self) -> mmap:
        return self._data
    
    @data.setter
    def data(self, data: mmap):
        self._data = data
    
    def move(self) -> BufferFrame:
        copy = BufferFrame(self._frame_id, self._page_size)
        copy.dirty = self._dirty
        copy.page_id = self._page_id

        copy.data = self._data
        new_data = mmap.mmap(-1, self._page_size)
        self._data = new_data
        return copy