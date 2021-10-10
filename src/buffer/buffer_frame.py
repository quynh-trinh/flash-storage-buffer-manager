class BufferFrame():
    def __init__(self, frame_id: int, page_size: int):
        self._dirty = False
        self._page_id = -1
        self._frame_id = frame_id
        self._page_size = page_size
        self._data = bytearray(self._page_size)

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
    def data(self) -> bytearray:
        return self._data
    
    @frame_id.setter
    def frame_id(self, data: bool):
        self._data = data