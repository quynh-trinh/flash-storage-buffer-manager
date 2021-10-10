from abc import ABC, abstractmethod

class AbstractReplacer(ABC):
    """
    Arguments
        frame_count: The number of frames in the buffer pool
    """
    @abstractmethod
    def __init__(self, frame_count):
        self._frame_count = frame_count

    """Notify the replacer that frame_id has been pinned in the buffer pool,
    and therefore should not be considered for replacement until it has
    been unpinned.
    """
    @abstractmethod
    def pin_page(self, frame_id: int):
        pass

    """Notify the replacer that frame_id has been unpinned in the buffer pool,
    and therefore can now be considered for replacement.
    """
    @abstractmethod
    def unpin_page(self, frame_id: int):
        pass

    """Get the next victim frame. This function should call pin_page
    for thread safety so the same victim won't be returned to two different
    threads.
    """
    @abstractmethod
    def get_victim(self) -> int:
        pass

