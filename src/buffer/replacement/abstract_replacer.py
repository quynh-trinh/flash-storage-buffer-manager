from abc import ABC, abstractmethod

class AbstractReplacer(ABC):
    """
    Arguments
        page_count: The number of pages in the buffer pool
    """
    @abstractmethod
    def __init__(self, page_count):
        self._page_count = page_count

    """Notify the replacer that page_id has been pinned in the buffer pool,
    and therefore should not be considered for replacement until it has
    been unpinned.
    """
    @abstractmethod
    def pin_page(self, page_id: int):
        pass

    """Notify the replacer that page_id has been unpinned in the buffer pool,
    and therefore can now be considered for replacement.
    """
    @abstractmethod
    def unpin_page(self, page_id: int, dirty: bool):
        pass

    """Get the next victim page.
    """
    @abstractmethod
    def get_victim(self) -> int:
        pass

