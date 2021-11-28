from random import Random
from threading import Lock
from collections import OrderedDict
from src.buffer.replacement.abstract_replacer import AbstractReplacer
from src.buffer.error import BufferFullError
from src.util.constants import INVALID_PAGE_ID

class LRUReplacer(AbstractReplacer):
    def __init__(self, frame_count):
        super().__init__(frame_count)
        self._unpinned_pages = set()
        # self._lru_queue = []
        self._lru_queue = OrderedDict()
        self._mutex = Lock()

    def pin_page(self, page_id: int):
        self._mutex.acquire()
        if page_id in self._unpinned_pages:
            self._unpinned_pages.remove(page_id)
        if page_id in self._lru_queue:
            # self._lru_queue.remove(page_id)
            self._lru_queue.move_to_end(page_id)
        else:
            self._lru_queue[page_id] = None
        self._mutex.release()        

    def unpin_page(self, page_id: int, dirty: bool = False):
        self._mutex.acquire()
        self._unpinned_pages.add(page_id)
        self._mutex.release()

    def get_victim(self) -> int:
        self._mutex.acquire()
        if len(self._unpinned_pages) == 0:
            raise BufferFullError()
        victim = INVALID_PAGE_ID
        for page in self._lru_queue:
            if page in self._unpinned_pages:
                victim = page
                self._unpinned_pages.remove(victim)
                self._lru_queue.pop(victim)
                break
        self._mutex.release()
        return victim