from random import Random
from threading import Lock
from src.buffer.replacement.abstract_replacer import AbstractReplacer
from src.buffer.error import BufferFullError

class RandomReplacer(AbstractReplacer):
    def __init__(self, frame_count):
        super().__init__(frame_count)
        self._unpinned_pages = []
        self._random = Random()
        self._mutex = Lock()

    def pin_page(self, page_id: int):
        self._mutex.acquire()
        if page_id in self._unpinned_pages:
            self._unpinned_pages.remove(page_id)
        self._mutex.release()        

    def unpin_page(self, page_id: int, dirty: bool):
        self._mutex.acquire()
        self._unpinned_pages.append(page_id)
        self._mutex.release()

    def get_victim(self) -> int:
        self._mutex.acquire()
        if len(self._unpinned_pages) == 0:
            raise BufferFullError()
        victim = self._unpinned_pages[self._random.randint(0, len(self._unpinned_pages)-1)]
        self._unpinned_pages.remove(victim)
        self._mutex.release()
        return victim