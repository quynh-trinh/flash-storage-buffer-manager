from threading import Lock
from src.buffer.error import BufferFullError
from src.buffer.replacement.abstract_replacer import AbstractReplacer
from src.util.constants import INVALID_PAGE_ID


class TwoQReplacer(AbstractReplacer):
    def __init__(self, page_count):
        super().__init__(page_count)
        self._mutex = Lock()
        self._fifo_queue = []
        self._lru_queue = []
        self._pages = {} # key=page_id; value=[<in_fifo>, <isPinned>]

    @property
    def fifo_q(self):
        return self._fifo_queue

    @property
    def lru_q(self):
        return self._lru_queue

    def pin_page(self, page_id: int):
        self._mutex.acquire()
        if page_id not in self._pages: # page has not been used
            self._pages[page_id] = [True, True]
            self._fifo_queue.append(page_id)
        else:
            if self._pages[page_id][0]: # page is in fifo
                self._fifo_queue.remove(page_id)
            else:
                self._lru_queue.remove(page_id)
            self._lru_queue.append(page_id)
            self._pages[page_id] = [False, True] # page is in lru and is being pinned
        self._mutex.release()

    def unpin_page(self, page_id: int, dirty=False):
        self._mutex.acquire()
        if page_id not in self._pages:
            self._mutex.release()
            raise ValueError(f"Error: Page {page_id} is not found. Failed to unpin.")
        self._pages[page_id][1] = False
        self._mutex.release()

    def get_victim(self) -> int:
        self._mutex.acquire()
        victim = INVALID_PAGE_ID
        for p in self._fifo_queue:
            if self._pages[p][1] is False:
                victim = p
                self._fifo_queue.remove(p)
                break
        if victim < 0:
            for p in self._lru_queue:
                if self._pages[p][1] is False:
                    victim = p
                    self._lru_queue.remove(p)
                    break
        if victim != INVALID_PAGE_ID:
            self._pages.pop(victim)
        else:
            self._mutex.release()
            raise BufferFullError
        self._mutex.release()
        return victim
