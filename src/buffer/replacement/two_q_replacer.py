from threading import Lock
from src.buffer.replacement.abstract_replacer import AbstractReplacer
from src.buffer.error import BufferFullError


class TwoQReplacer(AbstractReplacer):
    def __init__(self, frame_count):
        super().__init__(frame_count)
        self._mutex = Lock()
        self._fifo_queue = []
        self._lru_queue = []
        self._frames = {} # key=frame_id; value=[<in_fifo>, <isPinned>]

    @property
    def fifo_q(self):
        return self._fifo_queue

    @property
    def lru_q(self):
        return self._lru_queue

    def pin_page(self, frame_id: int):
        self._mutex.acquire()
        if frame_id not in self._frames: # frame has not been used
            self._frames[frame_id] = [True, True]
            self._fifo_queue.append(frame_id)
        else:
            if self._frames[frame_id][0] is True: # frame is in fifo
                self._fifo_queue.remove(frame_id)
                self._lru_queue.append(frame_id)
            self._frames[frame_id] = [False, True] # frame is in lru and is being pinned
        self._mutex.release()

    def unpin_page(self, frame_id: int):
        self._mutex.acquire()
        if frame_id not in self._frames:
            self._mutex.release()
            raise ValueError(f"Error: Frame with ID {frame_id} is not found. Unpin could not be done.")
        self._frames[frame_id][1] = False
        self._mutex.release()

    def get_victim(self) -> int:
        self._mutex.acquire()
        victim = -1
        for i, f in enumerate(self._fifo_queue):
            if self._frames[f][1] is False:
                victim = f
                break
        if victim < 0:
            for i, f in enumerate(self._lru_queue):
                if self._frames[f][1] is False:
                    victim = f
                    break
        self._mutex.release()
        return victim
