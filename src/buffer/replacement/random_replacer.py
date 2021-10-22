from random import Random
from threading import Lock
from src.buffer.replacement.abstract_replacer import AbstractReplacer
from src.buffer.error import BufferFullError

class RandomReplacer(AbstractReplacer):
    def __init__(self, frame_count):
        super().__init__(frame_count)
        self._unpinned_frames = [i for i in range(0, self._frame_count)]
        self._random = Random()
        self._mutex = Lock()

    def pin_page(self, frame_id: int):
        self._mutex.acquire()
        if frame_id in self._unpinned_frames:
            self._unpinned_frames.remove(frame_id)
        self._mutex.release()        

    def unpin_page(self, frame_id: int):
        self._mutex.acquire()
        self._unpinned_frames.append(frame_id)
        self._mutex.release()

    def get_victim(self) -> int:
        self._mutex.acquire()
        if len(self._unpinned_frames) == 0:
            raise BufferFullError
        victim = self._unpinned_frames[self._random.randint(0, len(self._unpinned_frames)-1)]
        self._mutex.release()
        return victim