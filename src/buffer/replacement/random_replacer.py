from random import Random
from threading import Lock
from src.buffer.replacement.abstract_replacer import AbstractReplacer

class RandomReplacer(AbstractReplacer):
    def __init__(self, frame_count):
        super().__init__(frame_count)
        self._free_frames = [i for i in range(0, self._frame_count)]
        self._random = Random()
        self._mutex = Lock()

    def pin_page(self, frame_id: int):
        self._mutex.acquire()
        self._free_frames.remove(frame_id)
        self._mutex.release()        

    def unpin_page(self, frame_id: int):
        self._mutex.acquire()
        self._free_frames.append(frame_id)
        self._mutex.release()

    def get_victim(self) -> int:
        self._mutex.acquire()
        victim = self._free_frames[self._random.randint(0, len(self._free_frames)-1)]
        self._mutex.release()
        return victim