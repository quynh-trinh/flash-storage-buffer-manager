from threading import Lock
from src.buffer.replacement.abstract_replacer import AbstractReplacer
from src.buffer.error import BufferFullError

class TwoQReplacer(AbstractReplacer):
    def __init__(self, frame_count):
        super().__init__(frame_count)
        self._mutex = Lock()
        self._pin_dict = {}
        self._fifo_queue = []
        self._lru_queue = {}
        self._free_frame_count = frame_count

    def pin_page(self, frame_id: int):
        """
        if the frame has never been used
          -> add its corresponding values to pin_dict, fifo_queue, and lru_queue
        if the frame is not pinned:
          -> pin the frame and
               move from fifo to lru and initialize the count to 2 in lru_queue
               (if not in fifo) increment the count in lru_queue
        """
        self._mutex.acquire()
        if frame_id not in self._pin_dict:
            self._fifo_queue.append(frame_id)
            self._free_frame_count -= 1
            self._pin_dict[frame_id] = True
            self._lru_queue[frame_id] = 1
        elif self._pin_dict[frame_id] is False:
            self._pin_dict[frame_id] = True
            if frame_id in self._fifo_queue:
                self._fifo_queue.remove(frame_id)
                self._lru_queue[frame_id] = 2
            else:
                self._lru_queue[frame_id] += 1
        self._mutex.release()

    def unpin_page(self, frame_id: int):
        self._mutex.acquire()
        if frame_id in self._lru_queue:
            self._pin_dict[frame_id] = False
            self._lru_queue[frame_id] = max(0, self._lru_queue[frame_id] - 1)
            if self._lru_queue[frame_id] == 0:
                self._free_frame_count += 1
        else:
            raise ValueError(f"Error: Frame with ID {frame_id} is not found. Unpin could not be done.")
        self._mutex.release()

    def get_victim(self) -> int:
        pass
        # self._mutex.acquire()
        # if self._free_frame_count == 0:
        #     raise BufferFullError
        # for id, count in self._lru_queue.items():
        #     if count == 0:
        #         if id in self._fifo_queue:
        #             self._fifo_queue.remove(id)
        #             self._fifo_queue.append(id)
        #
        # else:
        # self._mutex.release()
        # return victim