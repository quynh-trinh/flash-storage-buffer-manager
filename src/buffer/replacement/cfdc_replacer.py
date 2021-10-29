# from threading import Lock
# import heapq
# from src.buffer.replacement.abstract_replacer import AbstractReplacer
# from src.buffer.error import BufferFullError
# from src.buffer.buffer_frame import BufferFrame
#
#
# class CFDCReplacer(AbstractReplacer):
#     def __init__(self, frame_count):
#         super().__init__(frame_count)
#         self._mutex = Lock()
#         self.priority_window = 0.7
#         self._working = []
#         self._clean = []
#         self._dirty = []
#         self._fifo_queue = []
#         self._lru_queue = []
#         self._free_frame_count = frame_count
#         self._counter = {}
#
#     @property
#     def fifo_q(self):
#         return self._fifo_queue
#
#     @property
#     def lru_q(self):
#         return self._lru_queue
#
#     def pin_page(self, frame: BufferFrame):
#         self._mutex.acquire()
#         # when the frame has never been used
#         if frame not in self._counter:
#             self._fifo_queue.append(frame)
#             self._free_frame_count -= 1
#             self._counter[frame] = [0, True]
#         # when the frame is in fifo queue
#         elif frame in self._fifo_queue:
#             self._fifo_queue.remove(frame)
#             self._lru_queue.append(frame)
#         self._counter[frame][0] += 1
#         self._counter[frame][1] = True
#         self._mutex.release()
#
#     def unpin_page(self, frame: int):
#         self._mutex.acquire()
#         if frame in self._counter:
#             self._counter[frame][1] = False
#             self._counter[frame][0] = max(0, self._counter[frame][0] - 1)
#             if self._counter[frame] == 0:
#                 self._free_frame_count += 1
#             self._mutex.release()
#         else:
#             self._mutex.release()
#             raise ValueError(f"Error: Frame with ID {frame} is not found. Unpin could not be done.")
#
#     def get_victim(self) -> int:
#         self._mutex.acquire()
#         victim = -1
#         if self._free_frame_count == 0:
#             self._mutex.release()
#             return victim
#         for i, c in enumerate(self._fifo_queue):
#             if self._counter[c][0] == 0:
#                 victim = c
#                 break
#         if victim < 0:
#             for i, h in enumerate(self._lru_queue):
#                 if self._counter[h][0] == 0:
#                     victim = h
#                     self._lru_queue.remove(victim)
#                     self._fifo_queue.append(victim)
#                     break
#         self._mutex.release()
#         return victim
#
#     def compute_q(self):
#         pass
