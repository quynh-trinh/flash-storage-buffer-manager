from threading import Lock
from typing import Tuple
import time
from readerwriterlock import rwlock
from src.buffer.error import BufferFullError
from src.buffer.buffer_frame import BufferFrame
from src.buffer.replacement.abstract_replacer import AbstractReplacer
from src.util.constants import INVALID_FRAME_ID
from src.util.AtomicInteger import AtomicInteger

class BufferManager():
    def __init__(self, frame_count: int, page_size: int, replacer: AbstractReplacer):
        self._frame_count = frame_count
        self._page_size = page_size
        self._replacer = replacer

        self._frames_lock = Lock()
        self._frames = [BufferFrame(i, self._page_size) for i in range(self._frame_count)]
        self._page_to_frame = dict()
        self._next_unused_frame = 0

        self._lock_table = [rwlock.RWLockFair() for i in range(self._frame_count)]        
        self._use_counters = [AtomicInteger() for i in range(self._frame_count)]

        self._pending_writes_lock = Lock()
        self._pending_writes = set()

    
    def __del__(self):
        for frame in self._frames:
            if frame.dirty():
                self._write_frame(frame.frame_id)
    
    """ Waits for a pending write to complete, if needed.
    Preconditions: _frames_lock has been acquired by the caller.
    """
    def _wait_for_pending_write(self, page_id: int):
        self._pending_writes_lock.acquire()
        while page_id in self._pending_writes:
            self._pending_writes_lock.release()
            time.sleep(0.0001)
            self._pending_writes_lock.acquire()
        self._pending_writes_lock.release()
    
    """ Adds page_id to list of pending writes
    """
    def _add_pending_write(self, page_id: int):
        with self._pending_writes_lock:
            self._pending_writes.add(page_id)

    """ Removes page_id from list of pending writes
    """
    def _remove_pending_write(self, page_id: int):
        with self._pending_writes_lock:
            self._pending_writes.discard(page_id)
    
    """ Finds a free frame from the frame pool
    Returns the id of the free frame and a copy of the evicted frame,
    if a frame was evicted to make room for the new one.
    """
    def _find_free_frame(self, page_id: int) -> Tuple[int, BufferFrame]:
        frame_id = INVALID_FRAME_ID
        frame_to_evict = None
        found_existing = False
        
        # Latch frames while finding free frame
        with self._frames_lock:
            self._wait_for_pending_write(page_id)
            # Check if page already in pool
            if page_id in self._page_to_frame:
                frame_id = self._page_to_frame[page_id]
                found_existing = True
            # Check for unused frame
            if frame_id == INVALID_FRAME_ID and self._next_unused_frame < self._frame_count:
                frame_id = self._next_unused_frame
                self._next_unused_frame += 1
            # Get victim from replacer if not in pool
            if frame_id == INVALID_FRAME_ID:
                frame_id = self._replacer.get_victim()
                frame_to_evict = self._frames[frame_id].move()
                if frame_to_evict.dirty():
                    self._add_pending_write(frame_to_evict.page_id())

            # If no usable frame found
            if frame_id == INVALID_FRAME_ID:
                raise BufferFullError()

            self._use_counters[frame_id].inc()
            self._replacer.pin_page(frame_id)

            # If page is new to the pool, update frame metadata
            if not found_existing:
                self._frames[frame_id].dirty(False)
                self._frames[frame_id].page_id(page_id)
            # Lock frame in exclusive mode for reading
            self._lock_frame(page_id, True)

            return frame_id, frame_to_evict

    def fix_page(self, page_id: int, exclusive: bool) -> BufferFrame:
        frame_id, frame_to_evict = self._find_free_frame()

        if frame_to_evict != None:
            if frame_to_evict.dirty():
                self._write_frame(frame_to_evict)
                self._remove_pending_write(frame_to_evict.page_id())
        
            self._read_frame(frame_id)
            self._unlock_frame(frame_id)
        
        self._lock_frame(page_id, exclusive)
        return self._frames[frame_id]

    def unfix_page(self, frame: BufferFrame, is_dirty: bool):
        frame.dirty(frame.dirty() or is_dirty)
        
        self._unlock_frame(frame.frame_id())
        self._use_counters[frame.frame_id()].dec()

    def _lock_frame(self, frame_id: int, exclusive: bool):
        if exclusive:
            self._lock_table[frame_id].gen_wlock().acquire()
            self._frames[frame_id].exclusive(True)
        else:
            self._lock_table[frame_id].gen_rlock().acquire()
            self._frames[frame_id].exclusive(False)

    def _unlock_frame(self, frame_id: int):
        if self._frames[frame_id].exclusive():
            self._lock_table[frame_id].gen_wlock().release()
        else:
            self._lock_table[frame_id].gen_rlock().release()

    def _read_frame(self, frame_id: int):
        pass

    def _write_frame(self, frame: BufferFrame):
        pass