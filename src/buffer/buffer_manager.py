from threading import Lock
from typing import Tuple
import time

from src.buffer.metric_collector import MetricCollector, Metric
from src.buffer.error import BufferFullError
from src.buffer.buffer_frame import BufferFrame
from src.buffer.replacement.abstract_replacer import AbstractReplacer
from src.buffer.file_manager import FileManager
from src.util.constants import INVALID_FRAME_ID
from src.util.AtomicInteger import AtomicInteger
from src.util.ReaderWriterLock import ReaderWriterLock
from src.util.page_id_utils import get_segment_id, get_segment_page_id

class BufferManager():
    def __init__(self,
                 frame_count: int,
                 page_size: int,
                 replacer: AbstractReplacer,
                 file_manager: FileManager,
                 metric_collector: MetricCollector):
        self._frame_count = frame_count
        self._page_size = page_size
        self._replacer = replacer
        self._file_manager = file_manager
        self._metric_collector = metric_collector

        self._frames_lock = Lock()
        self._frames = []
        for i in range(self._frame_count):
            self._frames.append(BufferFrame(i, self._page_size))
        self._page_to_frame = dict()
        self._next_unused_frame = 0

        self._lock_table = [ReaderWriterLock() for i in range(self._frame_count)]        
        self._use_counters = [AtomicInteger() for i in range(self._frame_count)]

        self._pending_writes_lock = Lock()
        self._pending_writes = set()

    
    def __del__(self):
        for frame in self._frames:
            if frame.dirty:
                self._write_frame(frame)
    
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
    def _find_frame_to_use(self, page_id: int) -> Tuple[int, BufferFrame, bool]:
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
                victim_page_id = self._replacer.get_victim()
                frame_id = self._page_to_frame[victim_page_id]
                frame_to_evict = self._frames[frame_id].move()
                del(self._page_to_frame[frame_to_evict.page_id])
                if frame_to_evict.dirty:
                    self._add_pending_write(frame_to_evict.page_id)

            # If no usable frame found
            if frame_id == INVALID_FRAME_ID:
                raise BufferFullError()

            self._use_counters[frame_id].inc()
            self._page_to_frame[page_id] = frame_id
            self._replacer.pin_page(page_id)

            # If page is new to the pool, update frame metadata
            if not found_existing:
                self._frames[frame_id].dirty = False
                self._frames[frame_id].page_id = page_id
                # Lock frame in exclusive mode for reading
                self._lock_frame(frame_id, True)

            return frame_id, frame_to_evict, found_existing
    
    """ Returns true if it is safe to fix a page.
        Not thread safe.
    """
    def safe_to_fix_page(self, page_id: int, exclusive: bool) -> bool:
        if exclusive \
           and page_id in self._page_to_frame\
           and self._use_counters[self._page_to_frame[page_id]].value > 0:
                return False
        if not exclusive \
           and page_id in self._page_to_frame \
           and self._use_counters[self._page_to_frame[page_id]].value > 0 \
           and self._frames[self._page_to_frame[page_id]].exclusive:
                return False
        return True

    def fix_page(self, page_id: int, exclusive: bool, is_prefetch=False) -> BufferFrame:
        frame_id, frame_to_evict, found_existing = self._find_frame_to_use(page_id)

        if not is_prefetch:
            self._metric_collector.increment(Metric.BUFFER_MANAGER_ACCESSES)
            if found_existing:
                self._metric_collector.increment(Metric.BUFFER_MANAGER_HITS)
            else:
                self._metric_collector.increment(Metric.BUFFER_MANAGER_MISSES)

        if frame_to_evict != None:
            self._metric_collector.increment(Metric.BUFFER_MANAGER_EVICTIONS)
            if frame_to_evict.dirty:
                self._metric_collector.increment(Metric.BUFFER_MANAGER_DIRTY_EVICTIONS)
                self._write_frame(frame_to_evict)
                self._remove_pending_write(frame_to_evict.page_id)
            else:
                self._metric_collector.increment(Metric.BUFFER_MANAGER_CLEAN_EVICTIONS)
        if not found_existing:
            self._read_frame(frame_id)
            self._unlock_frame(frame_id)
        
        if not is_prefetch:
            self._lock_frame(frame_id, exclusive)
        return self._frames[frame_id]

    def unfix_page(self, frame: BufferFrame, is_dirty: bool):
        frame.dirty = frame.dirty or is_dirty
        
        self._unlock_frame(frame.frame_id)
        counter_val = self._use_counters[frame.frame_id].dec()
        if counter_val == 0:
            self._replacer.unpin_page(frame.page_id, is_dirty)

    def _lock_frame(self, frame_id: int, exclusive: bool):
        if exclusive:
            self._lock_table[frame_id].lock_exclusive()
            self._frames[frame_id].exclusive = True
        else:
            self._lock_table[frame_id].lock_shared()
            self._frames[frame_id].exclusive = False

    def _unlock_frame(self, frame_id: int):
        if self._frames[frame_id].exclusive:
            self._lock_table[frame_id].release_exclusive()
        else:
            self._lock_table[frame_id].release_shared()

    def _read_frame(self, frame_id: int):
        page_id = self._frames[frame_id].page_id
        segment_id = get_segment_id(page_id)
        segment_page_id = get_segment_page_id(page_id)

        self._file_manager.read_block(str(segment_id), segment_page_id, self._frames[frame_id].data)

    def _write_frame(self, frame: BufferFrame):
        page_id = frame.page_id
        segment_id = get_segment_id(page_id)
        segment_page_id = get_segment_page_id(page_id)

        self._file_manager.write_block(str(segment_id), segment_page_id, frame.data)