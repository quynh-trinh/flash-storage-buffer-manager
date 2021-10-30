from threading import Lock
import heapq
from src.util.constants import INVALID_PAGE_ID
from src.buffer.replacement.abstract_replacer import AbstractReplacer

class CFDCReplacer(AbstractReplacer):
    def __init__(self, frame_count):
        super().__init__(frame_count)
        self._mutex = Lock()
        self._buffer_capacity = frame_count
        self._PRIORITY_WINDOW = 0.6
        self._psize = int(frame_count * self._PRIORITY_WINDOW)
        self._wsize = frame_count - self._psize
        self._MAX_CLUSTER_SIZE = max(4, self._wsize / 4)
        self._GLOBALTIME = 1
        self._working_q = []
        self._clean_q = []
        self._dirty_q = []
        self._pages = {} # key=page_id; value=[<is_pinned>, <is_dirty>]
        self._demoted_pin = set() # store pinned pages demoted from working memory
        self._cluster_table = {} # key=cluster-num; value=[<timestamp>, <list-of-pages>]

    @property
    def working_q(self):
        return self._working_q

    @property
    def clean_q(self):
        return self._clean_q

    @property
    def dirty_q(self):
        return self._dirty_q

    def pin_page(self, page_id: int):
        """
        1. Page in working queue -> adjust its position within working queue
        2. Page is new to the pool or in self._demoted_pin
            a. working queue is not full -> add page to working queue
            b. working queue is full -> demote
                First try to demote an unpinned page (move it to priority region)
                Otherwise, demote a pinned page (move it to self._demoted_pin)
        3. Page is in priority region
        """
        self._mutex.acquire()
        # if self.get_buffer_size() == self._buffer_capacity and page_id not in self._pages:
        #     self._mutex.release()
        #     raise ValueError()
        if page_id in self._working_q:
            self._working_q.remove(page_id)
        elif page_id not in self._pages or page_id in self._demoted_pin:
            if page_id in self._demoted_pin:
                self._demoted_pin.remove(page_id)
            if len(self._working_q) == self._psize:
                demoted = INVALID_PAGE_ID
                for i, p in enumerate(self._working_q):
                    if not self._pages[p][1]:
                        demoted = self._working_q.pop(i)
                        # add demoted page to priority region
                if demoted == INVALID_PAGE_ID:
                    self._demoted_pin.add(self._working_q.pop(0))
        else:
            if self._pages[page_id][1]: # if dirty
                cnum = page_id // self._MAX_CLUSTER_SIZE
                cluster = self._cluster_table[cnum][1]
                cluster.remove(page_id)
                for e in self._dirty_q:
                    if e[1] == cnum:
                        e[0] = 0
                        break
                heapq.heapify(self._dirty_q)
            else:
                self._clean_q.remove(page_id)
        self._working_q.append(page_id)
        self._mutex.release()

    def unpin_page(self, page_id: int, dirty: bool):
        """
        Assuming that unpinned pages are in either working queue or demoted_pinned
        1. Page to be unpinned is in working queue, simply unpin it.
        2. Page to be unpinned was previously demoted, unpin it and move it clean/dirty queue accordingly.
        """
        self._mutex.acquire()
        if page_id not in self._pages:
            self._mutex.release()
            raise ValueError(f"Error: Page {page_id} is not found. Failed to unpin.")
        if page_id in self._demoted_pin:
            self._demoted_pin.remove(page_id)
            if dirty:
                cnum = page_id // self._MAX_CLUSTER_SIZE
                # assuming that priority = timestamp if there is only 1 page in cluster
                if cnum not in self._cluster_table:
                    timestamp = self._GLOBALTIME
                    priority = timestamp
                    self._cluster_table[cnum] = [timestamp, [page_id,]]
                    heapq.heappush(self._dirty_q,[priority, cnum])
                else:
                    timestamp = self._cluster_table[cnum][0]
                    self._cluster_table[cnum][1] =+ page_id
                    cluster = self._cluster_table[cnum][1]
                    s = 0
                    for i in range(1, len(cluster)):
                        s += abs(cluster[i] - cluster[i-1])
                    priority = s / (len(cluster)**2 * (self._GLOBALTIME-timestamp))
                    for e in self._dirty_q:
                        if e[1] == cnum:
                            e[0] = priority
                            break
                    heapq.heapify(self._dirty_q)
            else:
                self._clean_q.append(page_id)
        self._pages[page_id] = [False, dirty]
        self._mutex.release()

    def get_victim(self) -> int: # find and remove evicted page
        self._mutex.acquire()
        victim = INVALID_PAGE_ID
        if not self._clean_q:
            victim = self._clean_q.pop(0)
        elif not self._dirty_q:
            c = heapq.heappop(self._dirty_q) # c = [<priority>, <cluster-number>]
            cluster = self._cluster_table[c[1]][1]
            victim = cluster.pop(0)
            if not cluster: # cluster is now empty -> remove it from the table
                self._cluster_table.pop(c[1])
            else: # cluster still have pages -> set priority to 0 and push back to pq
                c[0] = 0
                heapq.heappush(self._dirty_q, c)
        # take the first clean unpinned page
        # if no such page exists in working queue, take the first dirty unpinned page
        elif not self._working_q:
            index = -1
            dirty_evicted = False
            for i, p in enumerate(self._working_q):
                if not self._pages[p][0] and index < 0: # first unpinned page is found
                    index = i
                    if not self._pages[p][1]: # clean
                        dirty_evicted = False
                        break
                    else:
                        dirty_evicted = True
                if not self._pages[p][0] and not self._pages[p][1]: # if page is unpinned and clean
                    index = i
                    dirty_evicted = False
                    break
            if index >= 0:
                victim = self._working_q.pop(index)
                if dirty_evicted:
                    self._GLOBALTIME += 1
        if victim != INVALID_PAGE_ID:
            self._pages.pop(victim)
        self._mutex.release()
        return victim

    def demote(self):
        pass

    def get_buffer_size(self):
        return self._working_q + self._demoted_pin + self._clean_q + self._dirty_q

