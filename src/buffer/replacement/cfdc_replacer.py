import os
from threading import Lock
import heapq
from src.util.constants import INVALID_PAGE_ID
from src.buffer.replacement.abstract_replacer import AbstractReplacer


class CFDCReplacer(AbstractReplacer):
    # log_filename = './cfdc_log.txt'

    def __init__(self, page_count, priority_window=0.8, max_cluster_size=2):
        super().__init__(page_count)
        self._mutex = Lock()
        self._buffer_capacity = page_count
        self._PRIORITY_WINDOW = priority_window
        self._psize = int(page_count * self._PRIORITY_WINDOW)
        self._wsize = page_count - self._psize
        self._MAX_CLUSTER_SIZE = max_cluster_size
        self._GLOBALTIME = 1
        self._working_q = []
        self._clean_q = []
        self._dirty_q = []
        self._dirty_unpin = set() # dirty pages in priority region
        self._pages = {} # key=page_id; value=[<is_pinned>, <is_dirty>]
        self._demoted_pin = set() # store pinned pages demoted from working memory
        self._cluster_table = {} # key=cluster-num; value=[<timestamp>, <list-of-pages>]

        # os.remove(CFDCReplacer.log_filename)
        # log = open(CFDCReplacer.log_filename, "x")

    def get_replacer_info(self):
        print(f"Page count = {self._buffer_capacity}\n"
              f"Working region size = {self._wsize}\n"
              f"Priority region size = {self._psize}")

    @property
    def working_q(self):
        return self._working_q

    @property
    def clean_q(self):
        return self._clean_q

    @property
    def dirty_q(self):
        return self._dirty_q

    @property
    def pages(self):
        return self._pages

    @property
    def demoted_pin(self):
        return self._demoted_pin

    @property
    def cluster_table(self):
        return self._cluster_table

    def pin_page(self, page_id: int):
        # log = open(CFDCReplacer.log_filename, 'a')
        # log.write(f'Pin {page_id}\n')
        # log.close()
        self._mutex.acquire()
        if page_id in self._working_q:
            self._working_q.remove(page_id)
        elif page_id in self._demoted_pin:
            self._demoted_pin.remove(page_id)
        elif page_id in self._clean_q:
            self._clean_q.remove(page_id)
        elif page_id in self._dirty_unpin:
            self._dirty_unpin.remove(page_id)
            cnum = page_id // self._MAX_CLUSTER_SIZE
            self._cluster_table[cnum][1].remove(page_id)
            cluster_removed = False
            if len(self._cluster_table[cnum][1]) == 0:
                self._cluster_table.pop(cnum)
                cluster_removed = True
            for e in self._dirty_q:
                if e[1] == cnum:
                    tmp = e
                    self._dirty_q.remove(e)
                    if self._cluster_table[cnum][1] != []:
                        tmp[0] = 0
                        heapq.heapify(self._dirty_q)
                        heapq.heappush(self._dirty_q, tmp)
                    else:
                        self._cluster_table.pop(cnum)
                    break
        if len(self._working_q) == self._wsize:
            demoted = self.find_page_to_demote()
            if demoted == INVALID_PAGE_ID: # all pages in working queue are pinned
                self._demoted_pin.add(self._working_q.pop(0))
            else: # an unpinned page needs to be removed from working queue
                self.demote(demoted, self._pages[demoted][1])
                self._working_q.remove(demoted)
                if (self._pages[demoted][1]):
                    self._dirty_unpin.add(demoted)
        self._working_q.append(page_id)
        self._pages[page_id] = [True, None] # is_dirty set to None as no info on whether a pinned page is clear or dirty
        self._mutex.release()

    def unpin_page(self, page_id: int, dirty: bool):
        """
        Assuming that pinned pages are in either working queue or demoted_pinned
        1. Page to be unpinned is in working queue, simply unpin it.
        2. Page to be unpinned was previously demoted, unpin it and move it clean/dirty queue accordingly.
        """
        self._mutex.acquire()
        if page_id not in self._pages:
            self._mutex.release()
            raise ValueError(f"Error: Page {page_id} is not found. Failed to unpin.")
        # log = open(CFDCReplacer.log_filename, 'a')
        # log.write(f'Unpin {page_id}\n')
        if page_id in self._demoted_pin:
            self._demoted_pin.remove(page_id)
            if dirty:
                self._dirty_unpin.add(page_id)
                self.demote(page_id, dirty)
            else:
                self._clean_q.append(page_id)
        self._pages[page_id] = [False, dirty]
        # log.close()
        self._mutex.release()

    def get_victim(self) -> int:
        """
        Find and remove evicted page
        """
        # log = open(CFDCReplacer.log_filename, 'a')
        self._mutex.acquire()
        victim = INVALID_PAGE_ID
        if self._clean_q != []:
            victim = self._clean_q.pop(0)
            # log.write(f'Get victim {victim} from clean queue.\n')
        elif self._dirty_q != []:
            c = heapq.heappop(self._dirty_q) # c = [<priority>, <cluster-number>]
            cluster = self._cluster_table[c[1]][1]
            victim = cluster.pop(0)
            # log.write(f'Get victim {victim} from dirty queue.\n')
            if cluster == []: # cluster is now empty -> remove it from the table
                self._cluster_table.pop(c[1])
            else: # cluster still have pages -> set priority to 0 and push back to pq
                c[0] = 0
                heapq.heappush(self._dirty_q, c)
            self._dirty_unpin.remove(victim)
        # take the first clean unpinned page
        # if no such page exists in working queue, take the first dirty unpinned page
        elif self._working_q != []:
            victim = self.find_page_to_demote()
            # log.write(f'Get victim {victim} from working queue.\n')
            if victim != INVALID_PAGE_ID:
                self._working_q.remove(victim)
                self._pages.pop(victim)
        self._mutex.release()
        # log.close()
        return victim

    def find_page_to_demote(self) -> int:
        index = -1
        for i, p in enumerate(self._working_q):
            if not self._pages[p][0] and index < 0:  # first unpinned page is found
                index = i
                if not self._pages[p][1]:  # clean
                    break
            if not self._pages[p][0] and not self._pages[p][1]:  # if page is unpinned and clean
                index = i
                break
        demoted_page = INVALID_PAGE_ID
        if index >= 0:
            demoted_page = self._working_q[index]
            if self._pages[demoted_page][1]:  # if evicted page is dirty
                self._GLOBALTIME += 1
        return demoted_page

    # move a page from working region to priority region
    def demote(self, page_id: int, dirty: bool):
        if dirty:
            cnum = page_id // self._MAX_CLUSTER_SIZE
            # assuming that priority = timestamp if there is only 1 page in cluster
            if cnum not in self._cluster_table:
                timestamp = self._GLOBALTIME
                priority = timestamp
                self._cluster_table[cnum] = [timestamp, [page_id, ]]
                heapq.heappush(self._dirty_q, [priority, cnum])
            else:
                timestamp = self._cluster_table[cnum][0]
                self._cluster_table[cnum][1].append(page_id)
                cluster = self._cluster_table[cnum][1]
                s = 0
                for i in range(1, len(cluster)):
                    s += abs(cluster[i] - cluster[i - 1])
                priority = s / (len(cluster) ** 2 * (self._GLOBALTIME - timestamp))
                for e in self._dirty_q:
                    if e[1] == cnum:
                        e[0] = priority
                        break
                heapq.heapify(self._dirty_q)
        else:
            self._clean_q.append(page_id)


