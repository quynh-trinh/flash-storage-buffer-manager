import os
from threading import Lock
import heapq
from src.util.constants import INVALID_PAGE_ID
from src.buffer.replacement.abstract_replacer import AbstractReplacer


class ACRReplacer(AbstractReplacer):

    def __init__(self, page_count):
        super().__init__(page_count)
        self._mutex = Lock()
        self._buffer_size = page_count

        self._cases = {}
        self._LC = {} # keep track of clean pages;
        self._LCT = [] # frequently-requested clean pages
        self._LCB = [] # once-requested clean pages

        self._LD = {}  # keep track of dirty pages
        self._LDT = [] # frequently-requested dirty pages
        self._LDB = [] # once-requested dirty pages

        self._LCH = []
        self._LDH = []

        self._CLC = 0
        self._CLD = 0
        self._RLC = 0
        self._RLD = 0
        self._MLC = 0
        self._MLD = 0
        self._delta_C = 0
        self._delta_D = 0


    # def get_replacer_info(self):
    #     print(f"Page count = {self._buffer_capacity}\n"
    #           f"Working region size = {self._wsize}\n"
    #           f"Priority region size = {self._psize}")

    # @property
    # def working_q(self):
    #     return self._working_q
    #
    # @property
    # def clean_q(self):
    #     return self._clean_q
    #
    # @property
    # def dirty_q(self):
    #     return self._dirty_q
    #
    # @property
    # def pages(self):
    #     return self._pages
    #
    # @property
    # def demoted_pin(self):
    #     return self._demoted_pin
    #
    # @property
    # def cluster_table(self):
    #     return self._cluster_table

    def pin_page(self, page_id: int):
        self._mutex.acquire()
        if page_id in self._LC or page_id in self._LD: # buffer hit
            self._cases[page_id] = 1
            if page_id in self._LC:
                self._RLC += 1
                if page_id in self._LCB:
                    self._delta_C = max(0, self._delta_C - 1)
                else:
                    self._RLD += 1
                    if page_id in self._LDB:
                        self._delta_D = max(0, self._delta_D - 1)
            # self.adjust_bottom_portion_list()
        elif page_id in self._LCH or page_id in self._LDH: # buffer miss
            self._cases[page_id] = 2
            if page_id in self._LCH:
                self._delta_C = min(len(self._LC), self._delta_C + 1)
            else:
                self._delta_D = min(len(self._LD), self._delta_D + 1)
            # self.adjust_bottom_portion_list()
        else: # buffer miss
            self._cases[page_id] = 3
            # self.adjust_bottom_portion_list()
        self._mutex.release()

    def unpin_page(self, page_id: int, dirty: bool):
        """
        Assuming that pinned pages are in either working queue or demoted_pinned
        1. Page to be unpinned is in working queue, simply unpin it.
        2. Page to be unpinned was previously demoted, unpin it and move it clean/dirty queue accordingly.
        """
        self._mutex.acquire()
        if self._cases[page_id] == 1:
            added_to_LD = False
            if page_id in self._LC and not dirty:
                self.adjust_queue(self._LC, page_id)
                self._LCT.append(page_id)
            elif page_id in self._LD:
                self.adjust_queue(self._LD, page_id)
                self._LDT.append(page_id)
                added_to_LD = True
            else:
                self._LDB.append(page_id)
                added_to_LD = True
            if page_id in self._LC:
                if added_to_LD:
                    self._LC.pop(page_id)
                    self._LD[page_id] = 0
                else:
                    self._LC[page_id] += 1
            else:
                self._LD[page_id] += 1
        elif self._cases[page_id] == 2:
            if not dirty:
                self._LCT.append(page_id)
                self._MLC =+ 1
                self._RLC =+ 1
            else:
                self._LDT.append(page_id)
                self._RLD =+ 1
        else:
            if not dirty:
                self._LCB.append(page_id)
                self._MLC =+ 1
                self._RLC =+ 1
            else:
                self._LDB.append(page_id)
                self._RLD =+ 1
        self._mutex.release()

    def get_victim(self) -> int:
        """
        Find and remove evicted page
        """
        # log = open(CFDCReplacer.log_filename, 'a')
        self._mutex.acquire()
        beta = self._CLC / (self._CLC + self._CLD)
        victim = INVALID_PAGE_ID
        if len(self._LC) < beta * self._buffer_size: # LD is longer than expected
            self._MLD =+ 1
            if self._LDB != []:
                victim = self._LDB.pop(0)
            if self._LD[victim] > 0:
                self._mutex.release()
                return victim
            if len(self._LCH) + len(self._LDH) == self._buffer_size // 2:
                self._LDH.pop(0)
            self._LCH.append(victim)
        else : # LC is longer than expected
            if self._LCB != []:
                victim = self._LCB.pop(0)
            if self._LC[victim] > 0:
                self._mutex.release()
                return victim
            if len(self._LCH) + len(self._LDH) == self._buffer_size // 2:
                self._LCH.pop(0)
            self._LCH.append(victim)
        if victim != -1:
            self._cases.pop(victim)
        self._mutex.release()
        # log.close()
        return victim

    def adjust_bottom_portion_list(self):
        if len(self._LC) + len(self._LD) == self._buffer_size:
            pass
        else:
            self._delta_C = len(self._LCB)
            self._delta_D = len(self._LDB)

    def adjust_queue(self, ds, page_id):
        found_page = False
        if ds is self._LC:
            for p in self._LCT:
                if p == page_id:
                    self._LCT.remove(p)
                    found_page = True
                    break
            if not found_page:
                for p in self._LCB:
                    if p == page_id:
                        self._LCB.remove(p)
                        break
        else:
            for p in self._LDT:
                if p == page_id:
                    self._LDT.remove(p)
                    found_page = True
                    break
            if not found_page:
                for p in self._LDB:
                    if p == page_id:
                        self._LDB.remove(p)
                        break

