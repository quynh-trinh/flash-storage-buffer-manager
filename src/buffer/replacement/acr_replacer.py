from threading import Lock
from src.util.constants import INVALID_PAGE_ID
from src.buffer.replacement.abstract_replacer import AbstractReplacer


class ACRReplacer(AbstractReplacer):

    def __init__(self, page_count):
        super().__init__(page_count)
        self._mutex = Lock()
        self._buffer_size = page_count

        self._cases = {}
        self._LC = {} # keep track of clean pages and hit counts
        self._LCT = [] # frequently-requested clean pages
        self._LCB = [] # once-requested clean pages

        self._LD = {}  # keep track of dirty pages and hit counts
        self._LDT = [] # frequently-requested dirty pages
        self._LDB = [] # once-requested dirty pages

        self._LCH = []
        self._LDH = []

        self._Cr = 1
        self._Cw = 2
        self._RLC = 0
        self._RLD = 0
        self._MLC = 0
        self._MLD = 0
        self._delta_C = 0
        self._delta_D = 0

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
        # print(f"in pin_page: {self._cases}")
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
        elif page_id in self._LCH or page_id in self._LDH: # buffer miss
            self._cases[page_id] = 2
            if page_id in self._LCH:
                self._delta_C = min(len(self._LC), self._delta_C + 1)
            else:
                self._delta_D = min(len(self._LD), self._delta_D + 1)
        else: # buffer miss
            self._cases[page_id] = 3
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
                self.temp_remove_page(self._LC, page_id)
                self._LCT.append(page_id)
            elif page_id in self._LD:
                self.temp_remove_page(self._LD, page_id)
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
            self.adjust_bottom_portion_list()
        elif self._cases[page_id] == 2:
            if not dirty:
                self._LCT.append(page_id)
                self._LC[page_id] = 0
                self._MLC =+ 1
                self._RLC =+ 1
            else:
                self._LDT.append(page_id)
                self._LD[page_id] = 0
                self._RLD =+ 1
            self.adjust_bottom_portion_list()
        else:
            if not dirty:
                self._LCB.append(page_id)
                self._MLC =+ 1
                self._RLC =+ 1
                self._LC[page_id] = 0
            else:
                self._LDB.append(page_id)
                self._RLD =+ 1
                self._LD[page_id] = 0
            self.adjust_bottom_portion_list()
        self._mutex.release()

    def get_victim(self) -> int:
        # print(f"in get_victim: {self._cases}")
        self._mutex.acquire()
        CLC = self._RLC * self._Cr
        CLD = self._RLD * (self._Cr + self._Cw)
        if CLC + CLD == 0:
            beta = 0
        else:
            beta = CLC / (CLC + CLD)
        victim = INVALID_PAGE_ID
        if len(self._LC) < beta * self._buffer_size: # LD is longer than expected
            self._MLD =+ 1
            if self._LDB != []:
                victim = self._LDB.pop(0)
            if victim in self._LD and self._LD[victim] > 0:
                self._LD.pop(victim)
                self._mutex.release()
                return victim
            if len(self._LCH) + len(self._LDH) == self._buffer_size // 2:
                self._LDH.pop(0)
            self._LDH.append(victim)
        else : # LC is longer than expected
            if self._LCB != []:
                victim = self._LCB.pop(0)
            if victim in self._LC and self._LC[victim] > 0:
                self._LC.pop(victim)
                self._mutex.release()
                return victim
            if len(self._LCH) + len(self._LDH) == self._buffer_size // 2:
                self._LCH.pop(0)
            self._LCH.append(victim)
        self._mutex.release()
        return victim

    def adjust_bottom_portion_list(self):
        if len(self._LC) + len(self._LD) == self._buffer_size:
            while len(self._LCB) != self._delta_C:
                # print(f"delta_c={self._delta_C}")
                # print(f"LCB={len(self._LCB)}")
                if len(self._LCB) < self._delta_C:
                    self._LCB.append(self._LCT.pop(0))
                else:
                    self._LCT.append(self._LCB.pop(0))
            while len(self._LDB) != self._delta_D:
                # print(f"delta_d={self._delta_D}")
                # print(f"LDB={len(self._LDB)}")
                if len(self._LDB) < self._delta_D:
                    self._LDB.append(self._LDT.pop(0))
                else:
                    self._LDT.append(self._LDB.pop(0))
            # print('Result:')
            # print(f"delta_c={self._delta_C}")
            # print(f"LCB={len(self._LCB)}")
            # print('---')
            # print(f"delta_d={self._delta_D}")
            # print(f"LDB={len(self._LDB)}")
            # print('------------')
        else:
            self._delta_C = len(self._LCB)
            self._delta_D = len(self._LDB)

    def temp_remove_page(self, ds, page_id):
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

