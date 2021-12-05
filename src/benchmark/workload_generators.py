from abc import ABC, abstractmethod
from typing import Tuple, Iterator
from enum import Enum
from numpy.random import seed
from numpy.random import randint
from numpy.random import default_rng
import numpy as np


from src.benchmark.abstract_workload_generator \
    import AbstractWorkloadGenerator, WorkloadGeneratorAction, RequestType


class DummyWorkloadGenerator(AbstractWorkloadGenerator):
    def __init__(self):
        super().__init__()

    def get_actions(self) -> Iterator[Tuple[WorkloadGeneratorAction, int, bool]]:
        for i in range(0, 100):
            yield (WorkloadGeneratorAction.FIX_PAGE, i, True)
            yield (WorkloadGeneratorAction.UNFIX_PAGE, i, True)
        return


class OHJWorkloadGenerator(AbstractWorkloadGenerator):
    """
    OHJ = Ou, Harder, Jin (research paper's authors)
    Single page requests are randomly generated between 1 and 100,000 (80-20 distribution)
    Starting page numbers of the scans are uniformly distributed between 1 and 100,000

    """
    def __init__(self):
        super().__init__()
        self.total_pages = 100000
        self.num_single_pages = self.total_pages
        self.num_scans = 100
        self.scan_length = 200
        self.total_calls = (self.num_single_pages + self.num_scans) * 2

        # Testing
        # self.total_pages = 100
        # self.num_single_pages = self.total_pages
        # self.num_scans = 10
        # self.scan_length = 5
        # self.total_calls = (self.num_single_pages + self.num_scans) * 2

        self._random_generator = default_rng(seed=12345)
        # seed(12345)
        # self._random_reads = list(randint(0, self.total_pages, self.num_single_pages))
        self._random_reads = self.generate_self_similar_distribution(0, self.total_pages+1, size=self.num_single_pages)
        self._curr_random_read = 0
        # self._random_writes = list(randint(0, self.total_pages, self.num_single_pages))
        self._random_writes = self.generate_self_similar_distribution(0, self.total_pages+1, size=self.num_single_pages)
        self._curr_random_write = 0

        # self._seq_reads = list(randint(0, self.total_pages - self.scan_length, self.num_scans))
        self._seq_reads = self._random_generator.uniform(0, self.total_pages+1-self.scan_length, size=self.num_scans).astype(np.int64)
        self._curr_seq_read = 0
        # self._seq_writes = list(randint(0, self.total_pages - self.scan_length, self.num_scans))
        self._seq_writes = self._random_generator.uniform(0, self.total_pages+1-self.scan_length, size=self.num_scans).astype(np.int64)
        self._curr_seq_write = 0

        # self._types = randint(1, 5, self.total_calls)
        self._types = []
        self._types += [1] * self.num_single_pages
        self._types += [2] * self.num_single_pages
        self._types += [3] * self.num_scans
        self._types += [4] * self.num_scans
        self._random_generator.shuffle(self._types)
    
    def generate_self_similar_distribution(self, lo, hi, skew=0.2, size=1):
        range = hi - lo
        s = self._random_generator.uniform(0.0, 1.0, size)

        s = (lo + (range * s ** (np.log(skew) / np.log(1 - skew)))).astype(np.int64)
        return s

    def get_actions(self) -> Iterator[Tuple[WorkloadGeneratorAction, int, bool]]:
        for type in self._types:
            if type == 1 and self._curr_random_read != self.num_single_pages - 1: # random read
                page = self._random_reads[self._curr_random_read]
                self._curr_random_read += 1
                yield (WorkloadGeneratorAction.FIX_PAGE, page, False)
                yield (WorkloadGeneratorAction.UNFIX_PAGE, page, False)
            elif type == 2 and self._curr_random_write != self.num_single_pages - 1: # random write
                page = self._random_writes[self._curr_random_write]
                self._curr_random_write += 1
                yield (WorkloadGeneratorAction.FIX_PAGE, page, True)
                yield (WorkloadGeneratorAction.UNFIX_PAGE, page, True)
            elif type == 3 and self._curr_seq_read != self.num_scans - 1: # sequential read
                start_page = self._seq_reads[self._curr_seq_read]
                self._curr_seq_read += 1
                for i in range(0, self.scan_length):
                    yield (WorkloadGeneratorAction.FIX_PAGE, start_page + i, False)
                    yield (WorkloadGeneratorAction.UNFIX_PAGE, start_page + i, False)
            elif type == 4 and self._curr_seq_write != self.num_scans - 1:  # sequential read
                start_page = self._seq_writes[self._curr_seq_write]
                self._curr_seq_write += 1
                for i in range(0, self.scan_length):
                    yield (WorkloadGeneratorAction.FIX_PAGE, start_page + i, True)
                    yield (WorkloadGeneratorAction.UNFIX_PAGE, start_page + i, True)
        return

