from abc import ABC, abstractmethod
from typing import Tuple, Iterator
from enum import Enum
from numpy.random import seed
from numpy.random import randint

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

    def get_actions(self) -> Iterator[Tuple[WorkloadGeneratorAction, int, bool]]:
        seed(1)
        random_reads = list(randint(0, self.total_pages, self.num_single_pages))
        random_writes = list(randint(0, self.total_pages, self.num_single_pages))

        seq_reads = list(randint(0, self.total_pages - self.scan_length, self.num_scans))
        seq_writes = list(randint(0, self.total_pages - self.scan_length, self.num_scans))

        types = randint(1, 5, self.total_calls)

        for type in types:
            if type == 1 and random_reads != []: # random read
                page = random_reads.pop(-1)
                yield (WorkloadGeneratorAction.FIX_PAGE, page, False)
                yield (WorkloadGeneratorAction.UNFIX_PAGE, page, False)
            elif type == 2 and random_writes != []: # random write
                page = random_writes.pop(-1)
                yield (WorkloadGeneratorAction.FIX_PAGE, page, True)
                yield (WorkloadGeneratorAction.UNFIX_PAGE, page, True)
            elif type == 3 and seq_reads != []: # sequential read
                start_page = seq_reads.pop(-1)
                for i in range(0, self.scan_length):
                    yield (WorkloadGeneratorAction.FIX_PAGE, start_page + i, False)
                    yield (WorkloadGeneratorAction.UNFIX_PAGE, start_page + i, False)
            elif type == 4 and seq_writes != []:  # sequential read
                start_page = seq_writes.pop(-1)
                for i in range(0, self.scan_length):
                    yield (WorkloadGeneratorAction.FIX_PAGE, start_page + i, True)
                    yield (WorkloadGeneratorAction.UNFIX_PAGE, start_page + i, True)
        return

