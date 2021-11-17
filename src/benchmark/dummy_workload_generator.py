from abc import ABC, abstractmethod
from typing import Tuple, Iterator
from enum import Enum

from src.benchmark.abstract_workload_generator import AbstractWorkloadGenerator, WorkloadGeneratorAction


class DummyWorkloadGenerator(AbstractWorkloadGenerator):
    def __init__(self):
        super().__init__()

    def get_actions(self) -> Iterator[Tuple[WorkloadGeneratorAction, int, bool]]:
        for i in range(0, 100):
            yield (WorkloadGeneratorAction.FIX_PAGE, i, True)
            yield (WorkloadGeneratorAction.UNFIX_PAGE, i, True)            
        return
