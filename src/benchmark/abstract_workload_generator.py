from abc import ABC, abstractmethod
from typing import Tuple, Iterator
from enum import Enum


class WorkloadGeneratorAction(Enum):
    FIX_PAGE = 1,
    UNFIX_PAGE = 2


class RequestType(Enum):
    RANDOM_READ = 1,
    RANDOM_WRITE = 2,
    SEQUENTIAL_READ = 3,
    SEQUENTIAL_WRITE = 4


class AbstractWorkloadGenerator(ABC):
    @abstractmethod
    def get_actions(self) -> Iterator[Tuple[WorkloadGeneratorAction, int, bool]]:
        pass
