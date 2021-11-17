from abc import ABC, abstractmethod
from typing import Tuple, Iterator
from enum import Enum

class WorkloadGeneratorAction(Enum):
    FIX_PAGE = 1,
    UNFIX_PAGE = 2

class AbstractWorkloadGenerator(ABC):
    @abstractmethod
    def get_actions(self) -> Iterator[Tuple[WorkloadGeneratorAction, int, bool]]:
        pass
