from enum import Enum
from typing import Dict

"""Class for collecting metrics which running benchmarks
"""
class MetricCollector:
    def __init__(self):
        self._metrics: Dict[Metric, int] = {}
    
    def _do_increment(self, key_name):
        if key_name in self._metrics:
            self._metrics[key_name] += 1
        else:
            self._metrics[key_name] = 1
    
    def increment(self, *args):
        for key_name in args:
            self._do_increment(key_name)
    
    def get_metric(self, key_name):
        if key_name in self._metrics:
            return self._metrics[key_name]
        else:
            return 0
    
    def reset(self):
        self._metrics.clear()

class Metric(Enum):
    BUFFER_MANAGER_ACCESSES = 1
    BUFFER_MANAGER_HITS = 2
    BUFFER_MANAGER_MISSES = 3
    BUFFER_MANAGER_EVICTIONS = 4
    BUFFER_MANAGER_CLEAN_EVICTIONS = 5
    BUFFER_MANAGER_DIRTY_EVICTIONS = 6