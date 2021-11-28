import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '../..'))

import pandas as pd
from typing import Dict

from src.benchmark.abstract_benchmark import AbstractBenchmark
from src.buffer.buffer_manager import BufferManager
from src.buffer.buffer_frame import BufferFrame
from src.buffer.dummy_file_manager import DummyFileManager
from src.buffer.replacement.random_replacer import RandomReplacer
from src.buffer.replacement.two_q_replacer import TwoQReplacer
from src.buffer.replacement.lru_replacer import LRUReplacer
from src.buffer.replacement.cfdc_replacer import CFDCReplacer
from src.buffer.metric_collector import Metric, MetricCollector
from src.util.constants import BENCHMARK_DATA_FOLDER
from src.benchmark.abstract_workload_generator import AbstractWorkloadGenerator, WorkloadGeneratorAction
from src.benchmark.workload_generators import DummyWorkloadGenerator, OHJWorkloadGenerator


class SyntheticBenchmark(AbstractBenchmark):
    def __init__(self, repetitions, frame_count, replacer, metric_collector, workload_generator: AbstractWorkloadGenerator):
        super().__init__(repetitions=repetitions)
        self._metric_collector = metric_collector
        self._frame_count = frame_count
        self._frame_size = 4 * 2**10
        self._replacer = replacer
        self._file_manager = DummyFileManager(page_size=self._frame_size)
        self._workload_generator = workload_generator

    def _setUp(self):
        self._buffer_manager = BufferManager(self._frame_count,
                                             self._frame_size,
                                             self._replacer,
                                             self._file_manager,
                                             self._metric_collector)
        self._frames: Dict[int, BufferFrame] = {}

    def _run(self):
        for action in self._workload_generator.get_actions():
            if action[0] == WorkloadGeneratorAction.FIX_PAGE:
                # print(f"Fix page   {action[1]:15} exclusive: {action[2]}")
                self._frames[action[1]] = self._buffer_manager.fix_page(action[1], action[2])
            else:
                # print(f"Unfix page {action[1]:15} dirty:     {action[2]}")
                self._buffer_manager.unfix_page(self._frames[action[1]], action[2])


if __name__ == '__main__':
    metrics_df = pd.DataFrame(columns=['algorithm', 'relative_buffer_pool_size', 'num_hits', 'num_misses', 'num_accesses', 'num_dirty_evictions'])

    total_pages_needed = 100000
    for i in range(10, 101, 10):
        frame_count = int(total_pages_needed * i/100)
        print(f"Frame count: {frame_count}")
        replacers = [#("Random", RandomReplacer(frame_count)),
                     #("2Q", TwoQReplacer(frame_count)),
                     #("LRU", LRUReplacer(frame_count)),
                     ("CFDC", CFDCReplacer(frame_count))
                     ]
        for replacer in replacers:
            metric_collector = MetricCollector()
            workload_generator = OHJWorkloadGenerator()
            benchmark = SyntheticBenchmark(1, frame_count, replacer[1], metric_collector, workload_generator)
            benchmark.run_benchmark()

            metrics_df = metrics_df.append({'algorithm': replacer[0],
                                            'relative_buffer_pool_size': i,
                                            'num_hits': metric_collector.get_metric(Metric.BUFFER_MANAGER_HITS),
                                            'num_misses': metric_collector.get_metric(Metric.BUFFER_MANAGER_MISSES),
                                            'num_accesses': metric_collector.get_metric(Metric.BUFFER_MANAGER_ACCESSES),
                                            'num_dirty_evictions': metric_collector.get_metric(Metric.BUFFER_MANAGER_DIRTY_EVICTIONS),
                                            'num_evictions': metric_collector.get_metric(Metric.BUFFER_MANAGER_EVICTIONS)
                                            },
                                            ignore_index=True)
            metrics_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/synthetic.csv')
