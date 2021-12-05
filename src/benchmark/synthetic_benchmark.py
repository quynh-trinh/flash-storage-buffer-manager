import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '../..'))

import pandas as pd
from typing import Dict
import mmap

from src.benchmark.abstract_benchmark import AbstractBenchmark
from src.buffer.buffer_manager import BufferManager
from src.buffer.buffer_frame import BufferFrame
from src.buffer.dummy_file_manager import DummyFileManager
from src.buffer.file_manager import FileManager
from src.buffer.replacement.random_replacer import RandomReplacer
from src.buffer.replacement.two_q_replacer import TwoQReplacer
from src.buffer.replacement.lru_replacer import LRUReplacer
from src.buffer.replacement.cfdc_replacer import CFDCReplacer
from src.buffer.replacement.acr_replacer import ACRReplacer
from src.buffer.metric_collector import Metric, MetricCollector
from src.util.constants import BENCHMARK_DATA_FOLDER
from src.benchmark.abstract_workload_generator import AbstractWorkloadGenerator, WorkloadGeneratorAction
from src.benchmark.workload_generators import DummyWorkloadGenerator, OHJWorkloadGenerator

PAGE_SIZE = 4 * 2 ** 10
DATA_FOLDER = "data/synthetic_benchmark/"
WITH_TIMING = False

class SyntheticBenchmark(AbstractBenchmark):
    def __init__(self, repetitions, frame_count, replacer, metric_collector):
        super().__init__(repetitions=repetitions)
        self._metric_collector = metric_collector
        self._frame_count = frame_count
        self._frame_size = PAGE_SIZE
        self._replacer = replacer

    def _setUp(self):
        self._metric_collector.reset()
        if WITH_TIMING:
            self._file_manager = FileManager(page_size=PAGE_SIZE, directory = DATA_FOLDER)
        else:
            self._file_manager = DummyFileManager(page_size=PAGE_SIZE)
        self._buffer_manager = BufferManager(self._frame_count,
                                             self._frame_size,
                                             self._replacer(),
                                             self._file_manager,
                                             self._metric_collector)
        self._frames: Dict[int, BufferFrame] = {}
        self._workload_generator = OHJWorkloadGenerator()

    def _run(self):
        for action in self._workload_generator.get_actions():
            if action[0] == WorkloadGeneratorAction.FIX_PAGE:
                # print(f"Fix page   {action[1]:15} exclusive: {action[2]}")
                self._frames[action[1]] = self._buffer_manager.fix_page(action[1], action[2])
            else:
                # print(f"Unfix page {action[1]:15} dirty:     {action[2]}")
                self._buffer_manager.unfix_page(self._frames[action[1]], action[2])

def setup():
    # Create empty files for later reads/writes
    file_manager = FileManager(directory=DATA_FOLDER)
    os.makedirs(DATA_FOLDER, exist_ok=True)
    file_name = "0"
    file_manager.create_file(file_name)
    with mmap.mmap(-1, PAGE_SIZE) as mm:
        file_manager.read_block(file_name, 0, mm)
        file_manager.write_block(file_name, (100000), mm)

if __name__ == '__main__':
    metrics_df = pd.DataFrame(columns=['algorithm', 'relative_buffer_pool_size', 'num_hits', 'num_misses', 'num_accesses', 'num_dirty_evictions'])

    # Only needs to be done once
    # setup()

    total_pages_needed = 100000
    for i in range(10, 101, 10):
        frame_count = int(total_pages_needed * i/100)
        print(f"Frame count: {frame_count}")
        replacers = [#("Random", lambda: RandomReplacer(frame_count)),
                     #("2Q", lambda: TwoQReplacer(frame_count)),
                     #("LRU", lambda: LRUReplacer(frame_count)),
                     #("CFDC", lambda: CFDCReplacer(frame_count, max_cluster_size=8))
                     ("ACR", lambda: ACRReplacer(frame_count))
                     ]
        for replacer in replacers:
            metric_collector = MetricCollector()
            benchmark = SyntheticBenchmark(2 if WITH_TIMING else 1, frame_count, replacer[1], metric_collector)
            benchmark.run_benchmark()

            if WITH_TIMING:
                for measurement in benchmark.time_measurements:
                    metrics_df = metrics_df.append({'algorithm': replacer[0],
                                                'relative_buffer_pool_size': i,
                                                'num_hits': metric_collector.get_metric(Metric.BUFFER_MANAGER_HITS),
                                                'num_misses': metric_collector.get_metric(Metric.BUFFER_MANAGER_MISSES),
                                                'num_accesses': metric_collector.get_metric(Metric.BUFFER_MANAGER_ACCESSES),
                                                'num_dirty_evictions': metric_collector.get_metric(Metric.BUFFER_MANAGER_DIRTY_EVICTIONS),
                                                'num_evictions': metric_collector.get_metric(Metric.BUFFER_MANAGER_EVICTIONS),
                                                'time': measurement
                                                },
                                                ignore_index=True)
            else:
                metrics_df = metrics_df.append({'algorithm': replacer[0],
                                                'relative_buffer_pool_size': i,
                                                'num_hits': metric_collector.get_metric(Metric.BUFFER_MANAGER_HITS),
                                                'num_misses': metric_collector.get_metric(Metric.BUFFER_MANAGER_MISSES),
                                                'num_accesses': metric_collector.get_metric(Metric.BUFFER_MANAGER_ACCESSES),
                                                'num_dirty_evictions': metric_collector.get_metric(Metric.BUFFER_MANAGER_DIRTY_EVICTIONS),
                                                'num_evictions': metric_collector.get_metric(Metric.BUFFER_MANAGER_EVICTIONS),
                                                },
                                                ignore_index=True)
            metrics_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/synthetic.csv')
