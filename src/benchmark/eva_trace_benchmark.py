import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '../..'))

import shutil
import mmap
import pandas as pd
from typing import Dict
from numpy.random import default_rng

from src.benchmark.abstract_benchmark import AbstractBenchmark
from src.buffer.buffer_manager import BufferManager
from src.buffer.buffer_frame import BufferFrame
from src.buffer.file_manager import FileManager
from src.buffer.dummy_file_manager import DummyFileManager
from src.buffer.replacement.random_replacer import RandomReplacer
from src.buffer.replacement.two_q_replacer import TwoQReplacer
from src.buffer.replacement.lru_replacer import LRUReplacer
from src.buffer.replacement.cfdc_replacer import CFDCReplacer
from src.buffer.replacement.acr_replacer import ACRReplacer
from src.buffer.metric_collector import Metric, MetricCollector
from src.util.constants import BENCHMARK_DATA_FOLDER, PAGE_SIZE
from src.benchmark.abstract_workload_generator import AbstractWorkloadGenerator, WorkloadGeneratorAction
from src.benchmark.eva_trace_workload_generator import EvaTraceWorkloadGenerator

VIDEO_PAGE_SIZE = 4 * 2 ** 20
DATA_FOLDER = "data/eva_benchmark/"

class EvaBenchmark(AbstractBenchmark):
    def __init__(self, repetitions, frame_count, replacer, metric_collector, workload_generator: AbstractWorkloadGenerator):
        super().__init__(repetitions=repetitions)
        self._metric_collector = metric_collector
        self._frame_count = frame_count
        self._frame_size = VIDEO_PAGE_SIZE
        self._replacer = replacer
        self._file_manager = DummyFileManager()
        self._workload_generator = workload_generator
        self._data_folder = "data/eva_benchmark"
        
        self._random_generator = default_rng(seed=12345)
        self._num_workers = 8

    def _setUp(self):
        self._buffer_manager = BufferManager(self._frame_count,
                                             self._frame_size,
                                             self._replacer,
                                             self._file_manager,
                                             self._metric_collector)
        self._frames: Dict[int, BufferFrame] = {}

    def _run(self):
        while not self._workload_generator.trace_done():
            worker_num = int(self._random_generator.uniform(0, self._num_workers))
            action = self._workload_generator.peek_action(worker_num)
            while action[0] == WorkloadGeneratorAction.FIX_PAGE \
                and not self._buffer_manager.safe_to_fix_page(action[1], action[2]):
                    # print(f"\t\t\tNot safe to fix page {action[1]} exclusive: {action[2]}")
                    worker_num = int(self._random_generator.uniform(0, self._num_workers))
                    action = self._workload_generator.peek_action(worker_num)
        
            self._workload_generator.consume_action(worker_num)
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
    for i in range(0, 4):
        file_name = f"{i}"
        file_manager.create_file(file_name)
        with mmap.mmap(-1, PAGE_SIZE) as mm:
            file_manager.read_block(file_name, 0, mm)
            file_manager.write_block(file_name, (212 * VIDEO_PAGE_SIZE) / PAGE_SIZE, mm)

if __name__ == '__main__':
    metrics_df = pd.DataFrame(columns=['algorithm', 'relative_buffer_pool_size', 'num_hits', 'num_misses', 'num_accesses', 'num_dirty_evictions', 'num_evictions'])

    # Only needs to be done once
    # setup()

    total_pages_needed = 4 * 212
    for i in range(10, 101, 10):
        frame_count = int(total_pages_needed * i/100)
        print(f"Frame count: {frame_count}")
        # replacers = [("Random", RandomReplacer(frame_count)),
        #              ("2Q", TwoQReplacer(frame_count)),
        #              ("LRU", LRUReplacer(frame_count)),
        #              ("CFDC", CFDCReplacer(frame_count)),
        #              ("ACR", ACRReplacer(frame_count))]
        replacers = [("ACR", ACRReplacer(frame_count))]
        for replacer in replacers:
            metric_collector = MetricCollector()
            workload_generator = EvaTraceWorkloadGenerator()
            benchmark = EvaBenchmark(1, frame_count, replacer[1], metric_collector, workload_generator)
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
            metrics_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/eva_trace_benchmark.csv')
