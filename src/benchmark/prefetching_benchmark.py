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
from src.buffer.metric_collector import Metric, MetricCollector
from src.util.constants import BENCHMARK_DATA_FOLDER, PAGE_SIZE
from src.benchmark.abstract_workload_generator import AbstractWorkloadGenerator, WorkloadGeneratorAction
from src.benchmark.eva_trace_workload_generator import EvaTraceWorkloadGenerator

VIDEO_PAGE_SIZE = 4 * 2 ** 20
DATA_FOLDER = "data/eva_benchmark/"
WITH_TIMING = False
PREFETCHING_DEPTH = 2

class EvaBenchmark(AbstractBenchmark):
    def __init__(self, repetitions, frame_count, replacer, metric_collector, read_ratio):
        super().__init__(repetitions=repetitions)
        self._metric_collector = metric_collector
        self._frame_count = frame_count
        self._replacer = replacer
        self._frame_size = VIDEO_PAGE_SIZE
        self._data_folder = DATA_FOLDER
        self._num_workers = 8
        self._read_ratio = read_ratio

    def _setUp(self):
        self._metric_collector.reset()
        if WITH_TIMING:
            self._file_manager = FileManager(page_size=VIDEO_PAGE_SIZE, directory = DATA_FOLDER)
        else:
            self._file_manager = DummyFileManager(page_size=VIDEO_PAGE_SIZE)
        self._buffer_manager = BufferManager(self._frame_count,
                                             self._frame_size,
                                             self._replacer(),
                                             self._file_manager,
                                             self._metric_collector)
        self._frames: Dict[int, BufferFrame] = {}
        total_requests = 100000 if WITH_TIMING else 100000
        self._workload_generator = EvaTraceWorkloadGenerator(total_requests=total_requests, read_ratio=self._read_ratio)
        
        self._random_generator = default_rng(seed=12345)

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
                for i in range(1, PREFETCHING_DEPTH+1):
                    prefetched_page_id = action[1] + i
                    if not self._buffer_manager.page_present(prefetched_page_id) and self._buffer_manager.safe_to_fix_page(prefetched_page_id, False):
                        # print(f"Prefetch Fix page   {prefetched_page_id:15} exclusive: False")
                        self._frames[prefetched_page_id] = self._buffer_manager.fix_page(prefetched_page_id, False, is_prefetch=True)
                        self._buffer_manager.unfix_page(self._frames[prefetched_page_id], False, is_prefetch=True)
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
    # Only needs to be done once
    # setup()

    total_pages_needed = 4 * 212
    # max_size = 51 if WITH_TIMING else 101
    for read_ratio in [0.9]:
        metrics_df = pd.DataFrame(columns=['algorithm', 'relative_buffer_pool_size', 'num_hits', 'num_misses', 'num_accesses', 'num_dirty_evictions', 'num_evictions'])
        for i in range(10, 101, 10):
            frame_count = int(total_pages_needed * i/100)
            print(f"Frame count: {frame_count}")
            replacers = [("Random", lambda: RandomReplacer(frame_count)),
                        ("2Q", lambda: TwoQReplacer(frame_count)),
                        ("LRU", lambda: LRUReplacer(frame_count)),
                        ("CFDC", lambda: CFDCReplacer(frame_count, max_cluster_size=2))]
            for replacer in replacers:
                metric_collector = MetricCollector()
                benchmark = EvaBenchmark(2 if WITH_TIMING else 1, frame_count, replacer[1], metric_collector, read_ratio)
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
                metrics_df.to_csv(f'{BENCHMARK_DATA_FOLDER}/prefetching_trace.csv')
