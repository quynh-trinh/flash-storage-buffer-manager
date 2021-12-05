from abc import ABC, abstractmethod
from typing import List, Tuple, Iterator
from enum import Enum
from numpy.random import default_rng

from src.benchmark.abstract_workload_generator import AbstractWorkloadGenerator, WorkloadGeneratorAction
from src.util.page_id_utils import make_page_id

class TraceRecord():
    def __init__(self, action: WorkloadGeneratorAction, page_id: int, is_read: bool = True):
        self.action = action
        self.page_id = page_id
        self.is_read = is_read

class EvaTraceWorkloadGenerator(AbstractWorkloadGenerator):
    def __init__(self, total_requests = 100000, read_ratio = 0.9):
        super().__init__()
        self._trace_file = "traces/seq_scan.log"
        self._trace = self._parse_log()
        self._num_pages = self._trace[-1].page_id
        # for record in self._trace:
        #     print(f"[{record.action}, {record.page_id}]")
        self._random_generator = default_rng(seed=12345)
        self._read_ratio = read_ratio
        self._num_total_requests = total_requests
        self._num_videos = 4
        self._num_workers = 8
        self._worker_requests: List[List[TraceRecord]] = []
        self._worker_curr_request = []
        for i in range(self._num_workers):
            self._worker_requests.append([])
            self._populate_worker_requests(i)
            self._worker_curr_request.append(0)
    
    def trace_done(self) -> bool:
        return self._num_total_requests == 0

    def get_actions(self) -> Iterator[Tuple[WorkloadGeneratorAction, int, bool]]:
        while self._num_total_requests > 0:
            worker_num = int(self._random_generator.uniform(0, self._num_workers))
            yield (self._worker_requests[worker_num][self._worker_curr_request[worker_num]].action,
                   self._worker_requests[worker_num][self._worker_curr_request[worker_num]].page_id,
                   not self._worker_requests[worker_num][self._worker_curr_request[worker_num]].is_read)
            
            self._worker_curr_request[worker_num] = self._worker_curr_request[worker_num] + 1
            if self._worker_curr_request[worker_num] == len(self._worker_requests[worker_num]):
                self._populate_worker_requests(worker_num)
                self._worker_curr_request[worker_num] = 0
            
            self._num_total_requests = self._num_total_requests - 1
    
    def peek_action(self, worker_num: int) -> Tuple[WorkloadGeneratorAction, int, bool]:
        if self._num_total_requests == 0:
            return None
        return (self._worker_requests[worker_num][self._worker_curr_request[worker_num]].action,
                self._worker_requests[worker_num][self._worker_curr_request[worker_num]].page_id,
                not self._worker_requests[worker_num][self._worker_curr_request[worker_num]].is_read)
    
    def consume_action(self, worker_num: int) -> None:
        if self._num_total_requests == 0:
            return None
        
        self._worker_curr_request[worker_num] = self._worker_curr_request[worker_num] + 1
        if self._worker_curr_request[worker_num] == len(self._worker_requests[worker_num]):
            self._populate_worker_requests(worker_num)
            self._worker_curr_request[worker_num] = 0
        
        self._num_total_requests = self._num_total_requests - 1
        
    
    def _substring_between(self, input_string: str, start_marker: str, end_marker: str) -> str:
        start_index = input_string.find(start_marker) + len(start_marker)
        end_index = input_string.find(end_marker, start_index)
        return input_string[start_index:end_index]
    
    def _parse_log(self) -> List[TraceRecord]:
        records = []
        size = -1
        with open(self._trace_file) as trace:
            for line in trace:
                action_str = line[0:line.find(',')]
                action = WorkloadGeneratorAction.FIX_PAGE
                if action_str == "Fix":
                    action = WorkloadGeneratorAction.FIX_PAGE
                elif action_str == "Free":
                    action = WorkloadGeneratorAction.UNFIX_PAGE
                else:
                    raise ValueError(f"Unknown action {action_str}")
                
                if size == -1:
                    size = int(self._substring_between(line, "size ", ","))
                
                batch_num = int(self._substring_between(line, "[(", "->")) // size

                records.append(TraceRecord(action, batch_num))
        return records
    
    def _populate_worker_requests(self, worker_num: int):
        self._worker_requests[worker_num] = []
        query_type = int(self._random_generator.uniform(1, 101))
        is_read = int(self._random_generator.uniform(1, 101)) <= int(self._read_ratio * 100)
        file = int(self._random_generator.uniform(0, self._num_videos))
        if(query_type <= 20):
            # whole sequential scan
            for trace in self._trace:
                self._worker_requests[worker_num].append(TraceRecord(trace.action, make_page_id(file, trace.page_id), is_read))
        elif(query_type <= 70):
            # Partial sequential scan
            starting_page = self._num_pages - int(self._random_generator.geometric(0.05))
            starting_page = max(starting_page, 0)
            starting_record_num = 0
            if starting_page > 0:
                starting_record_num = 2*starting_page - 1
            for i in range(starting_record_num, len(self._trace)):
                if self._trace[i].page_id >= starting_page:
                    record = self._trace[i]
                    self._worker_requests[worker_num].append(TraceRecord(record.action, make_page_id(file, record.page_id), record.is_read))
        else:
            # Point query
            starting_page = int(self._random_generator.uniform(0, self._num_pages))
            starting_page = max(starting_page, 0)
            starting_record_num = 0
            if starting_page > 0:
                starting_record_num = 2*starting_page - 1
            
            length = int(self._random_generator.geometric(0.05))
            ending_page = min(starting_page + length - 1, self._num_pages)

            for i in range(starting_record_num, len(self._trace)):
                if self._trace[i].page_id >= starting_page and self._trace[i].page_id <= ending_page:
                    record = self._trace[i]
                    self._worker_requests[worker_num].append(TraceRecord(record.action, make_page_id(file, record.page_id), record.is_read))
                if self._trace[i].page_id == ending_page and self._trace[i].action == WorkloadGeneratorAction.UNFIX_PAGE:
                    break
