from abc import ABC, abstractmethod

from src.util.metrics import Timing

class AbstractBenchmark(ABC):
    def __init__(self, repetitions = 1):
        self.repetitions = repetitions
        self._time_measurements = []

    def _setUp(self):
        pass

    @abstractmethod
    def _run(self):
        pass

    def _tearDown(self):
        pass

    def run_benchmark(self):
        for i in range(self.repetitions):
            print(f'Running set up')
            self._setUp()
            with Timing(f'Run {i+1} of {self.repetitions}') as t:
                self._run()
            self.time_measurements.append(t.get_time())
            print(f'Running tear down')
            self._tearDown()

    @property
    def time_measurements(self):
        return self._time_measurements
