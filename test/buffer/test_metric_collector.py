import unittest

from src.buffer.metric_collector import MetricCollector, Metric

class MetricCollectorTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def test_increment_get_metric(self):
        collector = MetricCollector()
        collector.increment(Metric.BUFFER_MANAGER_ACCESSES)
        collector.increment(Metric.BUFFER_MANAGER_HITS)
        collector.increment(Metric.BUFFER_MANAGER_ACCESSES)
        collector.increment(Metric.BUFFER_MANAGER_MISSES)

        self.assertEqual(collector.get_metric(Metric.BUFFER_MANAGER_ACCESSES), 2)
        self.assertEqual(collector.get_metric(Metric.BUFFER_MANAGER_HITS), 1)
        self.assertEqual(collector.get_metric(Metric.BUFFER_MANAGER_MISSES), 1)
    
    def test_get_empty_metric(self):
        collector = MetricCollector()
        self.assertEqual(collector.get_metric(Metric.BUFFER_MANAGER_ACCESSES), 0)
        