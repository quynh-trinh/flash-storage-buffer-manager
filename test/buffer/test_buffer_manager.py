import unittest

from src.buffer.buffer_manager import BufferManager
from src.buffer.replacement.random_replacer import RandomReplacer

class RandomReplacerTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def test_should_fix_page(self):
        frame_count = 10
        buffer_manager = BufferManager(frame_count, 4096, RandomReplacer(frame_count))