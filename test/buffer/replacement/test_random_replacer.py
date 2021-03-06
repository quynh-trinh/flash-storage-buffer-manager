from random import Random
import unittest

from src.buffer.replacement.random_replacer import RandomReplacer

class RandomReplacerTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_should_return_page(self):
        replacer = RandomReplacer(10)
        for i in range(0, 9):
            replacer.pin_page(i)
        for i in range(0, 9):
            replacer.unpin_page(i)
        victim = replacer.get_victim()

    def test_should_return_unpinned_page(self):
        replacer = RandomReplacer(10)
        for i in range(0, 10):
            replacer.pin_page(i)
        for i in range(0, 10):
            replacer.unpin_page(i)
            victim = replacer.get_victim()
            self.assertEqual(victim, i)