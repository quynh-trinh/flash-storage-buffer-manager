from random import Random
import unittest

from src.buffer.error import BufferFullError
from src.buffer.replacement.lru_replacer import LRUReplacer

class LruReplacerTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_should_return_lru_page(self):
        replacer = LRUReplacer(10)
        for i in range(0, 9):
            replacer.pin_page(i)
        for i in range(0, 9):
            replacer.unpin_page(i)
        for i in range(0, 9):
            self.assertEqual(replacer.get_victim(), i)
        self.assertRaises(BufferFullError, replacer.get_victim)

    def test_should_return_unpinned_lru_page(self):
        replacer = LRUReplacer(10)
        for i in range(0, 9):
            replacer.pin_page(i)
        replacer.unpin_page(4, dirty=False)
        replacer.unpin_page(2, dirty=False)
        replacer.unpin_page(8, dirty=False)

        self.assertEqual(replacer.get_victim(), 2)
        self.assertEqual(replacer.get_victim(), 4)
        self.assertEqual(replacer.get_victim(), 8)
        self.assertRaises(BufferFullError, replacer.get_victim)

    def test_should_return_repinned_lru_page(self):
        replacer = LRUReplacer(10)
        for i in range(0, 9):
            replacer.pin_page(i)
        replacer.unpin_page(4, dirty=False)
        replacer.unpin_page(2, dirty=False)
        replacer.unpin_page(8, dirty=False)
        replacer.pin_page(4)
        replacer.unpin_page(4, dirty=False)

        self.assertEqual(replacer.get_victim(), 2)
        self.assertEqual(replacer.get_victim(), 8)
        self.assertEqual(replacer.get_victim(), 4)
        self.assertRaises(BufferFullError, replacer.get_victim)

    def test_should_return_repinned_lru_page_2(self):
        replacer = LRUReplacer(10)
        for i in range(0, 9):
            replacer.pin_page(i)
        replacer.unpin_page(4, dirty=False)
        replacer.unpin_page(2, dirty=False)
        replacer.unpin_page(8, dirty=False)
        replacer.pin_page(4)

        self.assertEqual(replacer.get_victim(), 2)
        self.assertEqual(replacer.get_victim(), 8)
        self.assertRaises(BufferFullError, replacer.get_victim)