import unittest
from src.buffer.replacement.two_q_replacer import TwoQReplacer


class Test2QReplacer(unittest.TestCase):
    def setUp(self):
        self.twoq_replacer = TwoQReplacer(3)

    def test_fifo(self):
        self.twoq_replacer.pin_page(1)
        self.twoq_replacer.unpin_page(1)
        self.assertEqual([1], self.twoq_replacer.fifo_q)
        self.assertEqual([], self.twoq_replacer.lru_q)
        self.twoq_replacer.pin_page(2)
        self.twoq_replacer.pin_page(3)
        self.assertEqual([1, 2, 3], self.twoq_replacer.fifo_q)
        self.assertEqual([], self.twoq_replacer.lru_q)
        victim = self.twoq_replacer.get_victim()
        self.assertEqual(1, victim)
        self.twoq_replacer.pin_page(4)
        self.assertEqual([2, 3, 4], self.twoq_replacer.fifo_q)
        self.assertEqual([], self.twoq_replacer.lru_q)

    def test_move_to_lru(self):
        self.twoq_replacer.pin_page(1)
        self.twoq_replacer.pin_page(2)
        self.twoq_replacer.unpin_page(1)
        self.twoq_replacer.pin_page(2)
        self.assertEqual([1], self.twoq_replacer.fifo_q)
        self.assertEqual([2], self.twoq_replacer.lru_q)
        self.twoq_replacer.pin_page(1)
        self.assertEqual([], self.twoq_replacer.fifo_q)
        self.assertEqual([2,1], self.twoq_replacer.lru_q)

    def test_lru(self):
        self.twoq_replacer.pin_page(1)
        self.twoq_replacer.pin_page(2)
        self.twoq_replacer.pin_page(3)
        self.twoq_replacer.pin_page(3)
        self.assertEqual([1,2], self.twoq_replacer.fifo_q)
        self.assertEqual([3], self.twoq_replacer.lru_q)
        self.twoq_replacer.pin_page(2)
        self.assertEqual([1], self.twoq_replacer.fifo_q)
        self.assertEqual([3, 2], self.twoq_replacer.lru_q)
        self.twoq_replacer.pin_page(3)
        self.assertEqual([1], self.twoq_replacer.fifo_q)
        self.assertEqual([2, 3], self.twoq_replacer.lru_q)


    def test_find_victim_in_fifo(self):
        self.twoq_replacer.pin_page(1)
        self.twoq_replacer.pin_page(2)
        self.twoq_replacer.pin_page(2)
        self.twoq_replacer.unpin_page(1)
        victim = self.twoq_replacer.get_victim()
        self.assertEqual(1, victim)

    def test_find_victim_in_lru(self):
        self.twoq_replacer.pin_page(1)
        self.twoq_replacer.pin_page(2)
        self.twoq_replacer.pin_page(2)
        self.twoq_replacer.unpin_page(2)
        self.twoq_replacer.unpin_page(2)
        self.assertEqual([2], self.twoq_replacer.lru_q)
        victim = self.twoq_replacer.get_victim()
        self.assertEqual(2, victim)
        self.assertEqual([1], self.twoq_replacer.fifo_q)
        self.assertEqual([], self.twoq_replacer.lru_q)

if __name__ == '__main__':
    unittest.main()
