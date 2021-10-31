import unittest
from src.buffer.replacement.cfdc_replacer import CFDCReplacer


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.cfdc = CFDCReplacer(16)
        self.pages = self.cfdc.pages
        self.wq = self.cfdc.working_q
        self.cq = self.cfdc.clean_q
        self.dq = self.cfdc.dirty_q
        self.cfdc.get_replacer_info()
        self.dp = self.cfdc.demoted_pin
        self.ct = self.cfdc.cluster_table

    def test_working_queue(self):
        self.cfdc.pin_page(1)
        self.assertEqual([True, None], self.pages[1])
        self.cfdc.unpin_page(1, True)
        self.assertEqual([1], self.wq)
        self.assertEqual([False, True], self.pages[1])
        for i in range(2, 5):
            self.cfdc.pin_page(i)
        self.assertEqual([1, 2, 3, 4], self.wq)
        # print(self.pages)
        v = self.cfdc.get_victim()
        self.assertEqual(1, v)
        self.assertEqual([2, 3, 4], self.wq)
        self.cfdc.pin_page(5)
        self.assertEqual([2, 3, 4, 5], self.wq)

    def test_clean_queue(self):
        for i in range(1, 5):
            self.cfdc.pin_page(i)
        self.assertEqual([1, 2, 3, 4], self.wq)
        self.cfdc.unpin_page(3, False)
        self.assertEqual([False, False], self.pages[3])
        self.cfdc.pin_page(5)
        self.assertEqual([1, 2, 4, 5], self.wq)
        self.assertEqual([3], self.cq)
        self.cfdc.pin_page(3)
        self.assertEqual([2, 4, 5, 3], self.wq)
        self.assertEqual({1}, self.dp)

    def test_dirty_queue(self):
        for i in range(1, 5):
            self.cfdc.pin_page(i)
        self.assertEqual([1, 2, 3, 4], self.wq)
        self.cfdc.unpin_page(3, True)
        self.assertEqual([False, True], self.pages[3])
        self.cfdc.pin_page(5)
        self.assertEqual([1, 2, 4, 5], self.wq)
        cnum = self.dq[0][1]
        self.assertEqual([3], self.ct[cnum][1])

    def test_find_victims(self):
        for i in range(1, 17):
            self.cfdc.pin_page(i)
        self.assertEqual([13, 14, 15, 16], self.wq)
        self.cfdc.unpin_page(13, True)
        self.cfdc.unpin_page(15, False)
        self.cfdc.unpin_page(1, False)
        self.cfdc.unpin_page(2, True)
        self.assertEqual([1], self.cq)
        self.assertEqual([2], self.ct[2//2][1])
        self.assertEqual([13, 14, 15, 16], self.wq)
        victim = self.cfdc.get_victim()
        self.assertEqual(1, victim)
        self.cfdc.pin_page(victim) # 1
        self.assertEqual([13, 14, 16, 1], self.wq)
        self.assertEqual([15], self.cq)
        self.cfdc.pin_page(13)
        self.assertEqual([14, 16, 1, 13], self.wq)
        victim = self.cfdc.get_victim()
        self.assertEqual(15, victim)
        self.cfdc.pin_page(victim) # 15
        self.assertEqual([16, 1, 13, 15], self.wq)
        self.assertTrue(14 in self.dp)


if __name__ == '__main__':
    unittest.main()
