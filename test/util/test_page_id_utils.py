import unittest
import struct

from src.util.page_id_utils import make_page_id, get_segment_id, get_segment_page_id

class PageIdUtilsTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def test_make_page_id(self):
        self.assertEqual(0x0000000000000000, make_page_id(0, 0))
        self.assertEqual(0x0100000000000000, make_page_id(0x0100, 0))
        self.assertEqual(0x0100000000003f00, make_page_id(0x0100, 0x3f00))
        self.assertEqual(0x123456789abcdef0, make_page_id(0x1234, 0x56789abcdef0))
    
    def test_get_segment_id(self):
        self.assertEqual(0x1234, get_segment_id(0x123456789abcdef0))

    def test_get_segment_page_id(self):
        self.assertEqual(0x56789abcdef0, get_segment_page_id(0x123456789abcdef0))