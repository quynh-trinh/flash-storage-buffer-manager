import unittest
from src.buffer.replacement.acr_replacer import ACRReplacer

class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.cfdc = ACRReplacer(16)
        self.pages = self.cfdc.pages
        self.dp = self.cfdc.demoted_pin
        self.ct = self.cfdc.cluster_table


if __name__ == '__main__':
    unittest.main()
