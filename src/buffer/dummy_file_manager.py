from typing import Dict
from threading import Lock
import os, mmap
import math
from src.util.page_id_utils import get_segment_id, get_segment_page_id
from src.util.constants import PAGE_SIZE
from src.buffer.file_manager import FileManager

"""Implementation of the FileManager interface that doesn't actually do any I/O.
Can be used for testing buffer manager and replacement policy.
"""
class DummyFileManager(FileManager):
    def __init__(self, directory = 'data', page_size = PAGE_SIZE):
        pass
    
    def __del__(self):
        pass

    def create_file(self, file_name: str):
        pass
    
    def remove_file(self, file_name: str):
        pass

    def read_block(self, file_name: str, page_id: int, dest: bytearray):
        pass

    def write_block(self, file_name: str, page_id: int, src: bytearray):
        pass