import unittest
import struct
import os, mmap
from src.util.constants import PAGE_SIZE

from src.buffer.file_manager import FileManager

class FileManagerTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def test_create_file(self):
        file_manager = FileManager()
        file_manager.create_file('0')

        self.assertEqual(os.stat('data/0').st_size, FileManager._initial_size)
        file_manager.remove_file('0')
    
    def test_grow_file_error(self):
        file_manager = FileManager()
        file_manager.create_file('0')

        self.assertRaises(ValueError, file_manager._grow_file, '0', FileManager._initial_size / 2)
        file_manager.remove_file('0')
    
    def test_grow_file(self):
        file_manager = FileManager()
        file_manager.create_file('0')

        new_size_factor = 2.25
        file_manager._grow_file('0', FileManager._initial_size * new_size_factor)
        self.assertEqual(os.stat('data/0').st_size, FileManager._initial_size * new_size_factor)
        file_manager.remove_file('0')
    
    def test_write_read_file(self):
        file_manager = FileManager()
        file_manager.create_file('0')

        with mmap.mmap(-1, PAGE_SIZE) as mm1, \
             mmap.mmap(-1, PAGE_SIZE) as mm2:
            mm1.write(b'\x3f' * PAGE_SIZE)
            mm2.write(b'\x01' * PAGE_SIZE)
            file_manager.write_block('0', 1, mm1)
            file_manager.write_block('0', 2, mm2)
        
        with mmap.mmap(-1, PAGE_SIZE) as mm1, \
             mmap.mmap(-1, PAGE_SIZE) as mm2:
            file_manager.read_block('0', 1, mm1)
            file_manager.read_block('0', 2, mm2)

            mm1.seek(0, os.SEEK_SET)
            mm1_bytes = mm1.read(PAGE_SIZE)
            self.assertEqual(mm1_bytes, b'\x3f' * PAGE_SIZE)
            mm2.seek(0, os.SEEK_SET)
            mm2_bytes = mm2.read(PAGE_SIZE)
            self.assertEqual(mm2_bytes, b'\x01' * PAGE_SIZE)