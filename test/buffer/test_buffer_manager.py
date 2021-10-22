import unittest
import struct
import os
import glob

from src.buffer.file_manager import FileManager
from src.buffer.buffer_manager import BufferManager
from src.buffer.replacement.random_replacer import RandomReplacer
from src.util.page_id_utils import make_page_id
from src.buffer.error import BufferFullError

class BufferManagerTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def setUp(self):
        files = glob.glob('data/*')
        for f in files:
            os.remove(f)

    def test_should_fix_page(self):
        frame_count = 10
        file_manager = FileManager()
        buffer_manager = BufferManager(frame_count, 4096, RandomReplacer(frame_count), file_manager)
        frame = buffer_manager.fix_page(0, True)

        value = 1024
        struct.pack_into("Q", frame.data, 0, value)

        buffer_manager.unfix_page(frame, True)
        frame = buffer_manager.fix_page(0, True)
        value_from_frame = struct.unpack_from("Q", frame.data, 0)[0]
        self.assertEqual(value, value_from_frame)

        buffer_manager.unfix_page(frame, False)
    
    def test_should_fix_multiple_pages(self):
        frame_count = 10
        file_manager = FileManager()
        buffer_manager = BufferManager(frame_count, 4096, RandomReplacer(frame_count), file_manager)
        frames = []
        for i in range(0, frame_count):
            frames.append(buffer_manager.fix_page(i, True))
            value = 1024 + i
            struct.pack_into("Q", frames[i].data, 0, value)
        for i in range(0, frame_count):
            buffer_manager.unfix_page(frames[i], True)
        
        frames = []
        for i in range(0, frame_count):
            frames.append(buffer_manager.fix_page(i, False))
            expected_value = 1024 + i
            actual_value = struct.unpack_from("Q", frames[i].data)[0]
            self.assertEqual(expected_value, actual_value)
        for i in range(0, frame_count):
            buffer_manager.unfix_page(frames[i], True)
    
    def test_should_fix_multiple_times(self):
        frame_count = 10
        file_manager = FileManager()
        buffer_manager = BufferManager(frame_count, 4096, RandomReplacer(frame_count), file_manager)
        frame = buffer_manager.fix_page(0, False)
        frame_copy = buffer_manager.fix_page(0, False)
        buffer_manager.unfix_page(frame, False)
        buffer_manager.unfix_page(frame_copy, False)
    
    def test_persistent_restart(self):
        frame_count = 10
        file_manager = FileManager()
        buffer_manager = BufferManager(frame_count, 4096, RandomReplacer(frame_count), file_manager)
        for seg in range(0, 3):
            for page in range(0, 10):
                page_id = make_page_id(seg, page)
                frame = buffer_manager.fix_page(page_id, True)
                struct.pack_into("Q", frame.data, 0, page_id)
                buffer_manager.unfix_page(frame, True)
        
        file_manager = FileManager()
        buffer_manager = BufferManager(frame_count, 4096, RandomReplacer(frame_count), file_manager)
        for seg in range(0, 3):
            for page in range(0, 10):
                page_id = make_page_id(seg, page)
                frame = buffer_manager.fix_page(page_id, True)
                value = struct.unpack_from("Q", frame.data, 0)[0]
                self.assertEqual(page_id, value)
                buffer_manager.unfix_page(frame, True)
    
    def test_buffer_full_should_throw(self):
        frame_count = 10
        file_manager = FileManager()
        buffer_manager = BufferManager(frame_count, 4096, RandomReplacer(frame_count), file_manager)
        frames = []
        for i in range(0, frame_count):
            frames.append(buffer_manager.fix_page(i, False))
        self.assertRaises(BufferFullError, buffer_manager.fix_page, frame_count, False)