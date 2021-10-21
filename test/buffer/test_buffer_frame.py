import unittest

from src.buffer.buffer_frame import BufferFrame

class BufferFrameTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def test_should_move_buffer_frame(self):
        frame = BufferFrame(1, 4096)
        frame_bytes = frame.data
        string = "Hello World"
        content = bytes(string, 'utf-8')
        frame_bytes[0:len(content)] = content
        
        self.assertEqual(frame_bytes[0:len(content)].decode('utf-8'), string)
        
        new_frame = frame.move()
        new_frame_bytes = new_frame.data
        self.assertEqual(new_frame_bytes[0:len(content)].decode('utf-8'), string)
        frame_bytes = frame.data
        self.assertEqual(frame_bytes[0:len(content)].decode('utf-8'), "\0" * len(content))
