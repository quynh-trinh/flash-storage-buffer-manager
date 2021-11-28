from typing import Dict
from threading import Lock
import os, mmap
import math
from src.util.page_id_utils import get_segment_id, get_segment_page_id
from src.util.constants import PAGE_SIZE

class FileManager():
    _initial_size = 2**20 # 1MB

    def __init__(self, directory = 'data', page_size = PAGE_SIZE):
        self._directory = directory
        os.makedirs(self._directory, exist_ok=True)
        self._page_size = page_size
        self._header_cache = {}
        self._file_handles: Dict[str, int] = {}
        self._lock_table_lock = Lock()
        self._lock_table: Dict[str, Lock] = {}
        self._buffers: Dict[str, mmap.mmap] = {}
    
    def __del__(self):
        for file_path, handle in self._file_handles.items():
            os.close(handle)

    def _get_file_path(self, file_name: str) -> str:
        return f"{self._directory}/{file_name}"

    def _open_file(self, file_name: str) -> int:
        file_path = self._get_file_path(file_name)
        with self._lock_table_lock:
            if file_path not in self._file_handles:
                self._file_handles[file_path] = os.open(file_path, 
                                                    os.O_CREAT | os.O_RDWR | os.O_DIRECT)
                self._lock_table[file_path] = Lock()
                self._buffers[file_path] = mmap.mmap(-1, self._page_size)
            return self._file_handles[file_path]

    def create_file(self, file_name: str):
        self._open_file(file_name)
        file_path = self._get_file_path(file_name)
        with self._lock_table[file_path]:
            with mmap.mmap(-1, FileManager._initial_size) as mm:
                mm.write(b'\0' * FileManager._initial_size)
                os.write(self._file_handles[file_path], mm)
    
    def remove_file(self, file_name: str):
        self._open_file(file_name)
        file_path = self._get_file_path(file_name)
        with self._lock_table_lock:
            os.remove(file_path)
            del self._file_handles[file_path]
            del self._lock_table[file_path]
            del self._buffers[file_path]
    
    def _file_size(self, file_name: str):
        file_path = self._get_file_path(file_name)
        return os.stat(file_path).st_size
    
    def _grow_file(self, file_name: str, new_size: int):
        file_path = self._get_file_path(file_name)
        size = os.stat(file_path).st_size
        if(new_size < size):
            raise ValueError(f"New size of file {file_path} must be greater than old size.")
        size_diff = math.floor(new_size - size)
        with mmap.mmap(-1, size_diff) as mm:
            mm.write(b'\0' * size_diff)
            os.lseek(self._file_handles[file_path], 0, os.SEEK_END)
            os.write(self._file_handles[file_path], mm)

    def read_block(self, file_name: str, page_id: int, dest: bytearray):
        self._open_file(file_name)
        file_path = self._get_file_path(file_name)
        with self._lock_table[file_path]:
            fd = os.open(file_path, os.O_CREAT | os.O_RDWR | os.O_DIRECT)
            with os.fdopen(fd, "rb+", 0) as file_obj:
                file_obj.seek(page_id * self._page_size, os.SEEK_SET)
                file_obj.readinto(self._buffers[file_path])
                dest[:] = self._buffers[file_path][:]

    def write_block(self, file_name: str, page_id: int, src: bytearray):
        self._open_file(file_name)
        file_path = self._get_file_path(file_name)
        with self._lock_table[file_path]:
            offset = page_id * self._page_size
            if(self._file_size(file_name) < offset + self._page_size):
                self._grow_file(file_name, offset + self._page_size)
            self._buffers[file_path][:] = src[:]
            os.lseek(self._file_handles[file_path], offset, os.SEEK_SET)
            os.write(self._file_handles[file_path], self._buffers[file_path])