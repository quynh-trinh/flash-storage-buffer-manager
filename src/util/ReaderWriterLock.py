from src.util.ReaderWriterLockInternal import RWLock

class ReaderWriterLock():
    def __init__(self):
        self._lock = RWLock()
    
    def __del__(self):
        if self._lock.w_is_locked():
            self._lock.w_release()
        if self._lock.r_is_locked():
            self._lock.r_release()
    
    def lock_exclusive(self) -> bool:
        return self._lock.w_acquire()
    
    def lock_shared(self) -> bool:
        return self._lock.r_acquire()
    
    def release_exclusive(self):
        self._lock.w_release()
    
    def release_shared(self):
        self._lock.r_release()