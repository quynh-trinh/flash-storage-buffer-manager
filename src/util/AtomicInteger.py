from threading import Lock

class AtomicInteger():
    def __init__(self, value=0):
        self._lock = Lock()
        self._value = value
    
    @property
    def value(self) -> int:
        with self._lock:
            return self._value
    
    @value.setter
    def value(self, v):
        with self._lock:
            self._value = v
    
    def inc(self, amt=1) -> int:
        with self._lock:
            self._value += amt
            return self._value
    
    def dec(self, amt=1) -> int:
        return self.inc(-amt)