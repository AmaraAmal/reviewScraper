import threading
import time
class DictionaryWithTimeout:
    def __init__(self, timeout_seconds):
        self.timeout_seconds = timeout_seconds
        self.data = {}
        self.lock = threading.Lock()
        self._cleanup_thread = threading.Thread(target=self._cleanup_expired_elements, daemon=True)
        self._cleanup_thread.start()
    def _cleanup_expired_elements(self):
        while True:
            time.sleep(self.timeout_seconds)
            now = time.time()
            with self.lock:
                expired_keys = [key for key, (value, timestamp) in self.data.items() if now - timestamp > self.timeout_seconds]
                for key in expired_keys:
                    self.data.pop(key, None)
    def __getitem__(self, key):
        with self.lock:
            value, _ = self.data[key]
            return value
    def __setitem__(self, key, value):
        with self.lock:
            self.data[key] = (value, time.time())
    def __delitem__(self, key):
        with self.lock:
            del self.data[key]
    def __contains__(self, key):
        with self.lock:
            return key in self.data
    def keys(self):
        with self.lock:
            return list(self.data.keys())
    def values(self):
        with self.lock:
            return [value for value, _ in self.data.values()]
    def items(self):
        with self.lock:
            return list(self.data.items())
    def __len__(self):
        with self.lock:
            return len(self.data)