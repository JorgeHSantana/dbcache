from __future__ import annotations
import os, time, errno

class FileLock:
    def __init__(self, path: str, timeout: float = 30.0, poll: float = 0.1):
        self.lock_path = f"{path}.lock"
        self.timeout = timeout
        self.poll = poll
        self._fd: int | None = None

    def acquire(self) -> None:
        start = time.time()
        while True:
            try:
                fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                os.write(fd, str(os.getpid()).encode("utf-8"))
                self._fd = fd
                return
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
                if time.time() - start > self.timeout:
                    raise TimeoutError(f"Timeout acquiring lock: {self.lock_path}")
                time.sleep(self.poll)

    def release(self) -> None:
        if self._fd is not None:
            try:
                os.close(self._fd)
            finally:
                self._fd = None
                try:
                    os.remove(self.lock_path)
                except FileNotFoundError:
                    pass

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()
