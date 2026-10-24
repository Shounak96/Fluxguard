from collections import Counter
from threading import Lock


class Counters:
    """Small process-local counters; logs remain the durable operational record."""

    def __init__(self) -> None:
        self._values: Counter[str] = Counter()
        self._lock = Lock()

    def increment(self, name: str) -> None:
        with self._lock:
            self._values[name] += 1

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._values)


counters = Counters()
