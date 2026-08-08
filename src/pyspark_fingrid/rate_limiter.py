"""
A minimal, instance-scoped rate limiter.

This replaces a module-level global throttle. A global means every
FingridApiClient in a process (different API keys, different tests, etc.)
shares one clock, which is both semantically wrong (different keys have
independent rate limits) and awkward to test (tests had to monkeypatch
module internals rather than construct an isolated instance).
"""

import threading
import time


class RateLimiter:
    """Blocks callers, if needed, so consecutive calls to `wait()` are spaced
    at least `min_interval_seconds` apart. Thread-safe."""

    def __init__(self, min_interval_seconds: float = 0.0):
        self.min_interval_seconds = min_interval_seconds
        self._lock = threading.Lock()
        self._last_call_time = 0.0

    def wait(self) -> None:
        """Block until at least `min_interval_seconds` have passed since the
        previous call to `wait()` on this instance."""
        with self._lock:
            now = time.monotonic()
            remaining = self.min_interval_seconds - (now - self._last_call_time)
            if remaining > 0:
                time.sleep(remaining)
            self._last_call_time = time.monotonic()
