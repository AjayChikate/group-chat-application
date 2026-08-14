"""
rate_limiter.py
------------------------------------------------------------
Per-connection token-bucket rate limiter.

Why: the baseline tutorial has no defense against a client
(buggy or malicious) hammering send() in a loop and flooding
every other participant. A token bucket allows short bursts
(normal typing/sending behavior) while capping sustained rate.

capacity        -> max tokens (= max burst size)
refill_per_sec  -> tokens regenerated per second
------------------------------------------------------------
"""
import time


class TokenBucket:
    def __init__(self, capacity: float, refill_per_sec: float):
        self.capacity = capacity
        self.refill_per_sec = refill_per_sec
        self.tokens = capacity
        self.last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed_sec = now - self.last_refill
        if elapsed_sec > 0:
            self.tokens = min(self.capacity, self.tokens + elapsed_sec * self.refill_per_sec)
            self.last_refill = now

    def try_consume(self, cost: float = 1) -> bool:
        """Attempt to spend `cost` tokens. Returns True if allowed."""
        self._refill()
        if self.tokens >= cost:
            self.tokens -= cost
            return True
        return False