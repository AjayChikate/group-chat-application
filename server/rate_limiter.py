
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
        
        self._refill() # checking how many tokens are left
        if self.tokens >= cost: # token usage decerase what is left
            self.tokens -= cost
            return True
        return False