import time


class TokenBucket:
    def __init__(self, capacity: float, refill_per_sec: float):
        self.capacity = capacity
        self.refill_per_sec = refill_per_sec
        self.window_size = capacity / refill_per_sec
        self.window_start = time.monotonic()
        self.current_count = 0
        self.previous_count = 0

    def _update_window(self) -> None:
        now = time.monotonic()
        elapsed = now - self.window_start

        if elapsed >= self.window_size:
            windows_passed = int(elapsed / self.window_size)

            if windows_passed == 1:
                self.previous_count = self.current_count
            else:
                self.previous_count = 0

            self.current_count = 0
            self.window_start += windows_passed * self.window_size

    def try_consume(self, cost: float = 1) -> bool:
        self._update_window()

        now = time.monotonic()
        elapsed = now - self.window_start

        previous_weight = (
            (self.window_size - elapsed) / self.window_size
        )

        estimated_count = (
            self.previous_count * previous_weight
            + self.current_count
        )

        if estimated_count + cost <= self.capacity:
            self.current_count += cost
            return True

        return False