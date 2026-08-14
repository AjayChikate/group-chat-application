"""
logger.py
------------------------------------------------------------
Minimal structured logger. Every line is a single JSON object
with a timestamp and event name, instead of ad-hoc print()
strings. This is a small nod to real operational practice:
structured logs are grep/jq-able and can be piped into a log
aggregator later without changing call sites.
------------------------------------------------------------
"""
import json
from datetime import datetime, timezone


def log(event: str, **meta) -> None:
    """Emit one structured JSON log line to stdout."""
    line = {
        'ts': datetime.now(timezone.utc).isoformat(),
        'event': event,
        **meta,
    }
    print(json.dumps(line))