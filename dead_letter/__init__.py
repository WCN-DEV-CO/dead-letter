"""dead-letter — tiny zero-dependency dead-letter queue helper for Python.

Process items with bounded retries; anything that exhausts its attempts is moved
to a dead-letter queue with its failure reason and history, instead of being lost
or blocking the pipeline. Inspect, requeue, or drain the DLQ. Pure standard library.

Original implementation. MIT licensed.
"""
from __future__ import annotations
import time
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

__version__ = "0.1.0"
__all__ = ["DeadLetterQueue", "DeadLetter", "ProcessResult"]


@dataclass
class DeadLetter:
    item: Any
    reason: str
    attempts: int
    first_seen: float
    last_error: str = ""
    history: list = field(default_factory=list)


@dataclass
class ProcessResult:
    processed: int = 0
    dead_lettered: int = 0
    retried: int = 0


class DeadLetterQueue:
    """A processing wrapper with bounded retries + a dead-letter sink.

    Args:
        handler: callable(item) -> None. Raising an exception = a failed attempt.
        max_attempts: how many times to try an item before dead-lettering.
    """

    def __init__(self, handler: Callable[[Any], Any], max_attempts: int = 3) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.handler = handler
        self.max_attempts = max_attempts
        self._dead: list[DeadLetter] = []
        self._lock = threading.Lock()

    @property
    def dead_letters(self) -> list[DeadLetter]:
        with self._lock:
            return list(self._dead)

    def __len__(self) -> int:
        with self._lock:
            return len(self._dead)

    def process(self, item: Any) -> bool:
        """Process one item with bounded retries. Returns True if handled,
        False if it was dead-lettered after exhausting attempts."""
        first = time.time()
        errors: list[str] = []
        for attempt in range(1, self.max_attempts + 1):
            try:
                self.handler(item)
                return True
            except Exception as e:  # noqa: BLE001 - we intentionally capture all handler failures
                errors.append(f"attempt {attempt}: {type(e).__name__}: {e}")
        with self._lock:
            self._dead.append(DeadLetter(
                item=item, reason="max_attempts_exhausted",
                attempts=self.max_attempts, first_seen=first,
                last_error=errors[-1] if errors else "", history=errors))
        return False

    def process_batch(self, items) -> ProcessResult:
        res = ProcessResult()
        for it in items:
            if self.process(it):
                res.processed += 1
            else:
                res.dead_lettered += 1
        return res

    def requeue_all(self) -> ProcessResult:
        """Re-attempt every dead letter through the handler. Survivors leave the DLQ;
        still-failing items are re-dead-lettered."""
        with self._lock:
            pending = self._dead
            self._dead = []
        res = ProcessResult()
        for dl in pending:
            if self.process(dl.item):
                res.retried += 1
            else:
                res.dead_lettered += 1
        return res

    def drain(self) -> list[DeadLetter]:
        """Remove and return all dead letters (e.g. to persist them elsewhere)."""
        with self._lock:
            out = self._dead
            self._dead = []
            return out
