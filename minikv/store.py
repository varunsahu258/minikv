"""In-memory typed storage with lazy and active expiration."""

from __future__ import annotations

import random
import time
from collections.abc import Callable

Value = bytes | list[bytes]


class WrongTypeError(Exception):
    """Raised when an operation does not match the stored value type."""


class Store:
    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._data: dict[bytes, Value] = {}
        self._expires: dict[bytes, float] = {}

    def _purge(self, key: bytes) -> None:
        deadline = self._expires.get(key)
        if deadline is not None and self._clock() >= deadline:
            self._data.pop(key, None)
            self._expires.pop(key, None)

    def _get_value(self, key: bytes) -> Value | None:
        self._purge(key)
        return self._data.get(key)

    def set(
        self,
        key: bytes,
        value: bytes,
        *,
        ttl: float | None = None,
        absolute_expiry_ms: int | None = None,
    ) -> None:
        self._data[key] = value
        self._expires.pop(key, None)
        if ttl is not None:
            self._expires[key] = self._clock() + ttl
        elif absolute_expiry_ms is not None:
            self._expires[key] = self._clock() + (absolute_expiry_ms / 1000 - time.time())
            self._purge(key)

    def get(self, key: bytes) -> bytes | None:
        value = self._get_value(key)
        if value is None:
            return None
        if not isinstance(value, bytes):
            raise WrongTypeError
        return value

    def delete(self, *keys: bytes) -> int:
        removed = 0
        for key in keys:
            self._purge(key)
            if key in self._data:
                del self._data[key]
                self._expires.pop(key, None)
                removed += 1
        return removed

    def exists(self, *keys: bytes) -> int:
        return sum(self._get_value(key) is not None for key in keys)

    def dbsize(self) -> int:
        for key in list(self._data):
            self._purge(key)
        return len(self._data)

    def expire(self, key: bytes, seconds: int) -> int:
        return self.pexpire(key, seconds * 1000)

    def pexpire(self, key: bytes, milliseconds: int) -> int:
        if self._get_value(key) is None:
            return 0
        self._expires[key] = self._clock() + milliseconds / 1000
        self._purge(key)
        return 1

    def ttl(self, key: bytes, *, milliseconds: bool = False) -> int:
        if self._get_value(key) is None:
            return -2
        deadline = self._expires.get(key)
        if deadline is None:
            return -1
        remaining = deadline - self._clock()
        scale = 1000 if milliseconds else 1
        return max(-2, int(remaining * scale))

    def persist(self, key: bytes) -> int:
        if self._get_value(key) is None or key not in self._expires:
            return 0
        del self._expires[key]
        return 1

    def pexpireat(self, key: bytes, unix_milliseconds: int) -> int:
        if self._get_value(key) is None:
            return 0
        remaining = unix_milliseconds / 1000 - time.time()
        self._expires[key] = self._clock() + remaining
        self._purge(key)
        return 1

    def expiration_unix_ms(self, key: bytes) -> int | None:
        self._purge(key)
        deadline = self._expires.get(key)
        if deadline is None:
            return None
        return int((time.time() + deadline - self._clock()) * 1000)

    def snapshot(self) -> list[tuple[bytes, Value, int | None]]:
        """Return live values and wall-clock expiry times for AOF rewriting."""
        result: list[tuple[bytes, Value, int | None]] = []
        for key in list(self._data):
            value = self._get_value(key)
            if value is not None:
                result.append((key, value.copy() if isinstance(value, list) else value,
                               self.expiration_unix_ms(key)))
        return result

    def increment(self, key: bytes, amount: int = 1) -> int:
        value = self._get_value(key)
        if value is None:
            current = 0
        elif not isinstance(value, bytes):
            raise WrongTypeError
        else:
            try:
                current = _parse_integer(value)
            except ValueError as exc:
                raise ValueError("value is not an integer or out of range") from exc
        result = current + amount
        if not -(2**63) <= result <= 2**63 - 1:
            raise ValueError("value is not an integer or out of range")
        self._data[key] = str(result).encode("ascii")
        return result

    def lpush(self, key: bytes, *values: bytes) -> int:
        current = self._list_for_write(key)
        current[0:0] = list(reversed(values))
        return len(current)

    def rpush(self, key: bytes, *values: bytes) -> int:
        current = self._list_for_write(key)
        current.extend(values)
        return len(current)

    def lpop(self, key: bytes) -> bytes | None:
        value = self._get_value(key)
        if value is None:
            return None
        if isinstance(value, bytes):
            raise WrongTypeError
        result = value.pop(0) if value else None
        if not value:
            self._data.pop(key, None)
            self._expires.pop(key, None)
        return result

    def rpop(self, key: bytes) -> bytes | None:
        value = self._get_value(key)
        if value is None:
            return None
        if isinstance(value, bytes):
            raise WrongTypeError
        result = value.pop() if value else None
        if not value:
            self._data.pop(key, None)
            self._expires.pop(key, None)
        return result

    def lrange(self, key: bytes, start: int, stop: int) -> list[bytes]:
        value = self._get_value(key)
        if value is None:
            return []
        if isinstance(value, bytes):
            raise WrongTypeError
        length = len(value)
        if start < 0:
            start += length
        if stop < 0:
            stop += length
        start = max(start, 0)
        stop = min(stop, length - 1)
        return value[start : stop + 1] if start <= stop else []

    def llen(self, key: bytes) -> int:
        value = self._get_value(key)
        if value is None:
            return 0
        if isinstance(value, bytes):
            raise WrongTypeError
        return len(value)

    def sweep_expired(self, sample_size: int = 20) -> int:
        """Run Redis-style random expiration sampling once, including retries."""
        removed = 0
        while True:
            candidates = list(self._expires)
            if not candidates:
                return removed
            sample = random.sample(candidates, min(sample_size, len(candidates)))
            expired = 0
            for key in sample:
                before = key in self._data
                self._purge(key)
                if before and key not in self._data:
                    expired += 1
            removed += expired
            if not sample or expired / len(sample) <= 0.25:
                return removed

    def _list_for_write(self, key: bytes) -> list[bytes]:
        value = self._get_value(key)
        if value is None:
            value = []
            self._data[key] = value
        elif isinstance(value, bytes):
            raise WrongTypeError
        return value


def _parse_integer(value: bytes) -> int:
    digits = value[1:] if value[:1] in (b"+", b"-") else value
    if not digits or not digits.isdigit() or any(byte > 127 for byte in digits):
        raise ValueError
    result = int(value)
    if not -(2**63) <= result <= 2**63 - 1:
        raise ValueError
    return result
