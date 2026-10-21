"""RESP append-only persistence and snapshot rewrite."""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path
from typing import BinaryIO, cast

from minikv.protocol import RESPParser, RESPValue, encode_array
from minikv.store import Store

logger = logging.getLogger(__name__)

WRITES = {
    b"SET", b"DEL", b"EXPIRE", b"PEXPIRE", b"PEXPIREAT", b"PERSIST",
    b"INCR", b"DECR", b"INCRBY", b"LPUSH", b"RPUSH", b"LPOP", b"RPOP",
}


class AOF:
    def __init__(self, path: str | Path = "appendonly.aof", fsync: str = "everysec") -> None:
        if fsync not in {"always", "everysec", "no"}:
            raise ValueError("appendfsync must be always, everysec, or no")
        self.path = Path(path)
        self.fsync = fsync
        self._stream: BinaryIO | None = None
        self._lock = threading.RLock()

    def replay(self, store: Store) -> None:
        if not self.path.exists():
            return
        data = self.path.read_bytes()
        parser = RESPParser()
        values: list[RESPValue] = []
        # Feed incrementally. If malformed data occurs at the tail, keep all
        # preceding complete commands and discard the incomplete final record.
        offset = 0
        try:
            while offset < len(data):
                values.extend(parser.feed(data[offset : offset + 1]))
                offset += 1
        except ValueError:
            logger.warning("Ignoring truncated or malformed final AOF record in %s", self.path)
        if parser.buffered_bytes:
            logger.warning("Ignoring truncated final AOF record in %s", self.path)
        from minikv.commands import dispatch

        for value in values:
            dispatch(value, store)

    def append(self, command: list[bytes]) -> None:
        with self._lock:
            stream = self._open_append()
            stream.write(encode_array(cast(list[RESPValue], command)))
            if self.fsync == "always":
                self.sync()

    def _open_append(self) -> BinaryIO:
        if self._stream is None or self._stream.closed:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._stream = self.path.open("ab")
        return self._stream

    def sync(self) -> None:
        with self._lock:
            stream = self._open_append()
            stream.flush()
            os.fsync(stream.fileno())

    def close(self) -> None:
        with self._lock:
            if self._stream is not None:
                self._stream.flush()
                os.fsync(self._stream.fileno())
                self._stream.close()
                self._stream = None

    def rewrite(self, store: Store) -> None:
        with self._lock:
            if self._stream is not None:
                self._stream.flush()
                os.fsync(self._stream.fileno())
                self._stream.close()
                self._stream = None
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_name(self.path.name + ".tmp")
            with temporary.open("wb") as stream:
                for key, value, deadline in store.snapshot():
                    if isinstance(value, bytes):
                        command = [b"SET", key, value]
                        if deadline is not None:
                            command.extend([b"PXAT", str(deadline).encode("ascii")])
                        stream.write(encode_array(cast(list[RESPValue], command)))
                    else:
                        if not value:
                            continue
                        stream.write(encode_array(cast(list[RESPValue], [b"RPUSH", key, *value])))
                        if deadline is not None:
                            stream.write(encode_array(cast(list[RESPValue], [b"PEXPIREAT", key, str(deadline).encode("ascii")])))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            self._open_append()

    async def sync_loop(self) -> None:
        import asyncio

        while True:
            await asyncio.sleep(1)
            if self.fsync == "everysec" and self.path.exists():
                await asyncio.to_thread(self.sync)


def canonical_write(
    command: RESPValue, store: Store, result: RESPValue | Exception
) -> list[bytes] | None:
    """Return the replayable form of a successfully executed write."""
    if not isinstance(command, list) or not command or not all(
        isinstance(part, bytes) for part in command
    ):
        return None
    parts = cast(list[bytes], command)
    name = parts[0].upper()
    if name not in WRITES:
        return None
    args = parts[1:]
    if name == b"SET" and len(args) >= 2:
        if result != b"OK":
            return None
        key, value, *options = args
        normalized: list[bytes] = [b"SET", key, value]
        index = 0
        while index < len(options):
            option = options[index].upper()
            if option in (b"EX", b"PX") and index + 1 < len(options):
                try:
                    amount = int(options[index + 1])
                except ValueError:
                    return None
                expiry = store.expiration_unix_ms(key)
                if expiry is None:
                    expiry = int(
                        (time.time() + (amount if option == b"EX" else amount / 1000))
                        * 1000
                    )
                normalized.extend([b"PXAT", str(expiry).encode("ascii")])
                index += 2
            elif option == b"PXAT" and index + 1 < len(options):
                normalized.extend([b"PXAT", options[index + 1]])
                index += 2
            elif option in (b"NX", b"XX"):
                index += 1
            else:
                return None
        return normalized
    if name in (b"EXPIRE", b"PEXPIRE") and len(args) == 2:
        try:
            duration = int(args[1])
        except ValueError:
            return None
        deadline = store.expiration_unix_ms(args[0])
        if deadline is None:
            deadline = int(
                (time.time() + (duration if name == b"EXPIRE" else duration / 1000))
                * 1000
            )
        return [b"PEXPIREAT", args[0], str(deadline).encode("ascii")]
    return [name, *args]
