"""Incremental RESP2 parsing and response encoding."""

from __future__ import annotations

from dataclasses import dataclass, field

RESPValue = bytes | int | None | list["RESPValue"]


class RESPProtocolError(ValueError):
    """Raised when a client sends malformed RESP data."""


@dataclass
class RESPParser:
    """Buffer arbitrary chunks and yield every complete RESP value in order."""

    _buffer: bytearray = field(default_factory=bytearray)
    max_bulk_length: int | None = None

    @property
    def buffered_bytes(self) -> bytes:
        """Return the incomplete trailing frame, if any."""
        return bytes(self._buffer)

    def feed(self, data: bytes) -> list[RESPValue]:
        self._buffer.extend(data)
        values: list[RESPValue] = []
        position = 0
        while position < len(self._buffer):
            parsed = _parse_at(self._buffer, position, self.max_bulk_length)
            if parsed is None:
                break
            value, end = parsed
            values.append(value)
            position = end
        if position:
            del self._buffer[:position]
        return values


def _line_end(data: bytearray, start: int) -> int | None:
    end = data.find(b"\r\n", start)
    return None if end < 0 else end


def _parse_at(
    data: bytearray, start: int, max_bulk_length: int | None = None
) -> tuple[RESPValue, int] | None:
    if start >= len(data):
        return None
    marker = data[start]
    if marker in (ord("+"), ord("-"), ord(":")):
        end = _line_end(data, start + 1)
        if end is None:
            return None
        line = bytes(data[start + 1 : end])
        if marker == ord(":"):
            try:
                value: RESPValue = int(line)
            except ValueError as exc:
                raise RESPProtocolError("invalid RESP integer") from exc
        else:
            value = line
        return value, end + 2
    if marker == ord("$"):
        end = _line_end(data, start + 1)
        if end is None:
            return None
        try:
            length = int(data[start + 1 : end])
        except ValueError as exc:
            raise RESPProtocolError("invalid bulk string length") from exc
        body_start = end + 2
        if length == -1:
            return None, body_start
        if length < 0:
            raise RESPProtocolError("invalid bulk string length")
        if max_bulk_length is not None and length > max_bulk_length:
            raise RESPProtocolError("bulk string exceeds maximum request size")
        body_end = body_start + length
        if len(data) < body_end + 2:
            return None
        if data[body_end : body_end + 2] != b"\r\n":
            raise RESPProtocolError("bulk string is not terminated by CRLF")
        return bytes(data[body_start:body_end]), body_end + 2
    if marker == ord("*"):
        end = _line_end(data, start + 1)
        if end is None:
            return None
        try:
            count = int(data[start + 1 : end])
        except ValueError as exc:
            raise RESPProtocolError("invalid array length") from exc
        if count == -1:
            return None, end + 2
        if count < 0:
            raise RESPProtocolError("invalid array length")
        values: list[RESPValue] = []
        position = end + 2
        for _ in range(count):
            item = _parse_at(data, position, max_bulk_length)
            if item is None:
                return None
            value, position = item
            values.append(value)
        return values, position
    raise RESPProtocolError(f"unknown RESP type byte: {bytes([marker])!r}")


def encode_simple_string(value: str | bytes) -> bytes:
    return b"+" + _as_bytes(value) + b"\r\n"


def encode_error(value: str | bytes) -> bytes:
    return b"-" + _as_bytes(value) + b"\r\n"


def encode_integer(value: int) -> bytes:
    return b":" + str(value).encode("ascii") + b"\r\n"


def encode_bulk_string(value: str | bytes) -> bytes:
    payload = _as_bytes(value)
    return b"$" + str(len(payload)).encode("ascii") + b"\r\n" + payload + b"\r\n"


def encode_null_bulk_string() -> bytes:
    return b"$-1\r\n"


def encode_array(values: list[RESPValue]) -> bytes:
    return b"*" + str(len(values)).encode("ascii") + b"\r\n" + b"".join(
        encode(value) for value in values
    )


def encode(value: RESPValue) -> bytes:
    if value is None:
        return encode_null_bulk_string()
    if isinstance(value, int):
        return encode_integer(value)
    if isinstance(value, list):
        return encode_array(value)
    return encode_bulk_string(value)


def _as_bytes(value: str | bytes) -> bytes:
    return value.encode("utf-8") if isinstance(value, str) else value
