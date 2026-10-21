import pytest

from minikv.protocol import RESPParser, encode_array, encode_error, encode_integer


def test_parser_handles_split_mid_bulk() -> None:
    parser = RESPParser()
    assert parser.feed(b"*2\r\n$3\r\nGE") == []
    assert parser.feed(b"T\r\n$1\r\na\r\n") == [[b"GET", b"a"]]


def test_parser_handles_split_mid_crlf() -> None:
    parser = RESPParser()
    assert parser.feed(b"*1\r") == []
    assert parser.feed(b"\n$4\r\nPING\r") == []
    assert parser.feed(b"\n") == [[b"PING"]]


def test_parser_handles_pipelined_commands() -> None:
    parser = RESPParser()
    assert parser.feed(b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n") == [
        [b"PING"],
        [b"ECHO", b"hi"],
    ]


def test_encoders() -> None:
    assert encode_error("ERR bad") == b"-ERR bad\r\n"
    assert encode_integer(12) == b":12\r\n"
    assert encode_array([b"ok", None]) == b"*2\r\n$2\r\nok\r\n$-1\r\n"


def test_malformed_bulk_terminator_raises() -> None:
    parser = RESPParser()
    with pytest.raises(ValueError):
        parser.feed(b"$1\r\naXX")
