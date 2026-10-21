"""Command validation and dispatch."""

from collections.abc import Callable
from typing import cast

from minikv.protocol import RESPValue
from minikv.store import Store, WrongTypeError


class CommandError(Exception):
    pass


CommandResult = RESPValue | CommandError
Handler = Callable[[list[bytes], Store], CommandResult]


def _arity(name: str, args: list[bytes], minimum: int, maximum: int | None = None) -> None:
    if len(args) < minimum or (maximum is not None and len(args) > maximum):
        raise CommandError(f"wrong number of arguments for '{name}' command")


def _ping(args: list[bytes], _store: Store) -> CommandResult:
    _arity("ping", args, 0, 1)
    return b"PONG" if not args else args[0]


def _echo(args: list[bytes], _store: Store) -> CommandResult:
    _arity("echo", args, 1, 1)
    return args[0]


def _set(args: list[bytes], store: Store) -> CommandResult:
    _arity("set", args, 2)
    key, value, *options = args
    ttl: float | None = None
    absolute_expiry_ms: int | None = None
    condition: bytes | None = None
    index = 0
    while index < len(options):
        option = options[index].upper()
        if option in (b"EX", b"PX", b"PXAT"):
            if ttl is not None or absolute_expiry_ms is not None or index + 1 >= len(options):
                raise CommandError("syntax error")
            try:
                duration = _parse_integer(options[index + 1])
            except ValueError:
                raise CommandError("invalid expire time in 'set' command") from None
            if option == b"PXAT":
                absolute_expiry_ms = duration
                index += 2
                continue
            if duration <= 0:
                raise CommandError("invalid expire time in 'set' command")
            ttl = duration if option == b"EX" else duration / 1000
            index += 2
            continue
        if option in (b"NX", b"XX"):
            if condition is not None:
                raise CommandError("syntax error")
            condition = option
            index += 1
            continue
        raise CommandError("syntax error")
    if condition == b"NX" and store.exists(key):
        return None
    if condition == b"XX" and not store.exists(key):
        return None
    store.set(key, value, ttl=ttl, absolute_expiry_ms=absolute_expiry_ms)
    return b"OK"


def _get(args: list[bytes], store: Store) -> CommandResult:
    _arity("get", args, 1, 1)
    return store.get(args[0])


def _del(args: list[bytes], store: Store) -> CommandResult:
    _arity("del", args, 1)
    return store.delete(*args)


def _exists(args: list[bytes], store: Store) -> CommandResult:
    _arity("exists", args, 1)
    return store.exists(*args)


def _integer(name: str, value: bytes) -> int:
    try:
        return _parse_integer(value)
    except ValueError:
        raise CommandError("value is not an integer or out of range") from None


def _parse_integer(value: bytes) -> int:
    digits = value[1:] if value[:1] in (b"+", b"-") else value
    if not digits or not digits.isdigit() or any(byte > 127 for byte in digits):
        raise ValueError
    result = int(value)
    if not -(2**63) <= result <= 2**63 - 1:
        raise ValueError
    return result


def _expire(args: list[bytes], store: Store, *, milliseconds: bool) -> CommandResult:
    name = "pexpire" if milliseconds else "expire"
    _arity(name, args, 2, 2)
    duration = _integer(name, args[1])
    return (
        store.pexpire(args[0], duration)
        if milliseconds
        else store.expire(args[0], duration)
    )


def _expire_seconds(args: list[bytes], store: Store) -> CommandResult:
    return _expire(args, store, milliseconds=False)


def _expire_millis(args: list[bytes], store: Store) -> CommandResult:
    return _expire(args, store, milliseconds=True)


def _expire_at(args: list[bytes], store: Store) -> CommandResult:
    _arity("pexpireat", args, 2, 2)
    return store.pexpireat(args[0], _integer("pexpireat", args[1]))


def _ttl(args: list[bytes], store: Store) -> CommandResult:
    _arity("ttl", args, 1, 1)
    return store.ttl(args[0])


def _pttl(args: list[bytes], store: Store) -> CommandResult:
    _arity("pttl", args, 1, 1)
    return store.ttl(args[0], milliseconds=True)


def _persist(args: list[bytes], store: Store) -> CommandResult:
    _arity("persist", args, 1, 1)
    return store.persist(args[0])


def _incr(args: list[bytes], store: Store) -> CommandResult:
    _arity("incr", args, 1, 1)
    return store.increment(args[0])


def _decr(args: list[bytes], store: Store) -> CommandResult:
    _arity("decr", args, 1, 1)
    return store.increment(args[0], -1)


def _incrby(args: list[bytes], store: Store) -> CommandResult:
    _arity("incrby", args, 2, 2)
    return store.increment(args[0], _integer("incrby", args[1]))


def _lpush(args: list[bytes], store: Store) -> CommandResult:
    _arity("lpush", args, 2)
    return store.lpush(args[0], *args[1:])


def _rpush(args: list[bytes], store: Store) -> CommandResult:
    _arity("rpush", args, 2)
    return store.rpush(args[0], *args[1:])


def _lpop(args: list[bytes], store: Store) -> CommandResult:
    _arity("lpop", args, 1, 1)
    return store.lpop(args[0])


def _rpop(args: list[bytes], store: Store) -> CommandResult:
    _arity("rpop", args, 1, 1)
    return store.rpop(args[0])


def _lrange(args: list[bytes], store: Store) -> CommandResult:
    _arity("lrange", args, 3, 3)
    start = _integer("lrange", args[1])
    stop = _integer("lrange", args[2])
    return cast(RESPValue, store.lrange(args[0], start, stop))


def _llen(args: list[bytes], store: Store) -> CommandResult:
    _arity("llen", args, 1, 1)
    return store.llen(args[0])


def _rewrite_aof(args: list[bytes], _store: Store) -> CommandResult:
    _arity("bgrewriteaof", args, 0, 0)
    return b"OK"


def _dbsize(args: list[bytes], store: Store) -> CommandResult:
    _arity("dbsize", args, 0, 0)
    return store.dbsize()


def _info(args: list[bytes], _store: Store) -> CommandResult:
    _arity("info", args, 0, 1)
    return b"# Server\r\nminikv_version:0.1.0\r\n# Clients\r\nconnected_clients:unknown\r\n"


COMMANDS: dict[bytes, Handler] = {
    b"PING": _ping,
    b"ECHO": _echo,
    b"SET": _set,
    b"GET": _get,
    b"DEL": _del,
    b"EXISTS": _exists,
    b"EXPIRE": _expire_seconds,
    b"PEXPIRE": _expire_millis,
    b"PEXPIREAT": _expire_at,
    b"TTL": _ttl,
    b"PTTL": _pttl,
    b"PERSIST": _persist,
    b"INCR": _incr,
    b"DECR": _decr,
    b"INCRBY": _incrby,
    b"LPUSH": _lpush,
    b"RPUSH": _rpush,
    b"LPOP": _lpop,
    b"RPOP": _rpop,
    b"LRANGE": _lrange,
    b"LLEN": _llen,
    b"BGREWRITEAOF": _rewrite_aof,
    b"DBSIZE": _dbsize,
    b"INFO": _info,
}


def dispatch(value: RESPValue, store: Store) -> CommandResult:
    if not isinstance(value, list) or not value or not all(
        isinstance(item, bytes) for item in value
    ):
        return CommandError("Protocol error: expected an array of bulk strings")
    command, *args = cast(list[bytes], value)
    handler = COMMANDS.get(command.upper())
    if handler is None:
        return CommandError(f"unknown command '{command.decode('utf-8', 'replace')}'")
    try:
        return handler(args, store)
    except CommandError as exc:
        return exc
    except WrongTypeError:
        return CommandError("WRONGTYPE Operation against a key holding the wrong kind of value")
    except ValueError:
        return CommandError("value is not an integer or out of range")
