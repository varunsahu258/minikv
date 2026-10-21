from minikv.commands import CommandError, dispatch
from minikv.store import Store


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def run(store: Store, *args: bytes):
    result = dispatch(list(args), store)
    assert not isinstance(result, CommandError), result
    return result


def test_expiry_deadline_and_ttl_sentinels() -> None:
    clock = FakeClock()
    store = Store(clock)
    assert run(store, b"TTL", b"absent") == -2
    run(store, b"SET", b"plain", b"v")
    assert run(store, b"TTL", b"plain") == -1
    assert run(store, b"PEXPIRE", b"plain", b"2000") == 1
    assert run(store, b"PTTL", b"plain") == 2000
    clock.now += 0.5
    assert run(store, b"PTTL", b"plain") == 1500
    clock.now = 102.0  # Expiration is inclusive at the exact deadline.
    assert run(store, b"GET", b"plain") is None
    assert run(store, b"TTL", b"plain") == -2


def test_set_replaces_and_clears_ttl_and_persist() -> None:
    clock = FakeClock()
    store = Store(clock)
    run(store, b"SET", b"k", b"old", b"EX", b"10")
    assert run(store, b"TTL", b"k") == 10
    run(store, b"SET", b"k", b"new")
    assert run(store, b"TTL", b"k") == -1
    assert run(store, b"PERSIST", b"k") == 0
    run(store, b"PEXPIRE", b"k", b"2000")
    assert run(store, b"PERSIST", b"k") == 1
    assert run(store, b"PTTL", b"k") == -1


def test_negative_expiry_deletes_immediately() -> None:
    store = Store(FakeClock())
    run(store, b"SET", b"k", b"v")
    assert run(store, b"EXPIRE", b"k", b"-1") == 1
    assert run(store, b"EXISTS", b"k") == 0


def test_set_nx_xx_and_integer_commands() -> None:
    store = Store(FakeClock())
    assert run(store, b"SET", b"n", b"4", b"NX") == b"OK"
    assert run(store, b"SET", b"n", b"9", b"NX") is None
    assert run(store, b"SET", b"n", b"5", b"XX") == b"OK"
    assert run(store, b"INCR", b"n") == 6
    assert run(store, b"DECR", b"n") == 5
    assert run(store, b"INCRBY", b"n", b"-7") == -2
    run(store, b"SET", b"bad", b"four")
    error = dispatch([b"INCR", b"bad"], store)
    assert isinstance(error, CommandError)
    assert str(error) == "value is not an integer or out of range"


def test_list_commands_and_wrong_type() -> None:
    store = Store(FakeClock())
    assert run(store, b"LPUSH", b"l", b"a", b"b") == 2
    assert run(store, b"RPUSH", b"l", b"c") == 3
    assert run(store, b"LRANGE", b"l", b"0", b"-1") == [b"b", b"a", b"c"]
    assert run(store, b"LLEN", b"l") == 3
    assert run(store, b"LPOP", b"l") == b"b"
    assert run(store, b"RPOP", b"l") == b"c"
    assert run(store, b"LRANGE", b"l", b"-1", b"-1") == [b"a"]
    run(store, b"SET", b"str", b"text")
    error = dispatch([b"LLEN", b"str"], store)
    assert isinstance(error, CommandError)
    assert str(error).startswith("WRONGTYPE")


def test_active_sweeper_removes_expired_keys() -> None:
    clock = FakeClock()
    store = Store(clock)
    run(store, b"SET", b"a", b"1", b"PX", b"50")
    run(store, b"SET", b"b", b"2")
    clock.now += 0.05
    assert store.sweep_expired() == 1
    assert run(store, b"EXISTS", b"a", b"b") == 1


def test_expire_at_and_set_pxat_are_supported_for_replay() -> None:
    clock = FakeClock()
    store = Store(clock)
    run(store, b"SET", b"k", b"v", b"PXAT", str(int(__import__("time").time() * 1000 + 60_000)).encode())
    assert run(store, b"TTL", b"k") > 0
    assert run(store, b"PEXPIREAT", b"k", b"1") == 1
    assert run(store, b"GET", b"k") is None
