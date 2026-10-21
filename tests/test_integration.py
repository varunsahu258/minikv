import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
import redis

from minikv.server import MiniKVServer


@pytest.fixture
def running_server(
    tmp_path,
) -> tuple[MiniKVServer, threading.Thread, asyncio.AbstractEventLoop]:
    loop = asyncio.new_event_loop()
    server = MiniKVServer(port=0, aof_path=str(tmp_path / "appendonly.aof"))
    ready = threading.Event()

    def run() -> None:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(server.start())
        assert server._server is not None
        server.port = server._server.sockets[0].getsockname()[1]
        ready.set()
        loop.run_forever()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    assert ready.wait(timeout=5)
    yield server, thread, loop
    future = asyncio.run_coroutine_threadsafe(server.close(), loop)
    future.result(timeout=5)
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=5)
    loop.close()


def test_redis_py_client_round_trip(running_server: tuple[MiniKVServer, threading.Thread, asyncio.AbstractEventLoop]) -> None:
    server, _thread, _loop = running_server
    # The server implements RESP2; explicitly select it with redis-py versions
    # that otherwise negotiate RESP3 during connection setup.
    client = redis.Redis(
        host="127.0.0.1", port=server.port, decode_responses=True, protocol=2
    )
    try:
        assert client.ping() is True
        assert client.set("a", "1") is True
        assert client.get("a") == "1"
        assert client.exists("a") == 1
        assert client.delete("a", "missing") == 1
        assert client.get("a") is None
        assert client.set("counter", "1", ex=10) is True
        assert client.ttl("counter") > 0
        assert client.incrby("counter", 2) == 3
        assert client.persist("counter") is True
        assert client.ttl("counter") == -1
        assert client.lpush("items", "a", "b") == 2
        assert client.rpush("items", "c") == 3
        assert client.lrange("items", 0, -1) == ["b", "a", "c"]
        assert client.lpop("items") == "b"
        assert client.rpop("items") == "c"
    finally:
        client.close()


def test_one_hundred_clients_increment_atomically(running_server) -> None:
    server, _thread, _loop = running_server
    def increment(_: int) -> None:
        client = redis.Redis(host="127.0.0.1", port=server.port, protocol=2)
        try:
            for _ in range(20):
                client.incr("shared-counter")
        finally:
            client.close()

    with ThreadPoolExecutor(max_workers=100) as pool:
        list(pool.map(increment, range(100)))
    client = redis.Redis(host="127.0.0.1", port=server.port, protocol=2)
    try:
        assert client.get("shared-counter") == b"2000"
    finally:
        client.close()


def test_pubsub_fans_out_to_fifty_subscribers(running_server) -> None:
    server, _thread, _loop = running_server
    subscribers = [redis.Redis(host="127.0.0.1", port=server.port, protocol=2).pubsub() for _ in range(50)]
    try:
        for subscriber in subscribers:
            subscriber.subscribe("broadcast")
            assert subscriber.get_message(timeout=2)["type"] == "subscribe"
        publisher = redis.Redis(host="127.0.0.1", port=server.port, protocol=2)
        assert publisher.publish("broadcast", "hello") == 50
        for subscriber in subscribers:
            message = subscriber.get_message(timeout=2)
            assert message is not None
            assert message["data"] == b"hello"
        publisher.close()
    finally:
        for subscriber in subscribers:
            subscriber.close()


def test_transactions_info_and_dbsize(running_server) -> None:
    server, _thread, _loop = running_server
    client = redis.Redis(host="127.0.0.1", port=server.port, protocol=2)
    try:
        pipe = client.pipeline()
        pipe.set("tx:a", "1")
        pipe.incr("tx:a")
        pipe.set("tx:b", "2")
        assert pipe.execute() == [True, 2, True]
        assert client.get("tx:a") == b"2"
        assert client.dbsize() >= 2
        assert "minikv_version" in client.info()
        pipe = client.pipeline()
        pipe.multi()
        pipe.set("tx:discard", "x")
        pipe.discard()
        assert client.get("tx:discard") is None
    finally:
        client.close()


def test_oversized_bulk_is_rejected(running_server) -> None:
    server, _thread, loop = running_server

    async def request() -> bytes:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        writer.write(b"$1048577\r\n")
        await writer.drain()
        response = await reader.readline()
        writer.close()
        await writer.wait_closed()
        return response

    assert asyncio.run_coroutine_threadsafe(request(), loop).result(timeout=3).startswith(
        b"-ERR Protocol error: bulk string exceeds"
    )
