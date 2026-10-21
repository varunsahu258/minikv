import logging
import os
import signal
import subprocess
import sys
import time

import redis

from minikv.aof import AOF
from minikv.store import Store


def test_replay_ignores_truncated_final_command(tmp_path, caplog) -> None:
    path = tmp_path / "appendonly.aof"
    path.write_bytes(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$3\r\n")
    store = Store()
    with caplog.at_level(logging.WARNING):
        AOF(path, "always").replay(store)
    assert store.get(b"a") == b"1"
    assert store.get(b"b") is None
    assert "truncated final AOF record" in caplog.text


def test_expired_key_does_not_return_after_restart(tmp_path) -> None:
    path = tmp_path / "appendonly.aof"
    store = Store()
    from minikv.commands import dispatch

    dispatch([b"SET", b"short", b"v", b"PX", b"30"], store)
    # Persist through the same normalization used for successful server writes.
    from minikv.aof import canonical_write

    record = canonical_write([b"SET", b"short", b"v", b"PX", b"30"], store, b"OK")
    assert record is not None
    AOF(path, "always").append(record)
    time.sleep(0.05)
    restored = Store()
    AOF(path).replay(restored)
    assert restored.get(b"short") is None


def test_rewrite_compacts_and_preserves_values(tmp_path) -> None:
    path = tmp_path / "appendonly.aof"
    aof = AOF(path, "always")
    store = Store()
    for index in range(30):
        store.set(b"key", str(index).encode())
        aof.append([b"SET", b"key", str(index).encode()])
    before = path.stat().st_size
    aof.rewrite(store)
    after = path.stat().st_size
    restored = Store()
    aof.replay(restored)
    assert after < before
    assert restored.get(b"key") == b"29"


def test_sigkill_restart_recovers_acknowledged_writes(tmp_path) -> None:
    path = tmp_path / "appendonly.aof"
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "minikv",
            "--appendfsync",
            "always",
            "--appendfilename",
            str(path),
        ],
        cwd=os.getcwd(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = redis.Redis(host="127.0.0.1", port=6380, protocol=2)
    try:
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            try:
                if client.ping():
                    break
            except redis.RedisError:
                time.sleep(0.05)
        else:
            raise AssertionError("minikv did not start")
        for index in range(100):
            assert client.set(f"crash:{index}", f"value:{index}")
        os.kill(process.pid, signal.SIGKILL)
        process.wait(timeout=5)
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "minikv",
                "--appendfsync",
                "always",
                "--appendfilename",
                str(path),
            ],
            cwd=os.getcwd(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            try:
                value = client.get("crash:99")
                if value == b"value:99":
                    break
            except redis.RedisError:
                time.sleep(0.05)
        assert value == b"value:99"
        assert client.get("crash:0") == b"value:0"
    finally:
        client.close()
        if process.poll() is None:
            os.kill(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
