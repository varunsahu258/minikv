"""Asyncio TCP server with pub/sub, transactions, and bounded output queues."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import cast

from minikv.aof import AOF, canonical_write
from minikv.commands import CommandError, dispatch
from minikv.protocol import (
    RESPParser,
    RESPProtocolError,
    RESPValue,
    encode,
    encode_error,
)
from minikv.store import Store

SUBSCRIBE_COMMANDS = {b"SUBSCRIBE", b"UNSUBSCRIBE", b"PSUBSCRIBE", b"PUNSUBSCRIBE"}
CONNECTION_COMMANDS = SUBSCRIBE_COMMANDS | {b"PING"}


@dataclass(eq=False)
class Client:
    writer: asyncio.StreamWriter
    outgoing: asyncio.Queue[bytes | None] = field(
        default_factory=lambda: asyncio.Queue(maxsize=256)
    )
    channels: set[bytes] = field(default_factory=set)
    subscribed: bool = False
    in_multi: bool = False
    queued: list[RESPValue] = field(default_factory=list)
    closed: bool = False
    sender: asyncio.Task[None] | None = None


class MiniKVServer:
    """Single-event-loop server.

    Each connection has a bounded outbound queue. If it fills, that client is
    disconnected so a slow subscriber cannot stall publishers or other clients.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6380,
        *,
        clock: Callable[[], float] = time.monotonic,
        aof_path: str = "appendonly.aof",
        appendfsync: str = "everysec",
        idle_timeout: float = 60.0,
        max_bulk_length: int = 1024 * 1024,
        output_queue_limit: int = 256,
    ) -> None:
        self.host = host
        self.port = port
        self.store = Store(clock)
        self.aof = AOF(aof_path, appendfsync)
        self.idle_timeout = idle_timeout
        self.max_bulk_length = max_bulk_length
        self.output_queue_limit = output_queue_limit
        self._server: asyncio.Server | None = None
        self._sweeper: asyncio.Task[None] | None = None
        self._aof_syncer: asyncio.Task[None] | None = None
        self._clients: set[Client] = set()
        self._handlers: set[asyncio.Task[None]] = set()
        self._channels: dict[bytes, set[Client]] = {}

    async def start(self) -> None:
        self.aof.replay(self.store)
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        self._sweeper = asyncio.create_task(self._sweep_loop())
        if self.aof.fsync == "everysec":
            self._aof_syncer = asyncio.create_task(self.aof.sync_loop())

    async def serve_forever(self) -> None:
        if self._server is None:
            await self.start()
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            self._server = None
        for client in list(self._clients):
            self._close_client(client)
        senders = [client.sender for client in self._clients if client.sender is not None]
        if senders:
            _done, pending = await asyncio.wait(senders, timeout=1.0)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        current = asyncio.current_task()
        handlers = [task for task in self._handlers if task is not current]
        if handlers:
            _done, pending = await asyncio.wait(handlers, timeout=1.0)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        for task_name in ("_aof_syncer", "_sweeper"):
            task = getattr(self, task_name)
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                setattr(self, task_name, None)
        # Flush the append-only log for graceful shutdown in all fsync modes.
        await asyncio.to_thread(self.aof.close)

    async def _sweep_loop(self) -> None:
        while True:
            await asyncio.sleep(0.1)
            self.store.sweep_expired()

    async def _send_loop(self, client: Client) -> None:
        try:
            while True:
                payload = await client.outgoing.get()
                if payload is None:
                    break
                batch = bytearray(payload)
                stop = False
                while True:
                    try:
                        following = client.outgoing.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    if following is None:
                        stop = True
                        break
                    batch.extend(following)
                client.writer.write(batch)
                await client.writer.drain()
                if stop:
                    break
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            client.writer.close()
            try:
                await client.writer.wait_closed()
            except ConnectionError:
                pass

    def _queue(self, client: Client, payload: bytes) -> bool:
        if client.closed:
            return False
        try:
            client.outgoing.put_nowait(payload)
            return True
        except asyncio.QueueFull:
            # Slow consumers are disconnected when their bounded output queue fills.
            self._close_client(client)
            return False

    def _close_client(self, client: Client) -> None:
        if client.closed:
            return
        client.closed = True
        self._unsubscribe_all(client)
        try:
            client.outgoing.put_nowait(None)
        except asyncio.QueueFull:
            if client.sender is not None:
                client.sender.cancel()

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        current = asyncio.current_task()
        if current is not None:
            self._handlers.add(cast(asyncio.Task[None], current))
        client = Client(writer, asyncio.Queue(maxsize=self.output_queue_limit))
        self._clients.add(client)
        client.sender = asyncio.create_task(self._send_loop(client))
        parser = RESPParser(max_bulk_length=self.max_bulk_length)
        try:
            while not client.closed:
                try:
                    chunk = await asyncio.wait_for(
                        reader.read(65536), timeout=self.idle_timeout
                    )
                except TimeoutError:
                    break
                if not chunk:
                    break
                try:
                    commands = parser.feed(chunk)
                except RESPProtocolError as exc:
                    self._queue(client, encode_error(f"ERR Protocol error: {exc}"))
                    break
                for command in commands:
                    if not self._process(client, command):
                        break
        finally:
            self._close_client(client)
            if client.sender is not None:
                await asyncio.gather(client.sender, return_exceptions=True)
            self._clients.discard(client)
            if current is not None:
                self._handlers.discard(cast(asyncio.Task[None], current))

    def _process(self, client: Client, command: RESPValue) -> bool:
        if not isinstance(command, list) or not command or not isinstance(command[0], bytes):
            self._queue(client, encode_error("ERR Protocol error: expected command array"))
            return True
        name = command[0].upper()
        if client.subscribed and name not in CONNECTION_COMMANDS:
            self._queue(client, encode_error("ERR Can't execute command in subscribed mode"))
            return True
        if name == b"SUBSCRIBE":
            self._subscribe(client, command[1:])
            return True
        if name == b"UNSUBSCRIBE":
            self._unsubscribe(client, command[1:])
            return True
        if name in (b"PSUBSCRIBE", b"PUNSUBSCRIBE"):
            self._queue(client, encode_error("ERR pattern subscriptions are not supported"))
            return True
        if name == b"PING" and client.subscribed:
            args = command[1:]
            if len(args) > 1:
                self._queue(client, encode_error("ERR wrong number of arguments for 'ping' command"))
            else:
                self._queue(client, _array([b"pong", args[0] if args else None]))
            return True
        if name == b"MULTI":
            if client.in_multi:
                self._queue(client, encode_error("ERR MULTI calls can not be nested"))
            else:
                client.in_multi = True
                client.queued.clear()
                self._queue(client, b"+OK\r\n")
            return True
        if name == b"DISCARD":
            if not client.in_multi:
                self._queue(client, encode_error("ERR DISCARD without MULTI"))
            else:
                client.in_multi = False
                client.queued.clear()
                self._queue(client, b"+OK\r\n")
            return True
        if client.in_multi and name != b"EXEC":
            client.queued.append(command)
            self._queue(client, b"+QUEUED\r\n")
            return True
        if name == b"EXEC":
            if not client.in_multi:
                self._queue(client, encode_error("ERR EXEC without MULTI"))
                return True
            client.in_multi = False
            results: list[bytes] = []
            for queued in client.queued:
                result = self._execute(queued)
                results.append(
                    encode_error(f"ERR {result}")
                    if isinstance(result, CommandError)
                    else encode(result)
                )
            client.queued.clear()
            self._queue(client, b"*" + str(len(results)).encode() + b"\r\n" + b"".join(results))
            return True
        result = self._execute(command)
        self._queue(
            client,
            encode_error(f"ERR {result}") if isinstance(result, CommandError) else encode(result),
        )
        return True

    def _execute(self, command: RESPValue) -> RESPValue | CommandError:
        if isinstance(command, list) and command and all(
            isinstance(part, bytes) for part in command
        ):
            parts = cast(list[bytes], command)
            if parts[0].upper() == b"PUBLISH":
                if len(parts) != 3:
                    return CommandError("wrong number of arguments for 'publish' command")
                return self._publish(parts[1], parts[2])
        result = dispatch(command, self.store)
        if (
            isinstance(command, list)
            and command
            and isinstance(command[0], bytes)
            and cast(list[bytes], command)[0].upper() == b"BGREWRITEAOF"
        ):
            self.aof.rewrite(self.store)
        elif not isinstance(result, CommandError):
            record = canonical_write(command, self.store, result)
            if record is not None:
                self.aof.append(record)
        return result

    def _subscribe(self, client: Client, channels: list[RESPValue]) -> None:
        if not channels:
            self._queue(client, encode_error("ERR wrong number of arguments for 'subscribe' command"))
            return
        for raw_channel in channels:
            if not isinstance(raw_channel, bytes):
                self._queue(client, encode_error("ERR invalid channel"))
                return
            channel = raw_channel
            client.subscribed = True
            client.channels.add(channel)
            self._channels.setdefault(channel, set()).add(client)
            self._queue(client, _array([b"subscribe", channel, len(client.channels)]))

    def _unsubscribe(self, client: Client, channels: list[RESPValue]) -> None:
        targets = channels or list(client.channels)
        if not targets:
            client.subscribed = False
            self._queue(client, _array([b"unsubscribe", None, 0]))
            return
        for raw_channel in targets:
            if not isinstance(raw_channel, bytes):
                continue
            channel = raw_channel
            client.channels.discard(channel)
            subscribers = self._channels.get(channel)
            if subscribers is not None:
                subscribers.discard(client)
                if not subscribers:
                    self._channels.pop(channel, None)
            client.subscribed = bool(client.channels)
            self._queue(client, _array([b"unsubscribe", channel, len(client.channels)]))

    def _unsubscribe_all(self, client: Client) -> None:
        for channel in list(client.channels):
            subscribers = self._channels.get(channel)
            if subscribers is not None:
                subscribers.discard(client)
                if not subscribers:
                    self._channels.pop(channel, None)
        client.channels.clear()
        client.subscribed = False

    def _publish(self, channel: bytes, message: bytes) -> int:
        subscribers = list(self._channels.get(channel, ()))
        delivered = 0
        for client in subscribers:
            if self._queue(client, _array([b"message", channel, message])):
                delivered += 1
        return delivered


def _array(items: list[RESPValue]) -> bytes:
    return b"*" + str(len(items)).encode() + b"\r\n" + b"".join(encode(item) for item in items)


