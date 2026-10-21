# Design decisions

## Single-threaded asyncio ownership

The event loop is the only writer to the in-memory store. Command execution contains no `await`, so concurrent clients cannot interleave a single mutation and a transaction executes in one event-loop turn. This avoids lock overhead and makes behavior easy to inspect. CPU-heavy commands would block all clients, and this model does not provide durable transaction grouping across a crash.

## RESP2 first

The parser and encoders implement RESP2, including its array-form pub/sub acknowledgements and messages. RESP2 keeps the scope compatible with redis-cli and redis-py without introducing RESP3 negotiation state.

## Expiration in two places

Every key access checks its deadline and lazily removes stale values. A 100 ms active sampler handles expired keys that are never accessed. The injected monotonic clock keeps deterministic unit tests; persisted expirations are translated to wall-clock absolute milliseconds so downtime counts.

## AOF policies

Successful mutation commands are appended in RESP form. `always` fsyncs each append, `everysec` runs a background sync task, and `no` relies on OS buffering. AOF rewrite serializes live values and absolute deadlines to a temporary file, fsyncs it, then atomically replaces the old file. A truncated final command is ignored during replay.

## Slow subscribers

Each connection has a bounded output queue. When it fills, minikv disconnects that connection and removes its subscriptions. This is a simple memory bound and ensures one stalled consumer does not block publishers.

## Performance claims

Benchmark outputs are generated for the actual host and include the Redis version, OS, CPU, Python version, AOF policy, and pipeline depth. Results are not copied from other machines. Python command dispatch and per-command networking are expected to make minikv slower than Redis's native implementation.
