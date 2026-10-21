"""Run the server with ``python -m minikv``."""

import argparse
import asyncio
import signal

from minikv.server import MiniKVServer


async def main(aof_path: str, appendfsync: str) -> None:
    server = MiniKVServer(aof_path=aof_path, appendfsync=appendfsync)
    await server.start()
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stopped.set)
    await stopped.wait()
    await server.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the minikv RESP2 server")
    parser.add_argument("--appendonly", action="store_true", default=True)
    parser.add_argument("--appendfilename", default="appendonly.aof")
    parser.add_argument(
        "--appendfsync", choices=("always", "everysec", "no"), default="everysec"
    )
    options = parser.parse_args()
    try:
        asyncio.run(main(options.appendfilename, options.appendfsync))
    except KeyboardInterrupt:
        pass
