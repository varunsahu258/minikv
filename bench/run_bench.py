#!/usr/bin/env python3
"""Run redis-benchmark against minikv and Redis and write reproducible results."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REDIS_CONTAINER = "minikv-bench-redis"
BENCH_CONTAINER = "minikv-bench-client"


def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=True, text=True, capture_output=True, **kwargs)


def cpu_model() -> str:
    if sys.platform == "darwin":
        result = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], check=False, capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip()
    return platform.processor() or "unknown"


def redis_version() -> str:
    if shutil.which("redis-server"):
        output = run(["redis-server", "--version"]).stdout
        return output.strip()
    output = run(["docker", "run", "--rm", "redis:7-alpine", "redis-server", "--version"]).stdout
    return output.strip()


def start_minikv(python: str, path: str) -> subprocess.Popen[bytes]:
    command = [python, "-m", "minikv", "--appendfsync", "no", "--appendfilename", path]
    return subprocess.Popen(command, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait_port(port: int, process: subprocess.Popen[bytes]) -> None:
    import socket

    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"minikv exited unexpectedly with {process.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"server on port {port} did not become ready")


def setup_redis(temp_directory: str) -> tuple[str, list[str], bool, subprocess.Popen[bytes] | None]:
    if shutil.which("redis-server") and shutil.which("redis-benchmark"):
        server = subprocess.Popen(
            ["redis-server", "--port", "6381", "--save", "", "--appendonly", "no",
             "--dir", temp_directory],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if server.poll() is not None:
                raise RuntimeError("local Redis exited unexpectedly")
            try:
                import socket
                with socket.create_connection(("127.0.0.1", 6381), timeout=0.2):
                    return redis_version(), ["redis-benchmark"], False, server
            except OSError:
                time.sleep(0.1)
        server.terminate()
        server.wait()
        raise TimeoutError("local Redis did not become ready on port 6381")
    run(["docker", "run", "-d", "--rm", "--name", REDIS_CONTAINER,
         "-p", "127.0.0.1:6381:6379", "redis:7-alpine", "redis-server",
         "--save", "", "--appendonly", "no"])
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        result = subprocess.run(["docker", "exec", REDIS_CONTAINER, "redis-cli", "ping"],
                                check=False, capture_output=True, text=True)
        if result.returncode == 0 and "PONG" in result.stdout:
            break
        time.sleep(0.2)
    else:
        raise TimeoutError("Redis container did not become ready")
    version = run(["docker", "exec", REDIS_CONTAINER, "redis-server", "--version"]).stdout.strip()
    run(["docker", "run", "-d", "--rm", "--name", BENCH_CONTAINER, "redis:7-alpine", "sleep", "3600"])
    return version, ["docker", "exec", BENCH_CONTAINER, "redis-benchmark"], True, None


def bench_command(client: list[str], port: int, test: str, pipeline: int, *, docker: bool) -> list[str]:
    host = "host.docker.internal" if docker else "127.0.0.1"
    return [*client, "-h", host, "-p", str(port), "-t", test, "-n", "200000",
            "-c", "50", "-P", str(pipeline), "-q"]


def parse_benchmark(output: str) -> dict[str, float]:
    results: dict[str, float] = {}
    for line in output.splitlines():
        match = re.search(r"^([A-Z]+):\s+([\d.]+)\s+requests per second", line.strip())
        if match:
            results[match.group(1).lower()] = float(match.group(2))
    return results


def write_markdown(results: dict[str, object], path: Path) -> None:
    metadata = results["metadata"]
    rows = results["results"]
    assert isinstance(metadata, dict) and isinstance(rows, list)
    lines = [
        "# Benchmark results",
        "",
        f"Run date: {metadata['timestamp_utc']}",
        f"Machine: {metadata['cpu_model']} · {metadata['os']} · Python {metadata['python']}",
        f"Redis: {metadata['redis_version']}",
        "",
        "Run with `bench/run.sh`. Each row comes from `redis-benchmark -t set,get,incr,lpush -n 200000 -c 50`; pipeline depth is shown separately.",
        "The Redis comparison uses its default in-memory behavior with persistence disabled. minikv keeps AOF enabled with `appendfsync no`, so its writes still serialize to the AOF. Both endpoints are reached through the same redis-benchmark client; Docker is used when redis-benchmark is not installed locally.",
        "Results are machine- and load-specific, not a general Redis performance claim.",
        "",
        "| Target | Pipeline | SET req/s | GET req/s | INCR req/s | LPUSH req/s |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    grouped: dict[tuple[str, int], dict[str, float]] = {}
    for item in rows:
        assert isinstance(item, dict)
        grouped.setdefault((str(item["target"]), int(item["pipeline"])), {})[
            str(item["command"])
        ] = float(item["requests_per_sec"])
    for (target, pipeline), numbers in sorted(grouped.items()):
        values = [f"{numbers.get(command, 0):,.0f}" for command in ("set", "get", "incr", "lpush")]
        lines.append(f"| {target} | {pipeline} | " + " | ".join(values) + " |")
    lines.extend([
        "",
        "## Interpretation",
        "",
        "minikv is expected to trail Redis: Redis executes its data path in optimized native code, while minikv parses and dispatches commands in Python on one event loop. Each command also crosses asyncio stream handling; write commands additionally encode and append RESP to the AOF. Pipelining amortizes socket and scheduling overhead but does not remove Python per-command work.",
        "",
        "The numbers above measure this exact machine and configuration. No results are extrapolated to other hardware.",
        "",
    ])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.parse_args()
    py = os.environ.get("PYTHON", sys.executable)
    temp = tempfile.TemporaryDirectory(prefix="minikv-bench-")
    minikv_proc: subprocess.Popen[bytes] | None = None
    docker = True
    redis_proc: subprocess.Popen[bytes] | None = None
    try:
        redis_ver, client, docker, redis_proc = setup_redis(temp.name)
        minikv_proc = start_minikv(py, str(Path(temp.name) / "minikv.aof"))
        wait_port(6380, minikv_proc)
        output_rows: list[dict[str, object]] = []
        for target, port in (("minikv", 6380), ("Redis", 6381)):
            for pipeline in (1, 16):
                cmd = bench_command(client, port, "set,get,incr,lpush", pipeline, docker=docker)
                completed = run(cmd)
                parsed = parse_benchmark(completed.stdout + completed.stderr)
                for test, rps in parsed.items():
                    output_rows.append({"target": target, "pipeline": pipeline,
                                        "command": test, "requests_per_sec": rps})
                print(f"completed {target} pipeline={pipeline}", flush=True)
        result: dict[str, object] = {
            "metadata": {
                "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "cpu_model": cpu_model(),
                "os": platform.platform(),
                "python": platform.python_version(),
                "redis_version": redis_ver,
                "minikv_aof": "enabled, appendfsync no",
                "benchmark": "redis-benchmark -t set,get,incr,lpush -n 200000 -c 50",
                "client_location": "Docker redis:7-alpine" if docker else "local host",
            },
            "results": output_rows,
        }
        output = ROOT / "bench" / "results.json"
        output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        write_markdown(result, ROOT / "docs" / "benchmarks.md")
        return 0
    finally:
        if minikv_proc is not None and minikv_proc.poll() is None:
            minikv_proc.send_signal(signal.SIGTERM)
            try:
                minikv_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                minikv_proc.kill()
                minikv_proc.wait()
        if redis_proc is not None and redis_proc.poll() is None:
            redis_proc.terminate()
            redis_proc.wait(timeout=5)
        if docker:
            subprocess.run(["docker", "rm", "-f", BENCH_CONTAINER, REDIS_CONTAINER],
                           check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        temp.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
