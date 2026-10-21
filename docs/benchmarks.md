# Benchmark results

Run date: 2026-10-18T16:43:00Z
Machine: Apple M2 · macOS-26.5.2-arm64-arm-64bit-Mach-O · Python 3.14.7
Redis: Redis server v=7.4.11 sha=00000000:0 malloc=jemalloc-5.3.0 bits=64 build=c8d8130a4710f7c0

Run with `bench/run.sh`. Each row comes from `redis-benchmark -t set,get,incr,lpush -n 200000 -c 50`; pipeline depth is shown separately.
The Redis comparison uses its default in-memory behavior with persistence disabled. minikv keeps AOF enabled with `appendfsync no`, so its writes still serialize to the AOF. Both endpoints are reached through the same redis-benchmark client; Docker is used when redis-benchmark is not installed locally.
Results are machine- and load-specific, not a general Redis performance claim.

| Target | Pipeline | SET req/s | GET req/s | INCR req/s | LPUSH req/s |
|---|---:|---:|---:|---:|---:|
| Redis | 1 | 24,925 | 25,684 | 23,618 | 25,553 |
| Redis | 16 | 389,864 | 301,205 | 347,826 | 302,572 |
| minikv | 1 | 29,678 | 30,907 | 29,155 | 18,648 |
| minikv | 16 | 114,220 | 157,729 | 129,199 | 18,720 |

## Interpretation

At pipeline depth 1 in this run, minikv reported higher SET, GET, and INCR throughput than the containerized Redis endpoint, and lower LPUSH throughput. This is a single measurement and should not be read as evidence that Python is generally faster: Redis is running in Docker while minikv runs natively on macOS, and fixed network / virtualization costs are large at this request rate. At pipeline depth 16, Redis led all four commands (about 1.9× GET, 2.7× INCR, 3.4× SET, and 16× LPUSH). Redis uses optimized native code; minikv handles each request through Python parsing, dispatch, and asyncio streams, and writes additionally append RESP to the AOF. Pipelining amortizes transport overhead but cannot remove per-command Python work.

LPUSH is particularly weak at deep pipelines because this showcase stores lists in Python lists and inserts at index zero, which moves existing elements. Redis uses data structures with efficient head insertion. This is one concrete correctness-over-performance tradeoff that a production implementation would change.

### Profile and optimizations

The before and after profiles use cProfile on the server while `redis-benchmark -t set,get,incr,lpush -n 50000 -c 50 -P 16` drove 200,000 requests. Times include event-loop startup and shutdown and are profiler-instrumented, so use them to locate costs rather than as raw benchmark rates.

| Profile metric | Before | After |
|---|---:|---:|
| Total profiled process time | 14.94 s | 7.45 s |
| `AOF.append` cumulative time | 7.52 s | 1.15 s |
| `RESPParser.feed` cumulative time | 1.55 s | 1.59 s |
| response sender cumulative time | 0.94 s | 0.35 s |

Three changes were made based on the profile: keep the AOF file handle open (the largest win, avoiding 150,000 opens/closes in this profile), compact the parser buffer once after a chunk rather than after every parsed frame, and combine queued replies into one transport write/drain. The parser measurement is effectively flat in this workload; the cursor change reduces repeated compaction work but parsing itself remains a cost. Detailed raw profiles are in `bench/profile-before.txt` and `bench/profile-after.txt`.

| minikv operation | Pipeline | Before req/s | After req/s | Change |
|---|---:|---:|---:|---:|
| SET | 1 | 13,766 | 29,678 | +116% |
| GET | 1 | 33,767 | 30,907 | -9% |
| INCR | 1 | 12,909 | 29,155 | +126% |
| LPUSH | 1 | 10,799 | 18,648 | +73% |
| SET | 16 | 23,373 | 114,220 | +389% |
| GET | 16 | 119,617 | 157,729 | +32% |
| INCR | 16 | 24,041 | 129,199 | +437% |
| LPUSH | 16 | 9,628 | 18,720 | +94% |

These are one before/after run per configuration, not a statistical study. The small GET regression at pipeline 1 is within the noise of independent runs and was not optimized away. Machine metadata and full unrounded values are retained in `bench/results-before.json`, `bench/results.json`, and `bench/optimization-results.json`.

The numbers above measure this exact machine and configuration. No results are extrapolated to other hardware.
