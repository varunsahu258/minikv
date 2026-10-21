import random
import time

from minikv.protocol import RESPParser, RESPProtocolError


def test_parser_random_garbage_for_one_minute() -> None:
    randomizer = random.Random(731)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        parser = RESPParser(max_bulk_length=1024)
        for _ in range(randomizer.randint(1, 32)):
            chunk = randomizer.randbytes(randomizer.randint(1, 256))
            try:
                parser.feed(chunk)
            except RESPProtocolError:
                break
