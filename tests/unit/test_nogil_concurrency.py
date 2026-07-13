"""The blocking hiredis calls release the GIL, so concurrent blocking commands
on separate connections overlap instead of serializing.

If the GIL were held across redisCommandArgv/redisGetReply, N threads each
issuing a 1-second blocking BLPOP would take ~N seconds; with the GIL released
they overlap to ~1 second.
"""

import threading
import time
import uuid

import pytest

from cy_redis import CyRedisClient


@pytest.mark.redis
def test_blocking_commands_run_concurrently():
    n = 8
    prefix = "test:nogil:" + uuid.uuid4().hex
    client = CyRedisClient(host="localhost", port=6379, max_connections=n + 4)

    def block(i):
        # Key is never pushed, so BLPOP blocks for its full 1s timeout.
        client.execute_command(["BLPOP", f"{prefix}:{i}", "1"])

    threads = [threading.Thread(target=block, args=(i,)) for i in range(n)]
    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - start

    # Overlapped should be ~1s. Allow generous headroom for scheduling, but it
    # must be far below the ~n seconds a GIL-serialized run would take.
    assert elapsed < (
        n * 0.5
    ), f"{n} concurrent 1s BLPOPs took {elapsed:.2f}s — looks GIL-serialized"
