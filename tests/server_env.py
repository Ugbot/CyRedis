"""Address of the server under test.

The suite runs against Redis and against Valkey, which listen on different
ports in CI, so tests must never hardcode ``localhost:6379``. Import these
instead; ``REDIS_HOST``/``REDIS_PORT`` select the instance.
"""

import os

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
