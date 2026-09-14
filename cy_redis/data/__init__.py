"""Cross-process data structures for CyRedis.

``CySharedDict`` is a Redis-replicated dictionary whose writes are serialised
through a distributed lock, so it is safe for many processes to share.
``CySharedDictManager`` hands out named dicts under the ``shared_dict:``
key prefix.
"""

from cy_redis.data.shared_dict import CySharedDict, CySharedDictManager

__all__ = [
    "CySharedDict",
    "CySharedDictManager",
]
