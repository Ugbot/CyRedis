"""
Python wrapper to expose worker coordination classes under cy_redis.web_app_support.
Imports are best-effort to avoid crashing test collection if optional components fail.
"""

try:
    from cy_redis.web.web_app_support import WebAppSupport as WebApplicationSupport
except Exception:
    WebApplicationSupport = None

try:
    from cy_redis.workers.lifecycle_manager import LifecycleManager
except Exception:
    LifecycleManager = None

try:
    from cy_redis.workers.worker_coordinator import WorkerCoordinator
except Exception:
    WorkerCoordinator = None

try:
    from cy_redis.workers.worker_queue import WorkerQueue
except Exception:
    WorkerQueue = None

try:
    from cy_redis.auth.session_manager import SessionManager
except Exception:
    SessionManager = None

try:
    from cy_redis.workers.multi_session_tracker import MultiSessionTracker
except Exception:
    MultiSessionTracker = None

try:
    from cy_redis.data.concurrent_shared_dict import ConcurrentSharedDict
except Exception:
    ConcurrentSharedDict = None

__all__ = [
    "WebApplicationSupport",
    "LifecycleManager",
    "WorkerCoordinator",
    "WorkerQueue",
    "SessionManager",
    "MultiSessionTracker",
    "ConcurrentSharedDict",
]
