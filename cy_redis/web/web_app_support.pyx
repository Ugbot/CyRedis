# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: nonecheck=False
# distutils: language = c

"""
Web Application Support Module for CyRedis
Provides FastAPI-style features including worker queues, token management,
session lifecycle, 2FA, and multi-session tracking.
"""

import asyncio
import signal
import sys
import threading
from typing import Dict, List, Optional, Any, Union

# Import core Redis functionality (C-level for direct use in cdef classes)
from cy_redis.core.cy_redis_client cimport CyRedisClient

# Import all components via Python-level imports (no pxd needed)
from cy_redis.auth.token_manager import TokenManager
from cy_redis.auth.session_manager import SessionManager
from cy_redis.auth.two_factor_auth import TwoFactorAuth
from cy_redis.auth.password_reset_manager import PasswordResetManager

from cy_redis.workers.worker_queue import WorkerQueue
from cy_redis.workers.lifecycle_manager import LifecycleManager
from cy_redis.workers.worker_coordinator import WorkerCoordinator
from cy_redis.workers.multi_session_tracker import MultiSessionTracker

from cy_redis.data.shared_state_manager import SharedStateManager
from cy_redis.data.concurrent_shared_dict import ConcurrentSharedDict

from cy_redis.utils.redis_iterators import RedisStreamIterator, RedisListIterator, RedisPubSubIterator


# ===== ENHANCED WEB APP SUPPORT WITH WORKER COORDINATION =====

class WebAppSupport:
    """
    Enhanced main class with worker coordination and graceful shutdown.
    Acts as a coordinator for all web application support components.
    """

    def __init__(self, redis_client: CyRedisClient = None,
                 host: str = "localhost", port: int = 6379):
        # Preconditions: a default-constructed client needs a valid endpoint.
        if not host:
            raise ValueError("host must be a non-empty string")
        if not (0 < port <= 65535):
            raise ValueError("port must be in 1..65535")

        self.redis_client = redis_client or CyRedisClient(host, port)
        assert self.redis_client is not None, "redis_client must be set"

        # Initialize all managers
        self.worker_queue = WorkerQueue("default", self.redis_client)
        self.token_manager = TokenManager(self.redis_client)
        self.session_manager = SessionManager(self.redis_client)
        self.two_factor_auth = TwoFactorAuth(self.redis_client)
        self.password_reset = PasswordResetManager(self.redis_client)
        self.multi_session_tracker = MultiSessionTracker(self.redis_client, self.session_manager)
        self.shared_state = SharedStateManager(self.redis_client)

        # Enhanced lifecycle manager with worker coordination
        self.lifecycle_manager = LifecycleManager(self.redis_client)

        # Worker coordinator for scaling and recovery
        self.worker_coordinator = WorkerCoordinator(self.redis_client)

        # Postcondition: every coordinator the hooks rely on is constructed.
        assert self.lifecycle_manager is not None, "lifecycle_manager required"
        assert self.worker_coordinator is not None, "worker_coordinator required"

        # Setup enhanced lifecycle hooks
        self._setup_enhanced_lifecycle_hooks()

    def _setup_enhanced_lifecycle_hooks(self):
        """Setup enhanced lifecycle hooks with worker coordination"""
        assert self.lifecycle_manager is not None, "lifecycle_manager required"
        assert self.worker_queue is not None, "worker_queue required"

        def enhanced_startup():
            self.worker_queue.start()
            print("WebAppSupport with worker coordination initialized")

            # Start background monitoring
            import threading
            monitor_thread = threading.Thread(target=self._monitor_workers, daemon=True)
            monitor_thread.start()

        def enhanced_shutdown():
            # Graceful shutdown by default
            self.lifecycle_manager.shutdown(graceful=True)
            self.worker_queue.stop()
            print("WebAppSupport with worker coordination shutdown")

        def immediate_shutdown():
            # Immediate shutdown for SIGKILL scenarios
            self.lifecycle_manager.shutdown(graceful=False)

        # Add hooks with priorities (0 = highest priority)
        self.lifecycle_manager.add_startup_hook(enhanced_startup, priority=0)
        self.lifecycle_manager.add_shutdown_hook(enhanced_shutdown, priority=0)

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, lambda sig, frame: enhanced_shutdown())
        signal.signal(signal.SIGINT, lambda sig, frame: immediate_shutdown())

    def _monitor_workers(self):
        """Background worker monitoring for dead worker detection.

        Exit invariant: this runs on a daemon thread whose lifetime is bounded
        by the process — it terminates when the interpreter shuts the daemon
        thread down. Each iteration sleeps, so the loop cannot busy-spin.
        """
        import time
        assert self.worker_coordinator is not None, "worker_coordinator required"
        while True:
            try:
                dead_workers = self.worker_coordinator.detect_dead_workers()
                # Bounded fan-out: one recovery call per detected worker.
                for worker_id in (dead_workers or []):
                    self.worker_coordinator.handle_dead_worker(worker_id)

                time.sleep(30)  # Check every 30 seconds

            except Exception as e:
                print(f"Worker monitoring error: {e}")
                time.sleep(60)  # Wait longer on error

    def initialize(self):
        """Initialize the enhanced web app support system"""
        self.lifecycle_manager.initialize()

    def shutdown(self, graceful: bool = True):
        """Shutdown the enhanced web app support system"""
        self.lifecycle_manager.shutdown(graceful)

    def get_worker_info(self) -> Dict[str, Any]:
        """Get information about this worker"""
        return self.lifecycle_manager.get_worker_stats()

    def get_cluster_info(self) -> Dict[str, Any]:
        """Get information about the entire worker cluster"""
        return self.worker_coordinator.get_cluster_stats()

    def force_worker_recovery(self, worker_id: str):
        """Force recovery of a specific worker"""
        self.worker_coordinator.handle_dead_worker(worker_id)

    # Convenience methods for common operations
    def create_user_session(self, user_id: str, device_info: Dict[str, Any] = None) -> str:
        """Create a new user session with multi-session tracking"""
        # Precondition: a session must belong to an identified user.
        if not user_id:
            raise ValueError("user_id must be a non-empty string")

        session_id = self.session_manager.create_session(user_id)
        # Postcondition: the session manager must hand back a usable id before
        # we register it for multi-session tracking.
        assert session_id, "create_session must return a non-empty session id"
        self.multi_session_tracker.register_session(user_id, session_id, device_info)
        return session_id

    def authenticate_user(self, user_id: str, password: str) -> Dict[str, Any]:
        """Authenticate user and return tokens and session info"""
        # This would integrate with your user authentication system
        access_token = self.token_manager.create_access_token(user_id)
        refresh_token = self.token_manager.create_refresh_token(user_id)

        return {
            'access_token': access_token,
            'refresh_token': refresh_token,
            'token_type': 'bearer'
        }

    def verify_user_access(self, token: str, required_claims: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Verify user access token and check claims"""
        return self.token_manager.verify_user_access(token, required_claims)

    # JWT authentication methods
    def jwt_middleware(self, token: str, required_permissions: List[str] = None,
                      required_scopes: List[str] = None) -> Optional[Dict[str, Any]]:
        """JWT authentication middleware for protecting endpoints."""
        return self.token_manager.jwt_middleware(token, required_permissions, required_scopes)

    def require_auth(self, token: str, user_id: str = None) -> bool:
        """Simple authentication check."""
        return self.token_manager.require_auth(token, user_id)

    def require_permissions(self, token: str, permissions: List[str]) -> bool:
        """Check if token has required permissions."""
        return self.token_manager.require_permissions(token, permissions)

    def require_scopes(self, token: str, scopes: List[str]) -> bool:
        """Check if token has required scopes."""
        return self.token_manager.require_scopes(token, scopes)

    def create_websocket_token(self, user_id: str, session_id: str = None,
                              permissions: List[str] = None, expiry: int = None) -> str:
        """Create a JWT token for websocket connections."""
        return self.token_manager.create_websocket_token(user_id, session_id, permissions, expiry)

    def create_api_token(self, user_id: str, scopes: List[str] = None,
                        expiry: int = None) -> str:
        """Create an API token for programmatic access."""
        return self.token_manager.create_api_token(user_id, scopes, expiry)

    # Worker queue methods
    def enqueue_task(self, task_data: Dict[str, Any], delay: int = 0) -> str:
        return self.worker_queue.enqueue(task_data, delay)

    def get_queue_stats(self) -> Dict[str, Any]:
        return self.worker_queue.get_queue_stats()

    # Session management methods
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        return self.session_manager.get_session(session_id)

    def destroy_session(self, session_id: str):
        return self.session_manager.destroy_session(session_id)

    # 2FA methods
    def enable_2fa(self, user_id: str) -> Dict[str, Any]:
        return self.two_factor_auth.enable_2fa(user_id)

    def verify_totp(self, user_id: str, token: str) -> bool:
        return self.two_factor_auth.verify_totp(user_id, token)

    # Token management methods
    def refresh_access_token(self, refresh_token: str) -> Optional[str]:
        return self.token_manager.refresh_access_token(refresh_token)

    def revoke_token(self, token: str):
        return self.token_manager.revoke_token(token)

    # Shared state methods
    def increment_counter(self, counter_name: str, increment: int = 1) -> int:
        return self.shared_state.increment_counter(counter_name, increment)

    def get_counter(self, counter_name: str) -> int:
        return self.shared_state.get_counter(counter_name)

    def set_shared_data(self, key: str, data: Any, expiry: int = None):
        return self.shared_state.set_shared_data(key, data, expiry)

    def get_shared_data(self, key: str) -> Any:
        return self.shared_state.get_shared_data(key)

    # Redis streaming iterators
    def stream_iterator(self, stream_key: str, consumer_group: str = None,
                       consumer_name: str = None, batch_size: int = 10,
                       block_ms: int = 1000) -> RedisStreamIterator:
        """Create an async iterator for Redis streams."""
        return RedisStreamIterator(
            self.redis_client, stream_key, consumer_group,
            consumer_name, batch_size, block_ms
        )

    def list_iterator(self, list_key: str, batch_size: int = 10,
                     block_ms: int = 1000) -> RedisListIterator:
        """Create an async iterator for Redis lists."""
        return RedisListIterator(
            self.redis_client, list_key, batch_size, block_ms
        )

    def pubsub_iterator(self, channels: Union[str, List[str]],
                       timeout_ms: int = 1000) -> RedisPubSubIterator:
        """Create an async iterator for Redis pub/sub."""
        return RedisPubSubIterator(
            self.redis_client, channels, timeout_ms
        )

    # Worker coordination methods
    def get_worker_info(self) -> Dict[str, Any]:
        """Get information about this worker"""
        return self.lifecycle_manager.get_worker_stats()

    def get_cluster_info(self) -> Dict[str, Any]:
        """Get information about the entire worker cluster"""
        return self.worker_coordinator.get_cluster_stats()

    def force_worker_recovery(self, worker_id: str):
        """Force recovery of a specific worker"""
        self.worker_coordinator.handle_dead_worker(worker_id)

    def shutdown_graceful(self):
        """Perform graceful shutdown with workload yielding"""
        self.shutdown(graceful=True)

    def shutdown_immediate(self):
        """Perform immediate shutdown without waiting"""
        self.shutdown(graceful=False)

    # Shared dictionary methods
    def get_shared_dict(self, name: str) -> ConcurrentSharedDict:
        """Get a concurrent shared dictionary by name"""
        return ConcurrentSharedDict(name, self.redis_client)

    def create_shared_dict(self, name: str, initial_data: Dict[str, Any] = None) -> ConcurrentSharedDict:
        """Create a new concurrent shared dictionary"""
        shared_dict = ConcurrentSharedDict(name, self.redis_client)
        if initial_data:
            shared_dict.update(initial_data)
        return shared_dict


# ===== PYTHON WRAPPER CLASS =====

class WebApplicationSupport:
    """
    Python wrapper for WebAppSupport functionality with enhanced worker coordination.
    """

    def __init__(self, redis_client: CyRedisClient = None,
                 host: str = "localhost", port: int = 6379):
        # Preconditions: a default-constructed client needs a valid endpoint.
        if not host:
            raise ValueError("host must be a non-empty string")
        if not (0 < port <= 65535):
            raise ValueError("port must be in 1..65535")

        self.redis_client = redis_client or CyRedisClient(host, port)
        assert self.redis_client is not None, "redis_client must be set"

        # Initialize all managers
        self.worker_queue = WorkerQueue("default", self.redis_client)
        self.token_manager = TokenManager(self.redis_client)
        self.session_manager = SessionManager(self.redis_client)
        self.two_factor_auth = TwoFactorAuth(self.redis_client)
        self.password_reset = PasswordResetManager(self.redis_client)
        self.multi_session_tracker = MultiSessionTracker(self.redis_client, self.session_manager)
        self.shared_state = SharedStateManager(self.redis_client)

        # Enhanced lifecycle manager with worker coordination
        self.lifecycle_manager = LifecycleManager(self.redis_client)

        # Worker coordinator for scaling and recovery
        self.worker_coordinator = WorkerCoordinator(self.redis_client)

        # Postcondition: every coordinator the hooks rely on is constructed.
        assert self.lifecycle_manager is not None, "lifecycle_manager required"
        assert self.worker_coordinator is not None, "worker_coordinator required"

        # Setup enhanced lifecycle hooks
        self._setup_enhanced_lifecycle_hooks()

    def _setup_enhanced_lifecycle_hooks(self):
        """Setup enhanced lifecycle hooks with worker coordination"""
        assert self.lifecycle_manager is not None, "lifecycle_manager required"
        assert self.worker_queue is not None, "worker_queue required"

        def enhanced_startup():
            self.worker_queue.start()
            print("WebAppSupport with worker coordination initialized")

            # Start background monitoring
            import threading
            monitor_thread = threading.Thread(target=self._monitor_workers, daemon=True)
            monitor_thread.start()

        def enhanced_shutdown():
            # Graceful shutdown by default
            self.lifecycle_manager.shutdown(graceful=True)
            self.worker_queue.stop()
            print("WebAppSupport with worker coordination shutdown")

        def immediate_shutdown():
            # Immediate shutdown for SIGKILL scenarios
            self.lifecycle_manager.shutdown(graceful=False)

        # Add hooks with priorities (0 = highest priority)
        self.lifecycle_manager.add_startup_hook(enhanced_startup, priority=0)
        self.lifecycle_manager.add_shutdown_hook(enhanced_shutdown, priority=0)

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, lambda sig, frame: enhanced_shutdown())
        signal.signal(signal.SIGINT, lambda sig, frame: immediate_shutdown())

    def _monitor_workers(self):
        """Background worker monitoring for dead worker detection.

        Exit invariant: this runs on a daemon thread whose lifetime is bounded
        by the process — it terminates when the interpreter shuts the daemon
        thread down. Each iteration sleeps, so the loop cannot busy-spin.
        """
        import time
        assert self.worker_coordinator is not None, "worker_coordinator required"
        while True:
            try:
                dead_workers = self.worker_coordinator.detect_dead_workers()
                # Bounded fan-out: one recovery call per detected worker.
                for worker_id in (dead_workers or []):
                    self.worker_coordinator.handle_dead_worker(worker_id)

                time.sleep(30)  # Check every 30 seconds

            except Exception as e:
                print(f"Worker monitoring error: {e}")
                time.sleep(60)  # Wait longer on error

    def initialize(self):
        """Initialize the web app support system"""
        self.lifecycle_manager.initialize()

    def shutdown(self):
        """Shutdown the web app support system"""
        self.lifecycle_manager.shutdown()

    # Convenience methods for common operations
    def create_user_session(self, user_id: str, device_info: Dict[str, Any] = None) -> str:
        """Create a new user session with multi-session tracking"""
        # Precondition: a session must belong to an identified user.
        if not user_id:
            raise ValueError("user_id must be a non-empty string")

        session_id = self.session_manager.create_session(user_id)
        # Postcondition: the session manager must hand back a usable id before
        # we register it for multi-session tracking.
        assert session_id, "create_session must return a non-empty session id"
        self.multi_session_tracker.register_session(user_id, session_id, device_info)
        return session_id

    def authenticate_user(self, user_id: str, password: str) -> Dict[str, Any]:
        """Authenticate user and return tokens and session info"""
        # This would integrate with your user authentication system
        access_token = self.token_manager.create_access_token(user_id)
        refresh_token = self.token_manager.create_refresh_token(user_id)

        return {
            'access_token': access_token,
            'refresh_token': refresh_token,
            'token_type': 'bearer'
        }

    def verify_user_access(self, token: str, required_claims: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Verify user access token and check claims"""
        return self.token_manager.verify_user_access(token, required_claims)

    # JWT authentication methods
    def jwt_middleware(self, token: str, required_permissions: List[str] = None,
                      required_scopes: List[str] = None) -> Optional[Dict[str, Any]]:
        """JWT authentication middleware for protecting endpoints."""
        return self.token_manager.jwt_middleware(token, required_permissions, required_scopes)

    def require_auth(self, token: str, user_id: str = None) -> bool:
        """Simple authentication check."""
        return self.token_manager.require_auth(token, user_id)

    def require_permissions(self, token: str, permissions: List[str]) -> bool:
        """Check if token has required permissions."""
        return self.token_manager.require_permissions(token, permissions)

    def require_scopes(self, token: str, scopes: List[str]) -> bool:
        """Check if token has required scopes."""
        return self.token_manager.require_scopes(token, scopes)

    def create_websocket_token(self, user_id: str, session_id: str = None,
                              permissions: List[str] = None, expiry: int = None) -> str:
        """Create a JWT token for websocket connections."""
        return self.token_manager.create_websocket_token(user_id, session_id, permissions, expiry)

    def create_api_token(self, user_id: str, scopes: List[str] = None,
                        expiry: int = None) -> str:
        """Create an API token for programmatic access."""
        return self.token_manager.create_api_token(user_id, scopes, expiry)

    # Worker queue methods
    def enqueue_task(self, task_data: Dict[str, Any], delay: int = 0) -> str:
        return self.worker_queue.enqueue(task_data, delay)

    def get_queue_stats(self) -> Dict[str, Any]:
        return self.worker_queue.get_queue_stats()

    # Session management methods
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        return self.session_manager.get_session(session_id)

    def destroy_session(self, session_id: str):
        return self.session_manager.destroy_session(session_id)

    # 2FA methods
    def enable_2fa(self, user_id: str) -> Dict[str, Any]:
        return self.two_factor_auth.enable_2fa(user_id)

    def verify_totp(self, user_id: str, token: str) -> bool:
        return self.two_factor_auth.verify_totp(user_id, token)

    # Token management methods
    def refresh_access_token(self, refresh_token: str) -> Optional[str]:
        return self.token_manager.refresh_access_token(refresh_token)

    def revoke_token(self, token: str):
        return self.token_manager.revoke_token(token)

    # Shared state methods
    def increment_counter(self, counter_name: str, increment: int = 1) -> int:
        return self.shared_state.increment_counter(counter_name, increment)

    def get_counter(self, counter_name: str) -> int:
        return self.shared_state.get_counter(counter_name)

    def set_shared_data(self, key: str, data: Any, expiry: int = None):
        return self.shared_state.set_shared_data(key, data, expiry)

    def get_shared_data(self, key: str) -> Any:
        return self.shared_state.get_shared_data(key)

    # Redis streaming iterators
    def stream_iterator(self, stream_key: str, consumer_group: str = None,
                       consumer_name: str = None, batch_size: int = 10,
                       block_ms: int = 1000):
        """Create an async iterator for Redis streams."""
        return RedisStreamIterator(
            self.redis_client, stream_key, consumer_group,
            consumer_name, batch_size, block_ms
        )

    def list_iterator(self, list_key: str, batch_size: int = 10,
                     block_ms: int = 1000):
        """Create an async iterator for Redis lists."""
        return RedisListIterator(
            self.redis_client, list_key, batch_size, block_ms
        )

    def pubsub_iterator(self, channels, timeout_ms: int = 1000):
        """Create an async iterator for Redis pub/sub."""
        return RedisPubSubIterator(
            self.redis_client, channels, timeout_ms
        )

    # Worker coordination methods
    def get_worker_info(self) -> Dict[str, Any]:
        """Get information about this worker"""
        return self.lifecycle_manager.get_worker_stats()

    def get_cluster_info(self) -> Dict[str, Any]:
        """Get information about the entire worker cluster"""
        return self.worker_coordinator.get_cluster_stats()

    def force_worker_recovery(self, worker_id: str):
        """Force recovery of a specific worker"""
        self.worker_coordinator.handle_dead_worker(worker_id)

    def shutdown_graceful(self):
        """Perform graceful shutdown with workload yielding"""
        self.lifecycle_manager.shutdown(graceful=True)

    def shutdown_immediate(self):
        """Perform immediate shutdown without waiting"""
        self.lifecycle_manager.shutdown(graceful=False)

    # Shared dictionary methods
    def get_shared_dict(self, name: str):
        """Get a concurrent shared dictionary by name"""
        return ConcurrentSharedDict(name, self.redis_client)

    def create_shared_dict(self, name: str, initial_data: Dict[str, Any] = None):
        """Create a new concurrent shared dictionary"""
        shared_dict = ConcurrentSharedDict(name, self.redis_client)
        if initial_data:
            shared_dict.update(initial_data)
        return shared_dict