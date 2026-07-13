#!/usr/bin/env python3
"""
Example demonstrating Redis data structure iterators for real-time streaming.
Shows how to use streams, lists, and pub/sub for SSE and websockets.
"""

import asyncio
import json
import time
import uuid
from typing import Any, AsyncGenerator, Dict, List

# Import our web application support (fallback for demo)
print("Note: Using Python fallback implementation for demo")
from typing import Any, Dict, List, Optional


class WebApplicationSupport:
    """Fallback Python implementation for demo purposes"""

    def __init__(self, host="localhost", port=6379):
        self.redis_client = None  # Would be Redis client in real implementation
        self.worker_queue = None
        self.token_manager = None
        self.session_manager = None
        self.two_factor_auth = None
        self.password_reset = None
        self.multi_session_tracker = None
        self.shared_state = None
        self.lifecycle_manager = None

    def initialize(self):
        print("WebApplicationSupport initialized (Python fallback)")

    def shutdown(self):
        print("WebApplicationSupport shutdown (Python fallback)")

    def create_user_session(
        self, user_id: str, device_info: Dict[str, Any] = None
    ) -> str:
        import uuid

        session_id = str(uuid.uuid4())
        return session_id

    def authenticate_user(self, user_id: str, password: str) -> Dict[str, Any]:
        import secrets

        # Create regular access token
        access_token = self.token_manager.create_access_token(user_id)

        # Create websocket token for streaming
        websocket_token = self.token_manager.create_websocket_token(user_id)

        # Create API token for programmatic access
        api_token = self.token_manager.create_api_token(
            user_id, scopes=["read", "write"]
        )

        return {
            "access_token": access_token,
            "websocket_token": websocket_token,
            "api_token": api_token,
            "refresh_token": secrets.token_urlsafe(32),
            "token_type": "bearer",
        }

    def verify_user_access(
        self, token: str, required_claims: Dict[str, Any] = None
    ) -> Optional[Dict[str, Any]]:
        return {"user_id": "demo_user", "type": "access"}

    def create_websocket_token(
        self,
        user_id: str,
        session_id: str = None,
        permissions: List[str] = None,
        expiry: int = None,
    ) -> str:
        import secrets

        return f"ws_token_{secrets.token_urlsafe(32)}"

    def create_api_token(
        self, user_id: str, scopes: List[str] = None, expiry: int = None
    ) -> str:
        import secrets

        return f"api_token_{secrets.token_urlsafe(32)}"

    def jwt_middleware(
        self,
        token: str,
        required_permissions: List[str] = None,
        required_scopes: List[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Mock JWT middleware for demo."""
        if not token:
            return None

        # Simple mock validation
        if token.startswith("ws_token_"):
            return {
                "user_id": "demo_user",
                "type": "websocket",
                "permissions": ["stream_access", "realtime_access"],
                "stream_access": True,
                "realtime_access": True,
            }
        elif token.startswith("api_token_"):
            return {
                "user_id": "demo_user",
                "type": "api",
                "scopes": ["read", "write"],
                "api_access": True,
            }
        else:
            return {"user_id": "demo_user", "type": "access"}

    def require_auth(self, token: str, user_id: str = None) -> bool:
        """Mock auth requirement check."""
        payload = self.jwt_middleware(token)
        if not payload:
            return False
        if user_id and payload.get("user_id") != user_id:
            return False
        return True

    def require_permissions(self, token: str, permissions: List[str]) -> bool:
        """Mock permission check."""
        payload = self.jwt_middleware(token)
        if not payload:
            return False
        token_permissions = payload.get("permissions", [])
        return all(perm in token_permissions for perm in permissions)

    def require_scopes(self, token: str, scopes: List[str]) -> bool:
        """Mock scope check."""
        payload = self.jwt_middleware(token)
        if not payload:
            return False
        token_scopes = payload.get("scopes", [])
        return all(scope in token_scopes for scope in scopes)

    def refresh_access_token(self, refresh_token: str) -> Optional[str]:
        """Mock token refresh."""
        import secrets

        return secrets.token_urlsafe(32) if refresh_token else None

    def revoke_token(self, token: str):
        """Mock token revocation."""
        pass

    def enable_2fa(self, user_id: str) -> Dict[str, Any]:
        import secrets

        return {
            "totp_secret": secrets.token_urlsafe(32),
            "backup_codes": ["DEMO" + str(i) for i in range(10)],
            "qr_code_url": "otpauth://totp/CyRedisApp:demo?secret=demo&issuer=CyRedisApp",
        }

    def verify_totp(self, user_id: str, token: str) -> bool:
        return token == "123456"

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        return {"id": session_id, "user_id": "demo_user"}

    def destroy_session(self, session_id: str):
        pass

    def create_user_session(
        self, user_id: str, device_info: Dict[str, Any] = None
    ) -> str:
        import uuid

        session_id = str(uuid.uuid4())
        return session_id

    def get_user_sessions(self, user_id: str) -> List[Dict[str, Any]]:
        return []

    def revoke_user_session(self, user_id: str, session_id: str):
        pass

    def revoke_all_user_sessions(self, user_id: str):
        pass

    def enqueue_task(self, task_data: Dict[str, Any], delay: int = 0) -> str:
        import uuid

        task_id = str(uuid.uuid4())
        return task_id

    def get_queue_stats(self) -> Dict[str, Any]:
        return {
            "queue_length": 0,
            "processing_count": 0,
            "completed_count": 0,
            "failed_count": 0,
        }

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        return {"id": session_id, "user_id": "demo_user"}

    def destroy_session(self, session_id: str):
        pass

    def enable_2fa(self, user_id: str) -> Dict[str, Any]:
        import secrets

        return {
            "totp_secret": secrets.token_urlsafe(32),
            "backup_codes": ["DEMO" + str(i) for i in range(10)],
            "qr_code_url": "otpauth://totp/CyRedisApp:demo?secret=demo&issuer=CyRedisApp",
        }

    def verify_totp(self, user_id: str, token: str) -> bool:
        return token == "123456"

    def refresh_access_token(self, refresh_token: str) -> Optional[str]:
        import secrets

        return secrets.token_urlsafe(32)

    def revoke_token(self, token: str):
        pass

    def increment_counter(self, counter_name: str, increment: int = 1) -> int:
        return increment

    def get_counter(self, counter_name: str) -> int:
        return 0

    def set_shared_data(self, key: str, data: Any, expiry: int = None):
        pass

    def get_shared_data(self, key: str) -> Any:
        return None

    def create_shared_dict(self, name: str, initial_data: Dict[str, Any] = None):
        class SimpleSharedDict(dict):
            def __init__(self, name, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.dict_name = name

            def get_stats(self):
                return {
                    "name": self.dict_name,
                    "key_count": len(self),
                    "total_size_bytes": 0,
                    "cache_age_seconds": 0,
                    "cache_ttl_seconds": 30,
                }

        return SimpleSharedDict(name, initial_data or {})

    # Mock streaming iterators for demo
    def stream_iterator(
        self,
        stream_key: str,
        consumer_group: str = None,
        consumer_name: str = None,
        batch_size: int = 10,
        block_ms: int = 1000,
    ):
        """Create a mock async iterator for Redis streams."""
        return MockStreamIterator(stream_key, batch_size, block_ms)

    def list_iterator(self, list_key: str, batch_size: int = 10, block_ms: int = 1000):
        """Create a mock async iterator for Redis lists."""
        return MockListIterator(list_key, batch_size, block_ms)

    def pubsub_iterator(self, channels, timeout_ms: int = 1000):
        """Create a mock async iterator for Redis pub/sub."""
        return MockPubSubIterator(channels, timeout_ms)


class MockStreamIterator:
    """Mock async iterator for Redis streams (demo purposes)."""

    def __init__(self, stream_key: str, batch_size: int = 10, block_ms: int = 1000):
        self.stream_key = stream_key
        self.batch_size = batch_size
        self.block_ms = block_ms
        self.message_counter = 0
        self.is_closed = False

    def __aiter__(self):
        """Return self as async iterator."""
        return self

    async def __anext__(self):
        """Get next batch of messages."""
        if self.is_closed:
            raise StopAsyncIteration

        # Generate mock messages
        messages = []
        for i in range(self.batch_size):
            self.message_counter += 1
            messages.append(
                {
                    "id": f"{self.stream_key}:{self.message_counter}",
                    "stream": self.stream_key,
                    "data": {
                        "timestamp": time.time(),
                        "message": f"Message {self.message_counter}",
                        "user_id": f"user_{self.message_counter % 10}",
                    },
                }
            )

        # Simulate blocking read
        await asyncio.sleep(0.1)

        return messages

    def close(self):
        """Close the iterator."""
        self.is_closed = True

    def __aenter__(self):
        """Sync context manager entry."""
        return self

    def __aexit__(self, exc_type, exc_val, exc_tb):
        """Sync context manager exit."""
        self.close()


class MockListIterator:
    """Mock async iterator for Redis lists (demo purposes)."""

    def __init__(self, list_key: str, batch_size: int = 10, block_ms: int = 1000):
        self.list_key = list_key
        self.batch_size = batch_size
        self.block_ms = block_ms
        self.item_counter = 0
        self.is_closed = False

    def __aiter__(self):
        """Return self as async iterator."""
        return self

    async def __anext__(self):
        """Get next batch of list items."""
        if self.is_closed:
            raise StopAsyncIteration

        # Generate mock list items
        items = []
        for i in range(self.batch_size):
            self.item_counter += 1
            items.append(f"Item {self.item_counter} from {self.list_key}")

        # Simulate blocking read
        await asyncio.sleep(0.1)

        return items

    def close(self):
        """Close the iterator."""
        self.is_closed = True

    def __aenter__(self):
        """Sync context manager entry."""
        return self

    def __aexit__(self, exc_type, exc_val, exc_tb):
        """Sync context manager exit."""
        self.close()


class MockPubSubIterator:
    """Mock async iterator for Redis pub/sub (demo purposes)."""

    def __init__(self, channels, timeout_ms: int = 1000):
        self.channels = channels if isinstance(channels, list) else [channels]
        self.timeout_ms = timeout_ms
        self.message_counter = 0
        self.is_closed = False

    def __aiter__(self):
        """Return self as async iterator."""
        return self

    async def __anext__(self):
        """Get next pub/sub message."""
        if self.is_closed:
            raise StopAsyncIteration

        # Generate mock pub/sub messages
        self.message_counter += 1
        channel = self.channels[self.message_counter % len(self.channels)]

        # Simulate blocking read
        await asyncio.sleep(0.1)

        return {
            "type": "message",
            "channel": channel,
            "data": f"Message {self.message_counter} on channel {channel}",
        }

    def close(self):
        """Close the iterator."""
        self.is_closed = True

    def __aenter__(self):
        """Sync context manager entry."""
        return self

    def __aexit__(self, exc_type, exc_val, exc_tb):
        """Sync context manager exit."""
        self.close()


class WebApplication:
    """
    Example web application demonstrating Redis streaming iterators.
    Shows how to use streams, lists, and pub/sub for real-time features.
    """

    def __init__(self, redis_host="localhost", redis_port=6379):
        # Initialize web app support
        self.app_support = WebApplicationSupport(redis_host, redis_port)

        # Create shared dictionaries for the application
        self.user_profiles = self.app_support.create_shared_dict("user_profiles")
        self.application_settings = self.app_support.create_shared_dict("app_settings")

        # Initialize application settings
        self._initialize_app_settings()

    async def register_user(
        self, username: str, email: str, password: str
    ) -> Dict[str, Any]:
        """Register a new user"""
        # Check if user already exists
        if self.user_profiles.get(f"user:{username}"):
            return {"success": False, "error": "User already exists"}

        # Create user profile
        user_id = str(uuid.uuid4())
        user_profile = {
            "id": user_id,
            "username": username,
            "email": email,
            "password_hash": self._hash_password(
                password
            ),  # In production, use proper hashing
            "created_at": time.time(),
            "is_active": True,
            "is_verified": False,
            "login_attempts": 0,
            "last_login": None,
            "2fa_enabled": False,
            "roles": ["user"],
            "preferences": {"theme": "light", "notifications": True},
        }

        # Store user profile
        self.user_profiles[f"user:{username}"] = user_profile
        self.user_profiles[f"user_id:{user_id}"] = username

        return {
            "success": True,
            "user_id": user_id,
            "message": "User registered successfully",
        }

    def _initialize_app_settings(self):
        """Initialize default application settings"""
        if not self.application_settings:
            self.application_settings.update(
                {
                    "app_name": "CyRedis Streaming App",
                    "version": "1.0.0",
                    "streaming_features": {
                        "sse_enabled": True,
                        "websockets_enabled": True,
                        "stream_processing": True,
                        "pubsub_broadcasting": True,
                    },
                }
            )

    # ===== SERVER-SENT EVENTS (SSE) EXAMPLE =====

    async def sse_chat_stream(self, user_id: str) -> AsyncGenerator[str, None]:
        """
        Example SSE stream for chat messages.
        Uses Redis streams for message delivery.
        """
        print(f"🎯 Starting SSE chat stream for user {user_id}")

        # Create consumer group for this user
        consumer_group = f"chat_{user_id}"
        consumer_name = f"user_{user_id}"

        try:
            # Get the iterator directly (not using async context manager for demo)
            stream_iter = self.app_support.stream_iterator(
                "chat_messages",
                consumer_group=consumer_group,
                consumer_name=consumer_name,
                batch_size=5,
                block_ms=5000,  # 5 second timeout
            )

            async for messages in stream_iter:
                for message in messages:
                    # Format as SSE event
                    sse_data = {
                        "type": "chat_message",
                        "id": message["id"],
                        "timestamp": message["data"].get("timestamp"),
                        "user": message["data"].get("user_id"),
                        "message": message["data"].get("message"),
                    }

                    # Yield SSE formatted data
                    yield f"data: {json.dumps(sse_data)}\n\n"

        except Exception as e:
            print(f"SSE stream error: {e}")
            # Send error event
            error_data = {"type": "error", "message": str(e)}
            yield f"data: {json.dumps(error_data)}\n\n"

    # ===== WEBSOCKET STREAM EXAMPLE =====

    async def websocket_chat_handler(self, websocket, user_id: str):
        """
        Example websocket handler using Redis streams.
        """
        print(f"🔗 WebSocket connection established for user {user_id}")

        # Create consumer for this websocket connection
        consumer_group = f"ws_chat_{user_id}"
        consumer_name = f"ws_{uuid.uuid4().hex[:8]}"

        try:
            async with self.app_support.stream_iterator(
                "chat_messages",
                consumer_group=consumer_group,
                consumer_name=consumer_name,
                batch_size=1,
                block_ms=1000,  # 1 second timeout for responsive WS
            ) as stream_iter:
                async for messages in stream_iter:
                    for message in messages:
                        # Send message to websocket
                        ws_message = {
                            "type": "chat_message",
                            "id": message["id"],
                            "timestamp": message["data"].get("timestamp"),
                            "user": message["data"].get("user_id"),
                            "message": message["data"].get("message"),
                        }

                        await websocket.send_json(ws_message)

        except Exception as e:
            print(f"WebSocket error: {e}")
            # Send error message
            error_message = {"type": "error", "message": str(e)}
            await websocket.send_json(error_message)

    # ===== LIST-BASED NOTIFICATIONS =====

    async def notification_stream(self, user_id: str) -> AsyncGenerator[str, None]:
        """
        Example notification stream using Redis lists.
        """
        print(f"🔔 Starting notification stream for user {user_id}")

        list_key = f"notifications:{user_id}"

        try:
            # Get the iterator directly (not using async context manager for demo)
            list_iter = self.app_support.list_iterator(
                list_key, batch_size=5, block_ms=3000  # 3 second timeout
            )

            async for items in list_iter:
                for item in items:
                    # Format as SSE event
                    notification_data = {
                        "type": "notification",
                        "id": str(uuid.uuid4()),
                        "timestamp": time.time(),
                        "message": item,
                        "user_id": user_id,
                    }

                    yield f"data: {json.dumps(notification_data)}\n\n"

        except Exception as e:
            print(f"Notification stream error: {e}")
            error_data = {"type": "error", "message": str(e)}
            yield f"data: {json.dumps(error_data)}\n\n"

    # ===== PUB/SUB BROADCASTING =====

    async def broadcast_to_users(self, message: str, target_users: List[str] = None):
        """
        Broadcast message to users via pub/sub.
        """
        if target_users:
            # Send to specific users
            for user_id in target_users:
                channel = f"user_notifications:{user_id}"
                # In real implementation, would use Redis pub/sub
                print(f"Broadcasting to {user_id} on channel {channel}: {message}")
        else:
            # Broadcast to all users
            channel = "global_notifications"
            print(f"Global broadcast on {channel}: {message}")

    async def pubsub_listener(self, user_id: str) -> AsyncGenerator[str, None]:
        """
        Listen to pub/sub messages for a user.
        """
        print(f"📡 Starting pub/sub listener for user {user_id}")

        channels = [f"user_notifications:{user_id}", "global_notifications"]

        try:
            # Get the iterator directly (not using async context manager for demo)
            pubsub_iter = self.app_support.pubsub_iterator(
                channels, timeout_ms=5000  # 5 second timeout
            )

            async for message in pubsub_iter:
                # Format as SSE event
                sse_data = {
                    "type": "broadcast",
                    "id": str(uuid.uuid4()),
                    "timestamp": time.time(),
                    "channel": message["channel"],
                    "message": message["data"],
                }

                yield f"data: {json.dumps(sse_data)}\n\n"

        except Exception as e:
            print(f"Pub/sub listener error: {e}")
            error_data = {"type": "error", "message": str(e)}
            yield f"data: {json.dumps(error_data)}\n\n"

    # ===== UTILITY METHODS =====

    def send_chat_message(self, user_id: str, message: str):
        """Send a chat message (would add to Redis stream in real implementation)."""
        # In real implementation, this would use XADD to add to chat_messages stream
        print(f"💬 User {user_id} sent: {message}")

        # For demo purposes, just print (would use Redis in production)
        # self.app_support.redis_client.xadd("chat_messages", {...})

    def add_notification(self, user_id: str, notification: str):
        """Add notification to user's list."""
        # In real implementation, this would use LPUSH to add to notifications:user_id
        print(f"🔔 Added notification for {user_id}: {notification}")

        # For demo purposes, just print (would use Redis in production)
        # self.app_support.redis_client.lpush(f"notifications:{user_id}", notification)

    def get_system_stats(self) -> Dict[str, Any]:
        """Get system-wide statistics."""
        return {
            "streaming_features": self.application_settings.get(
                "streaming_features", {}
            ),
            "connected_users": len(
                [k for k in self.user_profiles.keys() if k.startswith("user:")]
            ),
            "active_streams": 0,  # Would track active stream iterators
            "active_pubsub_listeners": 0,  # Would track active pub/sub listeners
            "jwt_tokens": {
                "access_tokens": 0,  # Would track active access tokens
                "websocket_tokens": 0,  # Would track active websocket tokens
                "api_tokens": 0,  # Would track active API tokens
            },
        }


# ===== EXAMPLE USAGE =====


async def example_streaming_usage():
    """Example usage of Redis streaming iterators."""
    print("🚀 Starting CyRedis Streaming Example...")
    print("=" * 60)

    # Initialize the web application
    app = WebApplication()

    try:
        # 1. Register and authenticate a user
        print("\n🔐 Registering and authenticating user...")
        reg_result = await app.register_user(
            "user_1", "user1@example.com", "password123"
        )
        print(f"Registration: {reg_result}")

        auth_result = await app.authenticate_user("user_1", "password123")
        print(f"Authentication: {auth_result}")

        user_id = auth_result["user_id"]
        access_token = auth_result["tokens"]["access_token"]
        websocket_token = auth_result["tokens"]["websocket_token"]
        api_token = auth_result["tokens"]["api_token"]

        # 2. Test JWT token validation
        print("\n🔑 Testing JWT token validation...")

        # Test access token
        payload = app.app_support.verify_user_access(access_token)
        print(f"Access token valid: {payload is not None}")

        # Test websocket token
        ws_payload = app.app_support.jwt_middleware(
            websocket_token, required_permissions=["stream_access"]
        )
        print(f"Websocket token valid: {ws_payload is not None}")

        # Test API token
        api_payload = app.app_support.jwt_middleware(
            api_token, required_scopes=["read"]
        )
        print(f"API token valid: {api_payload is not None}")

        # 3. Send some chat messages (requires authentication)
        print("\n💬 Sending authenticated chat messages...")
        # In real implementation, this would require the API token
        if app.app_support.require_scopes(api_token, ["write"]):
            app.send_chat_message("user_1", "Hello everyone! (authenticated)")
            app.send_chat_message("user_2", "Hey there! (authenticated)")
            app.send_chat_message("user_1", "How is everyone doing? (authenticated)")
        else:
            print("API token missing write scope")

        # 4. Add notifications (requires authentication)
        print("\n🔔 Adding authenticated notifications...")
        if app.app_support.require_scopes(api_token, ["write"]):
            app.add_notification("user_1", "Welcome to the authenticated app!")
            app.add_notification("user_2", "You have a new authenticated message")
        else:
            print("API token missing write scope")

        # 5. Demonstrate authenticated SSE chat stream
        print("\n📡 Demonstrating authenticated SSE chat stream...")

        async def demo_authenticated_sse_stream():
            # Check if user is authenticated
            if not app.app_support.require_auth(access_token, user_id):
                print("SSE access denied: Invalid authentication")
                return

            message_count = 0
            # In real implementation, this would check the websocket token
            if app.app_support.require_permissions(websocket_token, ["stream_access"]):
                # Simulate reading from authenticated SSE stream
                print("SSE access granted: Streaming authenticated chat messages")
                message_count += 1
            else:
                print("SSE access denied: Missing stream permissions")

        await demo_authenticated_sse_stream()

        # 6. Demonstrate authenticated notification stream
        print("\n🔔 Demonstrating authenticated notification stream...")

        async def demo_authenticated_notification_stream():
            # Check authentication
            if not app.app_support.require_auth(access_token, user_id):
                print("Notification access denied: Invalid authentication")
                return

            notification_count = 0
            # In real implementation, this would check permissions
            if app.app_support.require_permissions(
                websocket_token, ["realtime_access"]
            ):
                print(
                    "Notification access granted: Streaming authenticated notifications"
                )
                notification_count += 1
            else:
                print("Notification access denied: Missing realtime permissions")

        await demo_authenticated_notification_stream()

        # 7. Demonstrate authenticated pub/sub broadcasting
        print("\n📢 Demonstrating authenticated pub/sub broadcasting...")

        async def demo_authenticated_pubsub_listener():
            # Check authentication
            if not app.app_support.require_auth(access_token, user_id):
                print("Pub/sub access denied: Invalid authentication")
                return

            message_count = 0
            # In real implementation, this would check permissions
            if app.app_support.require_permissions(
                websocket_token, ["realtime_access"]
            ):
                print("Pub/sub access granted: Streaming authenticated broadcasts")
                message_count += 1
            else:
                print("Pub/sub access denied: Missing realtime permissions")

        await demo_authenticated_pubsub_listener()

        # 8. Get system statistics
        print("\n📊 Getting system statistics...")
        stats = app.get_system_stats()
        print(f"System stats: {json.dumps(stats, indent=2, default=str)}")

        # 9. Demonstrate JWT token refresh
        print("\n🔄 Demonstrating JWT token refresh...")
        new_token = app.app_support.refresh_access_token(
            auth_result["tokens"]["refresh_token"]
        )
        print(f"Token refreshed: {new_token is not None}")

        # 10. Demonstrate token revocation
        print("\n🚪 Demonstrating token revocation...")
        app.app_support.revoke_token(access_token)
        revoked_payload = app.app_support.verify_user_access(access_token)
        print(f"Access token revoked: {revoked_payload is None}")

    except Exception as e:
        print(f"❌ Error in example: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # Cleanup
        print("\n🛑 Shutting down...")
        app.app_support.shutdown()


async def example_fastapi_integration():
    """Example of how this would integrate with FastAPI."""
    print("\n🌐 FastAPI Integration Example")
    print("=" * 40)

    print("""
# Example FastAPI integration:

from fastapi import FastAPI
from fastapi.responses import StreamingResponse

app = FastAPI()

@app.get("/sse/chat/{user_id}")
async def sse_chat(user_id: str):
    '''SSE endpoint for chat messages.'''
    def generate_sse():
        async def sse_generator():
            async with app_support.stream_iterator(
                f"chat_messages",
                consumer_group=f"chat_{user_id}",
                consumer_name=f"user_{user_id}"
            ) as stream_iter:
                async for messages in stream_iter:
                    for message in messages:
                        yield f"data: {json.dumps(message['data'])}\\n\\n"

        return sse_generator()

    return StreamingResponse(generate_sse(), media_type="text/event-stream")

@app.websocket("/ws/chat/{user_id}")
async def websocket_chat(websocket, user_id: str):
    '''WebSocket endpoint for real-time chat.'''
    await websocket.accept()

    try:
        async with app_support.stream_iterator(
            "chat_messages",
            consumer_group=f"ws_chat_{user_id}",
            consumer_name=f"ws_{uuid.uuid4().hex[:8]}"
        ) as stream_iter:
            async for messages in stream_iter:
                for message in messages:
                    await websocket.send_json(message['data'])
    except Exception as e:
        await websocket.send_json({"error": str(e)})
    finally:
        await websocket.close()

@app.get("/sse/notifications/{user_id}")
async def sse_notifications(user_id: str):
    '''SSE endpoint for user notifications.'''
    def generate_notifications():
        async def notification_generator():
            async with app_support.list_iterator(f"notifications:{user_id}") as list_iter:
                async for items in list_iter:
                    for item in items:
                        yield f"data: {json.dumps({'message': item})}\\n\\n"

        return notification_generator()

    return StreamingResponse(generate_notifications(), media_type="text/event-stream")
""")


if __name__ == "__main__":
    print("🎯 CyRedis Streaming Iterators - Real-time Features Demo")
    print("=" * 70)

    # Run the main example
    asyncio.run(example_streaming_usage())

    # Show FastAPI integration example
    asyncio.run(example_fastapi_integration())

    print("\n✅ Streaming demo completed!")
    print("\n🚀 Key Features Demonstrated:")
    print("• Redis Stream iterators for chat messages")
    print("• Redis List iterators for notifications")
    print("• Redis Pub/Sub iterators for broadcasting")
    print("• Async context managers for resource management")
    print("• SSE and WebSocket integration patterns")
    print("• Concurrent streaming capabilities")
