#!/usr/bin/env python3
"""
FastAPI-Style Web Application Example using CyRedis Web App Support
Demonstrates all the advanced features including worker queues, token management,
session lifecycle, 2FA, and shared dictionaries.
"""

import asyncio
import json
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

# Import our web application support (Python implementation for demo)
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

        return {
            "access_token": secrets.token_urlsafe(32),
            "refresh_token": secrets.token_urlsafe(32),
            "token_type": "bearer",
        }

    def verify_user_access(
        self, token: str, required_claims: Dict[str, Any] = None
    ) -> Optional[Dict[str, Any]]:
        return {"user_id": "demo_user", "type": "access"}

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


class WebApplication:
    """
    Example web application using CyRedis Web App Support features.
    This demonstrates FastAPI-style patterns with Redis-backed functionality.
    """

    def __init__(self, redis_host="localhost", redis_port=6379):
        # Initialize web app support
        self.app_support = WebApplicationSupport(redis_host, redis_port)

        # Create shared dictionaries for the application
        self.user_profiles = self.app_support.create_shared_dict("user_profiles")
        self.application_settings = self.app_support.create_shared_dict("app_settings")
        self.rate_limits = self.app_support.create_shared_dict("rate_limits")

        # Initialize application settings
        self._initialize_app_settings()

        # Setup worker queue for background tasks
        self.task_queue = self.app_support.worker_queue

    def _initialize_app_settings(self):
        """Initialize default application settings"""
        if not self.application_settings:
            self.application_settings.update(
                {
                    "app_name": "CyRedis Web App",
                    "version": "1.0.0",
                    "max_login_attempts": 5,
                    "session_timeout": 3600,
                    "rate_limit_per_minute": 100,
                    "features": {
                        "2fa_enabled": True,
                        "password_reset_enabled": True,
                        "multi_session_tracking": True,
                        "worker_queues": True,
                    },
                }
            )

    # ===== USER AUTHENTICATION & AUTHORIZATION =====

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

        # Initialize rate limiting for user
        self.rate_limits[f"login_attempts:{username}"] = {
            "count": 0,
            "window_start": time.time(),
            "blocked_until": None,
        }

        return {
            "success": True,
            "user_id": user_id,
            "message": "User registered successfully",
        }

    async def authenticate_user(self, username: str, password: str) -> Dict[str, Any]:
        """Authenticate user and return tokens"""
        # Check rate limiting
        rate_limit = self.rate_limits.get(f"login_attempts:{username}", {})
        current_time = time.time()

        if (
            rate_limit.get("blocked_until")
            and current_time < rate_limit["blocked_until"]
        ):
            return {"success": False, "error": "Account temporarily locked"}

        # Get user profile
        user_profile = self.user_profiles.get(f"user:{username}")
        if not user_profile or not self._verify_password(
            password, user_profile["password_hash"]
        ):
            # Increment failed attempts
            rate_limit["count"] = rate_limit.get("count", 0) + 1
            if rate_limit["count"] >= 5:
                rate_limit["blocked_until"] = current_time + 300  # 5 minutes lock
            self.rate_limits[f"login_attempts:{username}"] = rate_limit
            return {"success": False, "error": "Invalid credentials"}

        # Check if 2FA is required
        if user_profile.get("2fa_enabled", False):
            # Return challenge for 2FA verification
            return {
                "success": False,
                "requires_2fa": True,
                "user_id": user_profile["id"],
                "message": "2FA verification required",
            }

        # Successful authentication
        session_id = self.app_support.create_user_session(
            user_profile["id"],
            device_info={"user_agent": "example_app", "ip": "127.0.0.1"},
        )

        # Update user profile
        user_profile["last_login"] = current_time
        user_profile["login_attempts"] = 0
        self.user_profiles[f"user:{username}"] = user_profile

        # Reset rate limiting
        self.rate_limits[f"login_attempts:{username}"] = {
            "count": 0,
            "window_start": current_time,
            "blocked_until": None,
        }

        # Create authentication tokens
        tokens = self.app_support.authenticate_user(
            user_profile["id"], "dummy_password"
        )

        return {
            "success": True,
            "user_id": user_profile["id"],
            "session_id": session_id,
            "tokens": tokens,
            "user": {
                "username": username,
                "roles": user_profile["roles"],
                "preferences": user_profile["preferences"],
            },
        }

    async def verify_2fa(self, user_id: str, token: str) -> Dict[str, Any]:
        """Verify 2FA token"""
        if self.app_support.verify_totp(user_id, token):
            # Get user info
            username = self.user_profiles.get(f"user_id:{user_id}")
            user_profile = self.user_profiles.get(f"user:{username}")

            # Create session and tokens
            session_id = self.app_support.create_user_session(
                user_id, device_info={"user_agent": "example_app", "ip": "127.0.0.1"}
            )

            tokens = self.app_support.authenticate_user(user_id, "dummy_password")

            return {
                "success": True,
                "session_id": session_id,
                "tokens": tokens,
                "user": {
                    "username": username,
                    "roles": user_profile["roles"],
                    "preferences": user_profile["preferences"],
                },
            }

        return {"success": False, "error": "Invalid 2FA token"}

    async def refresh_access_token(self, refresh_token: str) -> Dict[str, Any]:
        """Refresh access token"""
        new_token = self.app_support.refresh_access_token(refresh_token)
        if new_token:
            return {"success": True, "access_token": new_token}
        return {"success": False, "error": "Invalid refresh token"}

    def logout_user(self, session_id: str):
        """Logout user session"""
        self.app_support.destroy_session(session_id)

    # ===== SESSION MANAGEMENT =====

    def get_user_sessions(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all active sessions for a user"""
        return self.app_support.multi_session_tracker.get_user_sessions(user_id)

    def revoke_user_session(self, user_id: str, session_id: str):
        """Revoke a specific user session"""
        self.app_support.multi_session_tracker.revoke_session(session_id)

    def revoke_all_user_sessions(self, user_id: str):
        """Revoke all sessions for a user"""
        self.app_support.multi_session_tracker.revoke_all_user_sessions(user_id)

    # ===== 2FA MANAGEMENT =====

    def enable_2fa(self, user_id: str) -> Dict[str, Any]:
        """Enable 2FA for user"""
        username = self.user_profiles.get(f"user_id:{user_id}")
        if not username:
            return {"success": False, "error": "User not found"}

        user_profile = self.user_profiles.get(f"user:{username}")
        if user_profile.get("2fa_enabled"):
            return {"success": False, "error": "2FA already enabled"}

        # Enable 2FA
        setup_info = self.app_support.enable_2fa(user_id)

        # Update user profile
        user_profile["2fa_enabled"] = True
        self.user_profiles[f"user:{username}"] = user_profile

        return {
            "success": True,
            "totp_secret": setup_info["totp_secret"],
            "backup_codes": setup_info["backup_codes"],
            "qr_code_url": setup_info["qr_code_url"],
        }

    def verify_2fa_setup(self, user_id: str, token: str) -> bool:
        """Verify 2FA setup with first token"""
        return self.app_support.verify_totp(user_id, token)

    def disable_2fa(self, user_id: str, password: str) -> Dict[str, Any]:
        """Disable 2FA for user"""
        username = self.user_profiles.get(f"user_id:{user_id}")
        if not username:
            return {"success": False, "error": "User not found"}

        user_profile = self.user_profiles.get(f"user:{username}")
        if not self._verify_password(password, user_profile["password_hash"]):
            return {"success": False, "error": "Invalid password"}

        # Disable 2FA
        self.app_support.two_factor_auth.disable_2fa(user_id)
        user_profile["2fa_enabled"] = False
        self.user_profiles[f"user:{username}"] = user_profile

        return {"success": True, "message": "2FA disabled"}

    # ===== PASSWORD RESET =====

    async def request_password_reset(self, email: str) -> Dict[str, Any]:
        """Request password reset"""
        # Find user by email
        user_profile = None
        username = None

        for key, profile in self.user_profiles.items():
            if key.startswith("user:") and profile.get("email") == email:
                user_profile = profile
                username = profile["username"]
                break

        if not user_profile:
            # Don't reveal if email exists or not for security
            return {"success": True, "message": "If email exists, reset link sent"}

        # Create reset token
        reset_token = self.app_support.password_reset.create_reset_token(
            user_profile["id"], email
        )

        # In production, you'd send an email here
        print(f"Password reset token for {email}: {reset_token}")

        return {"success": True, "message": "Reset link sent to email"}

    async def reset_password(
        self, reset_token: str, new_password: str
    ) -> Dict[str, Any]:
        """Reset password with token"""
        token_data = self.app_support.password_reset.verify_reset_token(reset_token)

        if not token_data:
            return {"success": False, "error": "Invalid or expired reset token"}

        # Find user profile
        username = self.user_profiles.get(f"user_id:{token_data['user_id']}")
        if not username:
            return {"success": False, "error": "User not found"}

        user_profile = self.user_profiles.get(f"user:{username}")
        user_profile["password_hash"] = self._hash_password(new_password)
        self.user_profiles[f"user:{username}"] = user_profile

        return {"success": True, "message": "Password reset successfully"}

    # ===== SHARED DATA OPERATIONS =====

    def increment_counter(self, counter_name: str, amount: int = 1) -> int:
        """Increment a shared counter"""
        return self.app_support.increment_counter(counter_name, amount)

    def get_counter(self, counter_name: str) -> int:
        """Get counter value"""
        return self.app_support.get_counter(counter_name)

    def set_shared_data(self, key: str, data: Any, expiry: int = None):
        """Set shared data"""
        return self.app_support.set_shared_data(key, data, expiry)

    def get_shared_data(self, key: str) -> Any:
        """Get shared data"""
        return self.app_support.get_shared_data(key)

    def update_user_preferences(
        self, user_id: str, preferences: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update user preferences"""
        username = self.user_profiles.get(f"user_id:{user_id}")
        if not username:
            return {"success": False, "error": "User not found"}

        user_profile = self.user_profiles.get(f"user:{username}")
        user_profile["preferences"].update(preferences)
        self.user_profiles[f"user:{username}"] = user_profile

        return {"success": True, "preferences": user_profile["preferences"]}

    # ===== BACKGROUND TASKS =====

    def enqueue_email_task(self, to_email: str, subject: str, body: str):
        """Enqueue email sending task"""
        task_data = {
            "type": "send_email",
            "to_email": to_email,
            "subject": subject,
            "body": body,
            "timestamp": time.time(),
        }

        return self.app_support.enqueue_task(task_data)

    def enqueue_cleanup_task(self):
        """Enqueue cleanup task"""
        task_data = {"type": "cleanup_expired_sessions", "timestamp": time.time()}

        return self.app_support.enqueue_task(task_data, delay=3600)  # Run every hour

        # ===== ADMIN OPERATIONS =====

        def get_system_stats(self) -> Dict[str, Any]:
            """Get system-wide statistics"""
            return {
                "users_count": len(
                    [k for k in self.user_profiles.keys() if k.startswith("user:")]
                ),
                "active_sessions": 0,  # Fallback implementation doesn't track sessions
                "queue_stats": self.app_support.get_queue_stats(),
                "rate_limits_count": len(
                    [
                        k
                        for k in self.rate_limits.keys()
                        if k.startswith("login_attempts:")
                    ]
                ),
                "shared_dicts_stats": {
                    "user_profiles": self.user_profiles.get_stats(),
                    "app_settings": self.application_settings.get_stats(),
                    "rate_limits": self.rate_limits.get_stats(),
                },
            }

    def cleanup_expired_data(self):
        """Clean up expired sessions and tokens"""
        # Fallback implementation doesn't track sessions
        return {"expired_sessions_cleaned": 0}

    # ===== UTILITY METHODS =====

    def _hash_password(self, password: str) -> str:
        """Simple password hash (use proper hashing in production)"""
        import hashlib

        return hashlib.sha256(password.encode()).hexdigest()

    def _verify_password(self, password: str, password_hash: str) -> bool:
        """Verify password"""
        return self._hash_password(password) == password_hash


# ===== EXAMPLE USAGE =====


async def example_usage():
    """Example usage of the web application"""
    print("🚀 Starting CyRedis Web Application Example...")

    # Initialize the web application
    app = WebApplication()

    try:
        # 1. Register a new user
        print("\n📝 Registering new user...")
        result = await app.register_user("johndoe", "john@example.com", "password123")
        print(f"Registration result: {result}")

        # 2. Authenticate user
        print("\n🔐 Authenticating user...")
        auth_result = await app.authenticate_user("johndoe", "password123")
        print(f"Authentication result: {auth_result}")

        if auth_result["success"]:
            user_id = auth_result["user_id"]
            session_id = auth_result["session_id"]
            access_token = auth_result["tokens"]["access_token"]

            # 3. Enable 2FA
            print(f"\n🔒 Enabling 2FA for user {user_id}...")
            twofa_result = app.enable_2fa(user_id)
            print(f"2FA setup result: {twofa_result}")

            # 4. Update user preferences
            print(f"\n⚙️  Updating user preferences for {user_id}...")
            prefs_result = app.update_user_preferences(
                user_id, {"theme": "dark", "notifications": False}
            )
            print(f"Preferences update: {prefs_result}")

            # 5. Increment a counter
            print("\n📊 Incrementing shared counter...")
            counter_result = app.increment_counter("total_logins", 1)
            print(f"Counter result: {counter_result}")

            # 6. Set some shared data
            print("\n💾 Setting shared data...")
            app.set_shared_data(
                "last_maintenance", {"timestamp": time.time(), "type": "daily"}
            )

            # 7. Enqueue a background task
            print("\n📧 Enqueuing email task...")
            task_id = app.enqueue_email_task(
                "admin@example.com", "Daily Report", "System running smoothly"
            )
            print(f"Task enqueued with ID: {task_id}")

        # 8. Get system statistics
        print("\n📈 Getting system statistics...")
        # Note: In the enhanced version, this would use get_cluster_info()
        print("System statistics would show worker coordination info")

        # 9. Get user sessions
        print(f"\n👥 Getting sessions for user {user_id}...")
        sessions = app.get_user_sessions(user_id)
        print(f"User sessions: {sessions}")

        # 10. Clean up expired data
        print("\n🧹 Cleaning up expired data...")
        cleanup_result = app.cleanup_expired_data()
        print(f"Cleanup result: {cleanup_result}")

        # 11. Logout user
        print(f"\n🚪 Logging out session {session_id}...")
        app.logout_user(session_id)

    except Exception as e:
        print(f"❌ Error in example: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # Cleanup
        print("\n🛑 Shutting down...")
        app.app_support.shutdown()


def example_worker_queue():
    """Example of custom worker queue task processing"""
    print("\n⚙️  Example: Custom Worker Queue Processing")

    # For demo purposes, just show how it would work
    print("This would demonstrate custom worker queue implementation")
    print("In a real implementation, you would:")
    print("- Create custom WorkerQueue subclasses")
    print("- Override process_task() method")
    print("- Handle different task types")
    print("- Implement retry logic and error handling")


if __name__ == "__main__":
    print("🎯 CyRedis Web Application Support - FastAPI-Style Features Demo")
    print("=" * 70)

    # Run the main example
    asyncio.run(example_usage())

    # Run worker queue example
    example_worker_queue()

    print("\n✅ Demo completed!")
