#!/usr/bin/env python3
"""
Session Management System

A distributed session manager demonstrating CyRedis for web sessions:
- Session creation and validation
- Automatic expiration
- Session data storage
- Concurrent session limits
- Session analytics

Features:
- Session CRUD operations
- Automatic TTL management
- User session limits
- Session statistics
- Active session monitoring
- Session hijacking prevention

Usage:
    # Interactive mode
    python session_manager_app.py

    # Create session
    python session_manager_app.py create --user alice

    # List user sessions
    python session_manager_app.py list --user alice

    # Monitor active sessions
    python session_manager_app.py monitor
"""

import sys
import argparse
import json
import uuid
import time
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import hashlib

try:
    from optimized_redis import OptimizedRedis
except ImportError:
    print("Error: CyRedis not properly installed. Run build_optimized.sh first.")
    sys.exit(1)


class SessionManager:
    """Distributed session manager using Redis"""

    def __init__(self, redis: OptimizedRedis, namespace: str = "sessions",
                 default_ttl: int = 1800, max_sessions_per_user: int = 5):
        """Initialize session manager

        Args:
            redis: Redis client instance
            namespace: Namespace for session keys
            default_ttl: Default session TTL in seconds (30 minutes)
            max_sessions_per_user: Maximum concurrent sessions per user
        """
        self.redis = redis
        self.namespace = namespace
        self.default_ttl = default_ttl
        self.max_sessions_per_user = max_sessions_per_user
        print(f"✓ Session manager initialized (TTL: {default_ttl}s)")

    def _session_key(self, session_id: str) -> str:
        """Generate session key"""
        return f"{self.namespace}:session:{session_id}"

    def _user_sessions_key(self, user_id: str) -> str:
        """Generate user sessions set key"""
        return f"{self.namespace}:user:{user_id}:sessions"

    def create_session(self, user_id: str, user_data: Optional[Dict] = None,
                       ip_address: Optional[str] = None, user_agent: Optional[str] = None,
                       ttl: Optional[int] = None) -> Optional[str]:
        """Create a new session

        Args:
            user_id: User identifier
            user_data: Additional user data to store
            ip_address: Client IP address
            user_agent: Client user agent
            ttl: Session TTL override

        Returns:
            Session ID if created, None if limit exceeded
        """
        # Check session limit
        user_sessions = self._get_user_sessions(user_id)
        if len(user_sessions) >= self.max_sessions_per_user:
            print(f"✗ Session limit reached for user {user_id} ({self.max_sessions_per_user} max)")
            # Remove oldest session
            oldest_session = min(user_sessions, key=lambda s: s["created_at"])
            self.destroy_session(oldest_session["session_id"])

        # Generate session ID
        session_id = str(uuid.uuid4())
        session_ttl = ttl or self.default_ttl

        # Create session data
        session_data = {
            "session_id": session_id,
            "user_id": user_id,
            "created_at": int(time.time()),
            "last_activity": int(time.time()),
            "ip_address": ip_address,
            "user_agent": user_agent,
            "user_data": user_data or {},
            "fingerprint": self._generate_fingerprint(user_id, ip_address, user_agent)
        }

        # Store session
        session_key = self._session_key(session_id)
        self.redis.setex(session_key, session_ttl, json.dumps(session_data))

        # Add to user's session set
        user_sessions_key = self._user_sessions_key(user_id)
        self.redis.sadd(user_sessions_key, session_id)
        self.redis.expire(user_sessions_key, session_ttl)

        print(f"✓ Created session {session_id[:8]}... for user {user_id}")
        return session_id

    def get_session(self, session_id: str, validate_fingerprint: Optional[Dict] = None) -> Optional[Dict]:
        """Get session data

        Args:
            session_id: Session identifier
            validate_fingerprint: Optional dict with user_id, ip_address, user_agent for validation

        Returns:
            Session data if valid, None otherwise
        """
        session_key = self._session_key(session_id)
        data = self.redis.get(session_key)

        if not data:
            return None

        session = json.loads(data)

        # Validate fingerprint if requested
        if validate_fingerprint:
            expected_fp = self._generate_fingerprint(
                validate_fingerprint.get("user_id"),
                validate_fingerprint.get("ip_address"),
                validate_fingerprint.get("user_agent")
            )
            if session["fingerprint"] != expected_fp:
                print(f"⚠️  Session fingerprint mismatch - possible hijacking")
                return None

        return session

    def update_session(self, session_id: str, user_data: Dict, extend_ttl: bool = True) -> bool:
        """Update session data

        Args:
            session_id: Session identifier
            user_data: Data to update
            extend_ttl: Whether to extend TTL

        Returns:
            True if updated successfully
        """
        session = self.get_session(session_id)
        if not session:
            return False

        # Update session data
        session["user_data"].update(user_data)
        session["last_activity"] = int(time.time())

        # Store updated session
        session_key = self._session_key(session_id)
        if extend_ttl:
            self.redis.setex(session_key, self.default_ttl, json.dumps(session))
        else:
            ttl = self.redis.ttl(session_key)
            if ttl > 0:
                self.redis.setex(session_key, ttl, json.dumps(session))

        return True

    def touch_session(self, session_id: str) -> bool:
        """Update session activity time and extend TTL

        Args:
            session_id: Session identifier

        Returns:
            True if session exists
        """
        return self.update_session(session_id, {}, extend_ttl=True)

    def destroy_session(self, session_id: str) -> bool:
        """Destroy a session

        Args:
            session_id: Session identifier

        Returns:
            True if session was destroyed
        """
        # Get session to find user
        session = self.get_session(session_id)
        if not session:
            return False

        # Remove from user's session set
        user_sessions_key = self._user_sessions_key(session["user_id"])
        self.redis.srem(user_sessions_key, session_id)

        # Delete session
        session_key = self._session_key(session_id)
        result = self.redis.delete(session_key)

        if result:
            print(f"✓ Destroyed session {session_id[:8]}...")
            return True

        return False

    def destroy_user_sessions(self, user_id: str) -> int:
        """Destroy all sessions for a user

        Args:
            user_id: User identifier

        Returns:
            Number of sessions destroyed
        """
        sessions = self._get_user_sessions(user_id)
        count = 0

        for session in sessions:
            if self.destroy_session(session["session_id"]):
                count += 1

        print(f"✓ Destroyed {count} session(s) for user {user_id}")
        return count

    def _get_user_sessions(self, user_id: str) -> List[Dict]:
        """Get all active sessions for a user

        Args:
            user_id: User identifier

        Returns:
            List of session data
        """
        user_sessions_key = self._user_sessions_key(user_id)
        session_ids = self.redis.smembers(user_sessions_key)

        sessions = []
        for session_id in session_ids:
            session_id = session_id.decode() if isinstance(session_id, bytes) else session_id
            session = self.get_session(session_id)
            if session:
                sessions.append(session)

        return sessions

    def list_user_sessions(self, user_id: str) -> List[Dict]:
        """List all active sessions for a user

        Args:
            user_id: User identifier

        Returns:
            List of session summaries
        """
        sessions = self._get_user_sessions(user_id)

        summaries = []
        for session in sessions:
            summaries.append({
                "session_id": session["session_id"],
                "created_at": datetime.fromtimestamp(session["created_at"]),
                "last_activity": datetime.fromtimestamp(session["last_activity"]),
                "ip_address": session.get("ip_address", "unknown"),
                "user_agent": session.get("user_agent", "unknown")[:50]
            })

        return summaries

    def _generate_fingerprint(self, user_id: str, ip_address: Optional[str],
                              user_agent: Optional[str]) -> str:
        """Generate session fingerprint for hijacking prevention

        Args:
            user_id: User identifier
            ip_address: Client IP
            user_agent: Client user agent

        Returns:
            Fingerprint hash
        """
        data = f"{user_id}:{ip_address or ''}:{user_agent or ''}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def get_stats(self) -> Dict[str, Any]:
        """Get session statistics

        Returns:
            Statistics dictionary
        """
        # Count all session keys
        pattern = f"{self.namespace}:session:*"
        session_keys = self.redis.keys(pattern)
        total_sessions = len(session_keys)

        # Count unique users
        user_pattern = f"{self.namespace}:user:*:sessions"
        user_keys = self.redis.keys(user_pattern)
        unique_users = len(user_keys)

        return {
            "total_sessions": total_sessions,
            "unique_users": unique_users,
            "avg_sessions_per_user": total_sessions / unique_users if unique_users > 0 else 0
        }


class SessionManagerApp:
    """Session manager application"""

    def __init__(self, host: str = "localhost", port: int = 6379):
        """Initialize session manager app

        Args:
            host: Redis host
            port: Redis port
        """
        self.redis = OptimizedRedis(host=host, port=port)
        self.manager = SessionManager(self.redis, default_ttl=1800, max_sessions_per_user=5)
        print(f"✓ Connected to Redis at {host}:{port}")

    def demo(self):
        """Run demonstration"""
        print("\n" + "="*60)
        print("Session Manager Demo")
        print("="*60)

        # Create sessions for different users
        print("\n1. Creating sessions...")
        alice_s1 = self.manager.create_session("alice", {"role": "admin"}, ip_address="192.168.1.10")
        alice_s2 = self.manager.create_session("alice", {"role": "admin"}, ip_address="192.168.1.11")
        bob_s1 = self.manager.create_session("bob", {"role": "user"}, ip_address="192.168.1.20")

        # List sessions
        print("\n2. Listing user sessions...")
        print("\nAlice's sessions:")
        for s in self.manager.list_user_sessions("alice"):
            print(f"  - {s['session_id'][:8]}... from {s['ip_address']} (created: {s['created_at']})")

        # Update session
        print("\n3. Updating session data...")
        self.manager.update_session(alice_s1, {"last_page": "/dashboard", "cart_items": 3})
        session = self.manager.get_session(alice_s1)
        print(f"✓ Session data: {session['user_data']}")

        # Session validation
        print("\n4. Validating session...")
        valid_session = self.manager.get_session(
            alice_s1,
            validate_fingerprint={"user_id": "alice", "ip_address": "192.168.1.10", "user_agent": None}
        )
        print(f"✓ Session valid: {valid_session is not None}")

        # Statistics
        print("\n5. Session statistics...")
        stats = self.manager.get_stats()
        print(f"  Total sessions: {stats['total_sessions']}")
        print(f"  Unique users: {stats['unique_users']}")
        print(f"  Avg sessions/user: {stats['avg_sessions_per_user']:.1f}")

        # Cleanup
        print("\n6. Destroying sessions...")
        self.manager.destroy_session(alice_s2)
        self.manager.destroy_user_sessions("bob")

        print("\n✓ Demo complete")

    def close(self):
        """Close Redis connection"""
        self.redis.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Session Management System")
    subparsers = parser.add_subparsers(dest="command")

    # Create command
    create_parser = subparsers.add_parser("create", help="Create session")
    create_parser.add_argument("--user", required=True, help="User ID")
    create_parser.add_argument("--ip", help="IP address")

    # Get command
    get_parser = subparsers.add_parser("get", help="Get session")
    get_parser.add_argument("session_id", help="Session ID")

    # List command
    list_parser = subparsers.add_parser("list", help="List user sessions")
    list_parser.add_argument("--user", required=True, help="User ID")

    # Destroy command
    destroy_parser = subparsers.add_parser("destroy", help="Destroy session")
    destroy_parser.add_argument("session_id", help="Session ID")

    # Stats command
    subparsers.add_parser("stats", help="Show statistics")

    # Demo command
    subparsers.add_parser("demo", help="Run demo")

    parser.add_argument("--host", default="localhost", help="Redis host")
    parser.add_argument("--port", type=int, default=6379, help="Redis port")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    try:
        app = SessionManagerApp(host=args.host, port=args.port)

        if args.command == "create":
            session_id = app.manager.create_session(args.user, ip_address=args.ip)
            print(f"Session ID: {session_id}")

        elif args.command == "get":
            session = app.manager.get_session(args.session_id)
            if session:
                print(json.dumps(session, indent=2, default=str))

        elif args.command == "list":
            sessions = app.manager.list_user_sessions(args.user)
            for s in sessions:
                print(f"{s['session_id'][:16]}... @ {s['ip_address']:15s} (last: {s['last_activity']})")

        elif args.command == "destroy":
            app.manager.destroy_session(args.session_id)

        elif args.command == "stats":
            stats = app.manager.get_stats()
            print(json.dumps(stats, indent=2))

        elif args.command == "demo":
            app.demo()

        app.close()

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
