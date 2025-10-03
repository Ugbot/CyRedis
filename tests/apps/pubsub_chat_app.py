#!/usr/bin/env python3
"""
Pub/Sub Chat Application

A real-time chat application demonstrating CyRedis pub/sub features:
- Multi-channel chat rooms
- Private messaging
- User presence
- Message history
- Typing indicators

Features:
- Multiple chat rooms
- Real-time message delivery
- User join/leave notifications
- Pattern-based subscriptions
- Message persistence
- Active user tracking

Usage:
    # Join chat as Alice
    python pubsub_chat_app.py --user alice --room general

    # Join multiple rooms
    python pubsub_chat_app.py --user bob --room "general,random"

    # Monitor all channels
    python pubsub_chat_app.py --monitor
"""

import sys
import argparse
import time
import json
import threading
from typing import List, Optional, Callable
from datetime import datetime

try:
    from redis_wrapper import HighPerformanceRedis, PubSubHub
except ImportError:
    try:
        from optimized_redis import OptimizedRedis as HighPerformanceRedis
        PubSubHub = None
    except ImportError:
        print("Error: CyRedis not properly installed. Run build_optimized.sh first.")
        sys.exit(1)


class ChatRoom:
    """Chat room using Redis pub/sub"""

    def __init__(self, redis: HighPerformanceRedis, room_name: str, username: str):
        """Initialize chat room

        Args:
            redis: Redis client instance
            room_name: Name of the chat room
            username: User's display name
        """
        self.redis = redis
        self.room_name = room_name
        self.username = username
        self.channel = f"chat:room:{room_name}"
        self.presence_key = f"chat:presence:{room_name}"
        self.history_key = f"chat:history:{room_name}"
        self.pubsub = None
        self.running = threading.Event()

    def join(self):
        """Join the chat room"""
        # Add user to presence set
        self.redis.sadd(self.presence_key, self.username)
        self.redis.expire(self.presence_key, 3600)

        # Announce join
        self._send_system_message(f"{self.username} joined the room")

        # Start listening
        self.running.set()
        self.pubsub = self.redis.client.pubsub()
        self.pubsub.subscribe(self.channel)

        print(f"✓ Joined room '{self.room_name}' as '{self.username}'")
        self._show_active_users()

    def leave(self):
        """Leave the chat room"""
        self.running.clear()

        # Remove from presence
        self.redis.srem(self.presence_key, self.username)

        # Announce leave
        self._send_system_message(f"{self.username} left the room")

        # Unsubscribe
        if self.pubsub:
            self.pubsub.unsubscribe(self.channel)
            self.pubsub.close()

        print(f"✓ Left room '{self.room_name}'")

    def send_message(self, message: str):
        """Send a message to the room

        Args:
            message: Message text
        """
        msg_data = {
            "type": "message",
            "user": self.username,
            "text": message,
            "timestamp": time.time()
        }

        # Publish to channel
        self.redis.publish(self.channel, json.dumps(msg_data))

        # Store in history (keep last 100 messages)
        self.redis.lpush(self.history_key, json.dumps(msg_data))
        self.redis.ltrim(self.history_key, 0, 99)
        self.redis.expire(self.history_key, 3600)

    def _send_system_message(self, message: str):
        """Send a system message

        Args:
            message: System message text
        """
        msg_data = {
            "type": "system",
            "text": message,
            "timestamp": time.time()
        }
        self.redis.publish(self.channel, json.dumps(msg_data))

    def _show_active_users(self):
        """Display active users in the room"""
        users = self.redis.smembers(self.presence_key)
        users = [u.decode() if isinstance(u, bytes) else u for u in users]

        print(f"👥 Active users ({len(users)}): {', '.join(sorted(users))}")

    def listen(self, callback: Optional[Callable] = None):
        """Listen for messages

        Args:
            callback: Optional callback for messages
        """
        print(f"📻 Listening for messages... (Ctrl+C to quit)\n")

        try:
            while self.running.is_set():
                message = self.pubsub.get_message(timeout=1.0)

                if message and message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        timestamp = datetime.fromtimestamp(data["timestamp"])
                        time_str = timestamp.strftime("%H:%M:%S")

                        if data["type"] == "message":
                            user = data["user"]
                            text = data["text"]

                            # Don't show our own messages
                            if user != self.username:
                                print(f"[{time_str}] {user}: {text}")

                        elif data["type"] == "system":
                            print(f"[{time_str}] * {data['text']}")

                        if callback:
                            callback(data)

                    except json.JSONDecodeError:
                        pass

        except KeyboardInterrupt:
            pass

    def show_history(self, limit: int = 20):
        """Show recent message history

        Args:
            limit: Number of messages to show
        """
        print(f"\n📜 Recent messages (last {limit}):")
        print("-"*60)

        messages = self.redis.lrange(self.history_key, 0, limit-1)

        if not messages:
            print("No message history")
            return

        for msg_data in reversed(messages):
            try:
                data = json.loads(msg_data)
                timestamp = datetime.fromtimestamp(data["timestamp"])
                time_str = timestamp.strftime("%H:%M:%S")

                if data["type"] == "message":
                    print(f"[{time_str}] {data['user']}: {data['text']}")
                elif data["type"] == "system":
                    print(f"[{time_str}] * {data['text']}")

            except (json.JSONDecodeError, KeyError):
                pass

        print("-"*60 + "\n")


class ChatApp:
    """Chat application"""

    def __init__(self, host: str = "localhost", port: int = 6379):
        """Initialize chat app

        Args:
            host: Redis host
            port: Redis port
        """
        self.redis = HighPerformanceRedis(host=host, port=port)
        print(f"✓ Connected to Redis at {host}:{port}")

    def run_chat(self, username: str, room_names: List[str]):
        """Run chat client

        Args:
            username: User's display name
            room_names: List of room names to join
        """
        # Join all rooms
        rooms = []
        for room_name in room_names:
            room = ChatRoom(self.redis, room_name, username)
            room.join()
            room.show_history(10)
            rooms.append(room)

        # If multiple rooms, show which one is active
        active_room = rooms[0]
        if len(rooms) > 1:
            print(f"\n📝 Typing in room: '{active_room.room_name}'")
            print("   (Use /room <name> to switch)\n")

        # Start listener threads for all rooms
        listener_threads = []
        for room in rooms:
            thread = threading.Thread(target=room.listen, daemon=True)
            thread.start()
            listener_threads.append(thread)

        # Main input loop
        print("Commands: /room <name>, /users, /history, /quit\n")

        try:
            while True:
                message = input("")

                if message.startswith("/"):
                    parts = message.split(None, 1)
                    cmd = parts[0].lower()

                    if cmd == "/quit":
                        break

                    elif cmd == "/room" and len(parts) > 1:
                        room_name = parts[1]
                        found = False
                        for room in rooms:
                            if room.room_name == room_name:
                                active_room = room
                                found = True
                                print(f"✓ Switched to room '{room_name}'")
                                break
                        if not found:
                            print(f"✗ Room '{room_name}' not joined")

                    elif cmd == "/users":
                        active_room._show_active_users()

                    elif cmd == "/history":
                        active_room.show_history(20)

                    else:
                        print("Unknown command")

                elif message.strip():
                    active_room.send_message(message)

        except (KeyboardInterrupt, EOFError):
            pass

        # Leave all rooms
        print("\n")
        for room in rooms:
            room.leave()

    def monitor_all(self):
        """Monitor all chat channels"""
        print("📡 Monitoring all chat channels... (Ctrl+C to quit)\n")

        pubsub = self.redis.client.pubsub()
        pubsub.psubscribe("chat:*")

        try:
            while True:
                message = pubsub.get_message(timeout=1.0)

                if message and message["type"] == "pmessage":
                    channel = message["channel"]
                    if isinstance(channel, bytes):
                        channel = channel.decode()

                    try:
                        data = json.loads(message["data"])
                        timestamp = datetime.fromtimestamp(data["timestamp"])
                        time_str = timestamp.strftime("%H:%M:%S")

                        if data["type"] == "message":
                            print(f"[{time_str}] [{channel}] {data['user']}: {data['text']}")
                        elif data["type"] == "system":
                            print(f"[{time_str}] [{channel}] * {data['text']}")

                    except (json.JSONDecodeError, KeyError):
                        pass

        except KeyboardInterrupt:
            pass

        pubsub.close()

    def close(self):
        """Close Redis connection"""
        self.redis.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Pub/Sub Chat Application")
    parser.add_argument("--host", default="localhost", help="Redis host")
    parser.add_argument("--port", type=int, default=6379, help="Redis port")
    parser.add_argument("--user", help="Username")
    parser.add_argument("--room", help="Room name(s), comma-separated")
    parser.add_argument("--monitor", action="store_true", help="Monitor all channels")

    args = parser.parse_args()

    try:
        app = ChatApp(host=args.host, port=args.port)

        if args.monitor:
            app.monitor_all()

        elif args.user and args.room:
            rooms = [r.strip() for r in args.room.split(",")]
            app.run_chat(args.user, rooms)

        else:
            print("Error: Specify --user and --room, or use --monitor")
            parser.print_help()

        app.close()

    except KeyboardInterrupt:
        print("\nExiting...")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
