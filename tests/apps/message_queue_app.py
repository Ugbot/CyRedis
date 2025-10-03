#!/usr/bin/env python3
"""
Message Queue Application with Producer/Consumer

A complete message queue system demonstrating CyRedis ReliableQueue with:
- Producer/Consumer pattern
- Message priorities
- Visibility timeouts
- Automatic retries
- Dead letter queues

Features:
- Multiple producers and consumers
- Priority-based message processing
- Automatic failure handling and retries
- Dead letter queue for failed messages
- Queue statistics and monitoring
- Graceful shutdown

Usage:
    # Start consumer
    python message_queue_app.py consumer --workers 3

    # Start producer
    python message_queue_app.py producer --count 100

    # Monitor queue
    python message_queue_app.py monitor

    # Process dead letter queue
    python message_queue_app.py dlq
"""

import sys
import argparse
import time
import json
import signal
import random
from typing import Dict, Any, Optional
from threading import Thread, Event
from datetime import datetime

try:
    from redis_wrapper import HighPerformanceRedis, ReliableQueue
except ImportError:
    print("Error: CyRedis not properly installed. Run build_optimized.sh first.")
    sys.exit(1)


class MessageQueueApp:
    """Message queue application with producer/consumer pattern"""

    def __init__(self, host: str = "localhost", port: int = 6379, queue_name: str = "messages"):
        """Initialize the message queue app

        Args:
            host: Redis host
            port: Redis port
            queue_name: Base name for the queue
        """
        self.redis = HighPerformanceRedis(host=host, port=port)
        self.queue = ReliableQueue(
            self.redis,
            queue_name=queue_name,
            visibility_timeout=30,  # 30 seconds to process
            max_retries=3,
            dead_letter_queue=f"{queue_name}:dlq"
        )
        self.queue_name = queue_name
        self.shutdown_event = Event()
        print(f"✓ Connected to Redis at {host}:{port}")
        print(f"✓ Using queue: '{queue_name}'")

    def produce_messages(self, count: int = 10, delay: int = 0):
        """Produce messages to the queue

        Args:
            count: Number of messages to produce
            delay: Delay between messages in milliseconds
        """
        print(f"\n📤 Starting producer - will send {count} messages")
        print("="*60)

        message_types = ["order", "notification", "email", "report"]
        priorities = {"critical": 0, "high": 1, "normal": 2, "low": 3}

        for i in range(count):
            message_type = random.choice(message_types)
            priority_name = random.choice(list(priorities.keys()))
            priority = priorities[priority_name]

            # Create message payload
            message = {
                "id": f"msg_{i}",
                "type": message_type,
                "timestamp": datetime.now().isoformat(),
                "data": {
                    "sequence": i,
                    "content": f"Message content {i}"
                }
            }

            # Randomly fail some messages (for testing retry logic)
            if random.random() < 0.1:  # 10% chance
                message["should_fail"] = True

            # Push to queue
            msg_id = self.queue.push(message, priority=priority)
            print(f"✓ Sent [{priority_name:8s}] {message_type:12s} (id: {msg_id[:8]}...)")

            if delay > 0 and i < count - 1:
                time.sleep(delay / 1000.0)

        print(f"\n✓ Producer finished - sent {count} messages")

    def consume_messages(self, worker_id: str, duration: Optional[int] = None):
        """Consume messages from the queue

        Args:
            worker_id: Identifier for this worker
            duration: Run for specified seconds (None = run until shutdown)
        """
        print(f"\n📥 Starting consumer '{worker_id}'")
        print("="*60)

        start_time = time.time()
        processed = 0
        failed = 0

        while not self.shutdown_event.is_set():
            # Check duration
            if duration and (time.time() - start_time) > duration:
                break

            # Pop message from queue
            messages = self.queue.pop(count=1)

            if not messages:
                time.sleep(0.5)  # Wait before trying again
                continue

            for message in messages:
                try:
                    msg_id = message.get("id", "unknown")
                    msg_type = message.get("type", "unknown")
                    msg_data = message.get("data", {})

                    print(f"⚙️  [{worker_id}] Processing {msg_type} (id: {msg_id})")

                    # Simulate message processing
                    time.sleep(random.uniform(0.1, 0.5))

                    # Check if message should fail (for testing)
                    if message.get("should_fail", False):
                        raise Exception("Simulated failure for testing")

                    # Acknowledge successful processing
                    self.queue.acknowledge(message["id"])
                    processed += 1
                    print(f"✓ [{worker_id}] Completed {msg_type} (total: {processed})")

                except Exception as e:
                    # Mark message as failed (will retry or move to DLQ)
                    result = self.queue.fail(message["id"], str(e))
                    failed += 1

                    if result["action"] == "retry":
                        print(f"⚠️  [{worker_id}] Failed {msg_type} - retry {result['retry_count']}/{self.queue.max_retries}")
                    else:
                        print(f"❌ [{worker_id}] Failed {msg_type} - moved to DLQ")

        print(f"\n✓ Consumer '{worker_id}' finished - processed: {processed}, failed: {failed}")

    def run_consumer_pool(self, num_workers: int = 3, duration: Optional[int] = None):
        """Run multiple consumer workers

        Args:
            num_workers: Number of worker threads
            duration: Run for specified seconds (None = run forever)
        """
        print(f"\n🚀 Starting consumer pool with {num_workers} workers")

        # Start worker threads
        threads = []
        for i in range(num_workers):
            worker_id = f"worker_{i+1}"
            thread = Thread(target=self.consume_messages, args=(worker_id, duration))
            thread.daemon = True
            thread.start()
            threads.append(thread)

        print(f"✓ {num_workers} workers started - Press Ctrl+C to stop")

        # Wait for threads
        try:
            for thread in threads:
                thread.join()
        except KeyboardInterrupt:
            print("\n⚠️  Shutting down workers...")
            self.shutdown_event.set()
            for thread in threads:
                thread.join(timeout=5)

    def monitor_queue(self, interval: int = 2):
        """Monitor queue statistics

        Args:
            interval: Update interval in seconds
        """
        print("\n📊 Queue Monitor - Press Ctrl+C to stop")
        print("="*60)

        try:
            while True:
                # Get queue lengths
                pending = self.redis.client.zcard(self.queue.pending_key)
                processing = self.redis.client.zcard(self.queue.processing_key)
                failed = self.redis.client.zcard(self.queue.failed_key)
                dead = self.redis.client.zcard(self.queue.dead_key)

                # Display stats
                timestamp = datetime.now().strftime("%H:%M:%S")
                print(f"[{timestamp}] Pending: {pending:4d} | Processing: {processing:4d} | "
                      f"Failed: {failed:4d} | DLQ: {dead:4d}")

                time.sleep(interval)

        except KeyboardInterrupt:
            print("\n✓ Monitor stopped")

    def process_dead_letter_queue(self, limit: int = 10):
        """Process messages in dead letter queue

        Args:
            limit: Maximum number of messages to display
        """
        print(f"\n💀 Dead Letter Queue (showing up to {limit} messages)")
        print("="*60)

        # Get messages from DLQ
        dlq_messages = self.redis.client.zrange(self.queue.dead_key, 0, limit-1, withscores=True)

        if not dlq_messages:
            print("✓ No messages in dead letter queue")
            return

        for i, (msg_json, timestamp) in enumerate(dlq_messages, 1):
            try:
                message = json.loads(msg_json)
                msg_type = message.get("type", "unknown")
                msg_id = message.get("id", "unknown")
                retry_count = message.get("retry_count", 0)
                last_error = message.get("last_error", "unknown")
                failed_at = datetime.fromtimestamp(message.get("failed_at", 0))

                print(f"\n{i}. Message ID: {msg_id}")
                print(f"   Type: {msg_type}")
                print(f"   Retries: {retry_count}")
                print(f"   Failed at: {failed_at}")
                print(f"   Error: {last_error}")

            except json.JSONDecodeError:
                print(f"\n{i}. Invalid message (corrupted JSON)")

        print(f"\n✓ Total in DLQ: {len(dlq_messages)}")

    def cleanup(self):
        """Clean up all queue data"""
        print("\n🗑️  Cleaning up queue data...")

        keys_to_delete = [
            self.queue.pending_key,
            self.queue.processing_key,
            self.queue.failed_key,
            self.queue.dead_key,
            f"{self.queue_name}:delayed"
        ]

        deleted = 0
        for key in keys_to_delete:
            if self.redis.client.delete(key):
                deleted += 1

        print(f"✓ Deleted {deleted} queue keys")

    def close(self):
        """Close Redis connection"""
        self.redis.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Message Queue Application with Producer/Consumer",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("mode", choices=["producer", "consumer", "monitor", "dlq", "cleanup"],
                        help="Operation mode")
    parser.add_argument("--host", default="localhost", help="Redis host")
    parser.add_argument("--port", type=int, default=6379, help="Redis port")
    parser.add_argument("--queue", default="messages", help="Queue name")
    parser.add_argument("--count", type=int, default=10, help="Number of messages (producer)")
    parser.add_argument("--workers", type=int, default=3, help="Number of workers (consumer)")
    parser.add_argument("--duration", type=int, help="Run duration in seconds")
    parser.add_argument("--delay", type=int, default=0, help="Delay between messages in ms (producer)")

    args = parser.parse_args()

    try:
        app = MessageQueueApp(host=args.host, port=args.port, queue_name=args.queue)

        if args.mode == "producer":
            app.produce_messages(count=args.count, delay=args.delay)

        elif args.mode == "consumer":
            app.run_consumer_pool(num_workers=args.workers, duration=args.duration)

        elif args.mode == "monitor":
            app.monitor_queue()

        elif args.mode == "dlq":
            app.process_dead_letter_queue()

        elif args.mode == "cleanup":
            confirm = input("Are you sure you want to delete all queue data? (yes/no): ")
            if confirm.lower() == "yes":
                app.cleanup()

        app.close()

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
