#!/usr/bin/env python3
"""
Task Scheduling System

A distributed task scheduler demonstrating CyRedis for scheduled jobs:
- Cron-like scheduling
- One-time delayed tasks
- Recurring tasks
- Task dependencies
- Distributed execution

Features:
- Schedule tasks with cron expressions or delays
- Recurring task support
- Task execution tracking
- Multiple worker support
- Task cancellation
- Execution history

Usage:
    # Start scheduler worker
    python task_scheduler_app.py worker

    # Schedule a task
    python task_scheduler_app.py schedule "cleanup_logs" --delay 3600

    # List scheduled tasks
    python task_scheduler_app.py list

    # Cancel a task
    python task_scheduler_app.py cancel <task_id>
"""

import sys
import argparse
import time
import json
import uuid
from typing import Dict, Any, Optional, Callable, List
from datetime import datetime, timedelta
from threading import Thread, Event

try:
    from optimized_redis import OptimizedRedis
except ImportError:
    print("Error: CyRedis not properly installed. Run build_optimized.sh first.")
    sys.exit(1)


class TaskScheduler:
    """Distributed task scheduler using Redis sorted sets"""

    def __init__(self, redis: OptimizedRedis, namespace: str = "scheduler"):
        """Initialize task scheduler

        Args:
            redis: Redis client instance
            namespace: Namespace for scheduler keys
        """
        self.redis = redis
        self.namespace = namespace
        self.scheduled_key = f"{namespace}:scheduled"
        self.executing_key = f"{namespace}:executing"
        self.completed_key = f"{namespace}:completed"
        self.failed_key = f"{namespace}:failed"
        self.tasks = {}  # Task registry
        self.running = Event()

    def register_task(self, name: str, handler: Callable):
        """Register a task handler

        Args:
            name: Task name
            handler: Function to execute
        """
        self.tasks[name] = handler
        print(f"✓ Registered task: {name}")

    def schedule_task(self, task_name: str, args: Optional[Dict] = None,
                      delay: Optional[int] = None, run_at: Optional[datetime] = None,
                      recurring: Optional[int] = None) -> str:
        """Schedule a task for execution

        Args:
            task_name: Name of registered task
            args: Task arguments
            delay: Delay in seconds from now
            run_at: Specific datetime to run
            recurring: Recurring interval in seconds

        Returns:
            Task ID
        """
        task_id = str(uuid.uuid4())

        # Calculate execution time
        if run_at:
            execute_at = int(run_at.timestamp())
        elif delay:
            execute_at = int(time.time()) + delay
        else:
            execute_at = int(time.time())

        # Create task data
        task_data = {
            "id": task_id,
            "name": task_name,
            "args": args or {},
            "scheduled_at": int(time.time()),
            "execute_at": execute_at,
            "recurring": recurring,
            "attempts": 0
        }

        # Store in scheduled sorted set (score = execution time)
        self.redis.zadd(self.scheduled_key, {json.dumps(task_data): execute_at})

        print(f"✓ Scheduled task '{task_name}' (id: {task_id[:8]}...)")
        return task_id

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a scheduled task

        Args:
            task_id: Task ID to cancel

        Returns:
            True if task was cancelled
        """
        # Find and remove from scheduled tasks
        scheduled = self.redis.zrange(self.scheduled_key, 0, -1)

        for task_json in scheduled:
            task = json.loads(task_json)
            if task["id"] == task_id:
                self.redis.zrem(self.scheduled_key, task_json)
                print(f"✓ Cancelled task {task_id[:8]}...")
                return True

        print(f"✗ Task {task_id[:8]}... not found")
        return False

    def list_scheduled(self, limit: int = 20) -> List[Dict]:
        """List scheduled tasks

        Args:
            limit: Maximum number of tasks to return

        Returns:
            List of scheduled tasks
        """
        scheduled = self.redis.zrange(self.scheduled_key, 0, limit-1, withscores=True)

        tasks = []
        for task_json, score in scheduled:
            task = json.loads(task_json)
            task["execute_at_dt"] = datetime.fromtimestamp(score)
            tasks.append(task)

        return tasks

    def get_stats(self) -> Dict[str, int]:
        """Get scheduler statistics

        Returns:
            Dictionary with counts
        """
        return {
            "scheduled": self.redis.zcard(self.scheduled_key),
            "executing": self.redis.zcard(self.executing_key),
            "completed": self.redis.zcard(self.completed_key),
            "failed": self.redis.zcard(self.failed_key)
        }

    def _execute_task(self, task: Dict) -> bool:
        """Execute a task

        Args:
            task: Task data

        Returns:
            True if successful
        """
        task_name = task["name"]
        task_id = task["id"]

        if task_name not in self.tasks:
            print(f"✗ Unknown task: {task_name}")
            return False

        try:
            print(f"⚙️  Executing task '{task_name}' (id: {task_id[:8]}...)")

            # Execute task handler
            handler = self.tasks[task_name]
            handler(**task.get("args", {}))

            print(f"✓ Completed task '{task_name}' (id: {task_id[:8]}...)")
            return True

        except Exception as e:
            print(f"✗ Failed task '{task_name}': {e}")
            return False

    def run_worker(self, worker_id: str, poll_interval: int = 1):
        """Run scheduler worker

        Args:
            worker_id: Worker identifier
            poll_interval: Polling interval in seconds
        """
        print(f"🚀 Starting scheduler worker: {worker_id}")
        self.running.set()

        while self.running.is_set():
            try:
                now = int(time.time())

                # Get tasks that are due (score <= now)
                due_tasks = self.redis.zrangebyscore(
                    self.scheduled_key,
                    "-inf",
                    now,
                    start=0,
                    num=10
                )

                for task_json in due_tasks:
                    task = json.loads(task_json)

                    # Remove from scheduled
                    self.redis.zrem(self.scheduled_key, task_json)

                    # Add to executing
                    self.redis.zadd(self.executing_key, {task_json: now})

                    # Execute task
                    task["attempts"] += 1
                    success = self._execute_task(task)

                    # Remove from executing
                    self.redis.zrem(self.executing_key, task_json)

                    if success:
                        # Add to completed
                        task["completed_at"] = int(time.time())
                        self.redis.zadd(self.completed_key, {json.dumps(task): now})

                        # Reschedule if recurring
                        if task.get("recurring"):
                            next_run = int(time.time()) + task["recurring"]
                            task["execute_at"] = next_run
                            task["attempts"] = 0
                            self.redis.zadd(self.scheduled_key, {json.dumps(task): next_run})
                            print(f"  ↻ Rescheduled recurring task for {datetime.fromtimestamp(next_run)}")
                    else:
                        # Add to failed
                        task["failed_at"] = int(time.time())
                        self.redis.zadd(self.failed_key, {json.dumps(task): now})

                time.sleep(poll_interval)

            except Exception as e:
                print(f"Worker error: {e}")
                time.sleep(poll_interval)

        print(f"✓ Worker {worker_id} stopped")

    def stop(self):
        """Stop the scheduler worker"""
        self.running.clear()


class TaskSchedulerApp:
    """Task scheduler application"""

    def __init__(self, host: str = "localhost", port: int = 6379):
        """Initialize scheduler app

        Args:
            host: Redis host
            port: Redis port
        """
        self.redis = OptimizedRedis(host=host, port=port)
        self.scheduler = TaskScheduler(self.redis)
        self._register_demo_tasks()
        print(f"✓ Connected to Redis at {host}:{port}")

    def _register_demo_tasks(self):
        """Register demo task handlers"""

        def cleanup_logs(**kwargs):
            """Clean up old logs"""
            days = kwargs.get("days", 7)
            print(f"  📁 Cleaning up logs older than {days} days...")
            time.sleep(0.5)  # Simulate work

        def send_report(**kwargs):
            """Send scheduled report"""
            report_type = kwargs.get("type", "daily")
            print(f"  📊 Generating {report_type} report...")
            time.sleep(0.5)

        def backup_database(**kwargs):
            """Backup database"""
            database = kwargs.get("database", "main")
            print(f"  💾 Backing up database: {database}...")
            time.sleep(1.0)

        def process_batch(**kwargs):
            """Process batch job"""
            batch_id = kwargs.get("batch_id", "unknown")
            print(f"  🔄 Processing batch: {batch_id}...")
            time.sleep(0.5)

        def health_check(**kwargs):
            """System health check"""
            print(f"  ❤️  Running health check...")
            time.sleep(0.2)

        self.scheduler.register_task("cleanup_logs", cleanup_logs)
        self.scheduler.register_task("send_report", send_report)
        self.scheduler.register_task("backup_database", backup_database)
        self.scheduler.register_task("process_batch", process_batch)
        self.scheduler.register_task("health_check", health_check)

    def schedule_demo_tasks(self):
        """Schedule some demo tasks"""
        print("\n📅 Scheduling demo tasks...")

        # One-time tasks with delays
        self.scheduler.schedule_task("cleanup_logs", {"days": 30}, delay=5)
        self.scheduler.schedule_task("send_report", {"type": "weekly"}, delay=10)
        self.scheduler.schedule_task("backup_database", {"database": "users"}, delay=15)

        # Recurring task (every 20 seconds)
        self.scheduler.schedule_task("health_check", recurring=20, delay=2)

        print("✓ Demo tasks scheduled")

    def list_tasks(self):
        """List scheduled tasks"""
        print("\n📋 Scheduled Tasks")
        print("="*80)

        tasks = self.scheduler.list_scheduled(limit=50)

        if not tasks:
            print("No tasks scheduled")
            return

        for i, task in enumerate(tasks, 1):
            task_name = task["name"]
            task_id = task["id"][:8]
            execute_at = task["execute_at_dt"].strftime("%Y-%m-%d %H:%M:%S")
            recurring = f" (recurring: {task['recurring']}s)" if task.get("recurring") else ""

            print(f"{i}. [{task_id}...] {task_name:20s} @ {execute_at}{recurring}")

    def show_stats(self):
        """Show scheduler statistics"""
        print("\n📊 Scheduler Statistics")
        print("="*60)

        stats = self.scheduler.get_stats()
        print(f"Scheduled:  {stats['scheduled']:4d}")
        print(f"Executing:  {stats['executing']:4d}")
        print(f"Completed:  {stats['completed']:4d}")
        print(f"Failed:     {stats['failed']:4d}")

    def run_worker(self, worker_id: str = "worker_1"):
        """Run scheduler worker"""
        try:
            self.scheduler.run_worker(worker_id)
        except KeyboardInterrupt:
            print("\n⚠️  Stopping worker...")
            self.scheduler.stop()

    def close(self):
        """Close Redis connection"""
        self.redis.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Task Scheduling System",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Worker command
    worker_parser = subparsers.add_parser("worker", help="Run scheduler worker")
    worker_parser.add_argument("--id", default="worker_1", help="Worker ID")

    # Schedule command
    schedule_parser = subparsers.add_parser("schedule", help="Schedule a task")
    schedule_parser.add_argument("task", help="Task name")
    schedule_parser.add_argument("--delay", type=int, help="Delay in seconds")
    schedule_parser.add_argument("--recurring", type=int, help="Recurring interval in seconds")

    # List command
    subparsers.add_parser("list", help="List scheduled tasks")

    # Stats command
    subparsers.add_parser("stats", help="Show statistics")

    # Demo command
    subparsers.add_parser("demo", help="Run demo")

    # Cancel command
    cancel_parser = subparsers.add_parser("cancel", help="Cancel a task")
    cancel_parser.add_argument("task_id", help="Task ID to cancel")

    # Global args
    parser.add_argument("--host", default="localhost", help="Redis host")
    parser.add_argument("--port", type=int, default=6379, help="Redis port")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    try:
        app = TaskSchedulerApp(host=args.host, port=args.port)

        if args.command == "worker":
            app.run_worker(worker_id=args.id)

        elif args.command == "schedule":
            app.scheduler.schedule_task(
                args.task,
                delay=args.delay,
                recurring=args.recurring
            )

        elif args.command == "list":
            app.list_tasks()

        elif args.command == "stats":
            app.show_stats()

        elif args.command == "demo":
            print("\n" + "="*60)
            print("Task Scheduler Demo")
            print("="*60)
            app.schedule_demo_tasks()
            app.list_tasks()
            print("\nStarting worker to process tasks...")
            print("Press Ctrl+C to stop\n")
            time.sleep(2)
            app.run_worker()

        elif args.command == "cancel":
            app.scheduler.cancel_task(args.task_id)

        app.close()

    except KeyboardInterrupt:
        print("\nExiting...")
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
