#!/usr/bin/env python3
"""
Job Queue Application with Workers

Demonstrates:
- Distributed job queue using Redis lists
- Multiple worker processes
- Job status tracking
- Priority queues
- Dead letter queue for failed jobs
- Job retry mechanism

Usage:
    # Start workers
    python job_queue_app.py worker --workers 3

    # Enqueue jobs
    python job_queue_app.py enqueue --count 10

    # Monitor queue
    python job_queue_app.py monitor

    # Process specific priority
    python job_queue_app.py worker --priority high
"""

import argparse
import json
import time
import uuid
import signal
import sys
from multiprocessing import Process
from datetime import datetime
from typing import Dict, Any, Optional

try:
    from optimized_redis import OptimizedRedis as Redis
except ImportError:
    try:
        from redis_wrapper import HighPerformanceRedis as Redis
    except ImportError:
        print("Error: Could not import CyRedis client")
        sys.exit(1)


class JobQueue:
    """Distributed job queue implementation."""

    def __init__(self, host='localhost', port=6379):
        self.redis = Redis(host=host, port=port)
        self.queue_key = "job_queue:tasks"
        self.processing_key = "job_queue:processing"
        self.completed_key = "job_queue:completed"
        self.failed_key = "job_queue:failed"
        self.status_key_prefix = "job_queue:status:"
        self.priority_queues = {
            "high": "job_queue:tasks:high",
            "normal": "job_queue:tasks:normal",
            "low": "job_queue:tasks:low",
        }
        self.running = True

    def enqueue(self, job_type: str, data: Dict[str, Any], priority: str = "normal") -> str:
        """Enqueue a new job."""
        job_id = str(uuid.uuid4())
        job = {
            "id": job_id,
            "type": job_type,
            "data": data,
            "priority": priority,
            "created_at": datetime.now().isoformat(),
            "status": "queued",
            "attempts": 0,
        }

        # Store job details
        self.redis.set(f"{self.status_key_prefix}{job_id}", json.dumps(job))

        # Add to appropriate priority queue
        queue_key = self.priority_queues.get(priority, self.queue_key)
        self.redis.rpush(queue_key, job_id)

        print(f"✓ Enqueued job {job_id} ({job_type}) with {priority} priority")
        return job_id

    def dequeue(self, priority: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Dequeue a job (blocking)."""
        if priority:
            # Dequeue from specific priority queue
            queue_key = self.priority_queues.get(priority, self.queue_key)
            result = self.redis.blpop(queue_key, timeout=1)
        else:
            # Check all queues in priority order
            queue_keys = [
                self.priority_queues["high"],
                self.priority_queues["normal"],
                self.priority_queues["low"],
            ]
            result = self.redis.blpop(queue_keys, timeout=1)

        if not result:
            return None

        queue, job_id = result
        if isinstance(job_id, bytes):
            job_id = job_id.decode('utf-8')

        # Get job details
        job_data = self.redis.get(f"{self.status_key_prefix}{job_id}")
        if not job_data:
            return None

        if isinstance(job_data, bytes):
            job_data = job_data.decode('utf-8')

        job = json.loads(job_data)

        # Move to processing
        self.redis.sadd(self.processing_key, job_id)
        job["status"] = "processing"
        job["started_at"] = datetime.now().isoformat()
        self.redis.set(f"{self.status_key_prefix}{job_id}", json.dumps(job))

        return job

    def complete_job(self, job_id: str, result: Any = None):
        """Mark job as completed."""
        job_data = self.redis.get(f"{self.status_key_prefix}{job_id}")
        if job_data:
            if isinstance(job_data, bytes):
                job_data = job_data.decode('utf-8')

            job = json.loads(job_data)
            job["status"] = "completed"
            job["completed_at"] = datetime.now().isoformat()
            job["result"] = result

            self.redis.set(f"{self.status_key_prefix}{job_id}", json.dumps(job))
            self.redis.srem(self.processing_key, job_id)
            self.redis.sadd(self.completed_key, job_id)

            print(f"✓ Completed job {job_id}")

    def fail_job(self, job_id: str, error: str, retry: bool = True):
        """Mark job as failed and optionally retry."""
        job_data = self.redis.get(f"{self.status_key_prefix}{job_id}")
        if not job_data:
            return

        if isinstance(job_data, bytes):
            job_data = job_data.decode('utf-8')

        job = json.loads(job_data)
        job["attempts"] += 1
        job["last_error"] = error
        job["last_attempt_at"] = datetime.now().isoformat()

        # Retry logic
        max_attempts = 3
        if retry and job["attempts"] < max_attempts:
            # Re-queue for retry
            job["status"] = "queued"
            self.redis.set(f"{self.status_key_prefix}{job_id}", json.dumps(job))
            self.redis.srem(self.processing_key, job_id)

            queue_key = self.priority_queues.get(job["priority"], self.queue_key)
            self.redis.rpush(queue_key, job_id)

            print(f"↻ Retrying job {job_id} (attempt {job['attempts']}/{max_attempts})")
        else:
            # Move to failed queue
            job["status"] = "failed"
            job["failed_at"] = datetime.now().isoformat()
            self.redis.set(f"{self.status_key_prefix}{job_id}", json.dumps(job))
            self.redis.srem(self.processing_key, job_id)
            self.redis.sadd(self.failed_key, job_id)

            print(f"✗ Job {job_id} failed permanently: {error}")

    def get_stats(self) -> Dict[str, int]:
        """Get queue statistics."""
        return {
            "high_priority": self.redis.llen(self.priority_queues["high"]),
            "normal_priority": self.redis.llen(self.priority_queues["normal"]),
            "low_priority": self.redis.llen(self.priority_queues["low"]),
            "processing": self.redis.scard(self.processing_key),
            "completed": self.redis.scard(self.completed_key),
            "failed": self.redis.scard(self.failed_key),
        }


def process_job(job: Dict[str, Any]) -> Any:
    """
    Process a job (simulate work).

    In a real application, this would dispatch to different handlers
    based on job type.
    """
    job_type = job["type"]
    data = job["data"]

    print(f"  Processing {job_type}: {data}")

    # Simulate different job types
    if job_type == "email":
        time.sleep(0.5)  # Simulate sending email
        return {"sent": True, "to": data.get("to")}

    elif job_type == "data_processing":
        time.sleep(1.0)  # Simulate heavy processing
        return {"processed": data.get("records", 0)}

    elif job_type == "report":
        time.sleep(2.0)  # Simulate report generation
        return {"report_id": str(uuid.uuid4())}

    elif job_type == "failing_job":
        # Simulate occasional failures
        import random
        if random.random() < 0.3:
            raise Exception("Random failure occurred")
        return {"status": "ok"}

    else:
        time.sleep(0.1)
        return {"status": "completed"}


def worker(worker_id: int, priority: Optional[str] = None):
    """Worker process that processes jobs."""
    queue = JobQueue()

    def signal_handler(sig, frame):
        print(f"\n[Worker {worker_id}] Shutting down...")
        queue.running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print(f"[Worker {worker_id}] Started (priority: {priority or 'all'})")

    while queue.running:
        try:
            job = queue.dequeue(priority=priority)

            if job:
                print(f"[Worker {worker_id}] Processing job {job['id']}")

                try:
                    result = process_job(job)
                    queue.complete_job(job["id"], result)
                except Exception as e:
                    print(f"[Worker {worker_id}] Job {job['id']} failed: {e}")
                    queue.fail_job(job["id"], str(e))

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"[Worker {worker_id}] Error: {e}")
            time.sleep(1)

    print(f"[Worker {worker_id}] Stopped")


def enqueue_jobs(count: int = 10):
    """Enqueue test jobs."""
    queue = JobQueue()

    job_types = [
        ("email", {"to": "user@example.com", "subject": "Test"}),
        ("data_processing", {"records": 1000}),
        ("report", {"type": "monthly"}),
        ("failing_job", {"data": "test"}),
    ]

    priorities = ["high", "normal", "low"]

    for i in range(count):
        job_type, data = job_types[i % len(job_types)]
        priority = priorities[i % len(priorities)]
        queue.enqueue(job_type, data, priority=priority)

    print(f"\n✓ Enqueued {count} jobs")


def monitor():
    """Monitor queue statistics."""
    queue = JobQueue()

    def signal_handler(sig, frame):
        print("\nStopping monitor...")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    print("Job Queue Monitor (Ctrl+C to stop)\n")

    try:
        while True:
            stats = queue.get_stats()

            print("\r" + " " * 80, end="")  # Clear line
            print(
                f"\r📊 High: {stats['high_priority']} | "
                f"Normal: {stats['normal_priority']} | "
                f"Low: {stats['low_priority']} | "
                f"Processing: {stats['processing']} | "
                f"Completed: {stats['completed']} | "
                f"Failed: {stats['failed']}",
                end="",
                flush=True
            )

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n\nStopped")


def main():
    parser = argparse.ArgumentParser(description="Job Queue Application")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Worker command
    worker_parser = subparsers.add_parser("worker", help="Start worker")
    worker_parser.add_argument("--workers", type=int, default=1, help="Number of workers")
    worker_parser.add_argument("--priority", choices=["high", "normal", "low"], help="Priority queue to process")

    # Enqueue command
    enqueue_parser = subparsers.add_parser("enqueue", help="Enqueue jobs")
    enqueue_parser.add_argument("--count", type=int, default=10, help="Number of jobs to enqueue")

    # Monitor command
    subparsers.add_parser("monitor", help="Monitor queue")

    args = parser.parse_args()

    if args.command == "worker":
        if args.workers > 1:
            processes = []
            for i in range(args.workers):
                p = Process(target=worker, args=(i, args.priority))
                p.start()
                processes.append(p)

            # Wait for all processes
            try:
                for p in processes:
                    p.join()
            except KeyboardInterrupt:
                print("\nShutting down workers...")
                for p in processes:
                    p.terminate()
                for p in processes:
                    p.join()
        else:
            worker(0, args.priority)

    elif args.command == "enqueue":
        enqueue_jobs(args.count)

    elif args.command == "monitor":
        monitor()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
