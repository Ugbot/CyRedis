#!/usr/bin/env python3
"""
Real-Time Leaderboard Application

A real-time leaderboard system demonstrating CyRedis sorted sets:
- Global and time-based leaderboards
- Score updates and rankings
- Percentile calculations
- Leaderboard pagination
- Multiple scoring dimensions

Features:
- Real-time score updates
- Efficient rank queries
- Top N and bottom N queries
- User rank and score lookup
- Range queries
- Time-windowed leaderboards

Usage:
    # Add scores
    python leaderboard_app.py add --user alice --score 1500

    # Show top players
    python leaderboard_app.py top --count 10

    # Get user rank
    python leaderboard_app.py rank --user alice

    # Simulate game
    python leaderboard_app.py simulate --players 100 --rounds 50

    # Demo mode
    python leaderboard_app.py demo
"""

import sys
import argparse
import random
import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime

try:
    from optimized_redis import OptimizedRedis
except ImportError:
    print("Error: CyRedis not properly installed. Run build_optimized.sh first.")
    sys.exit(1)


class Leaderboard:
    """Real-time leaderboard using Redis sorted sets"""

    def __init__(self, redis: OptimizedRedis, name: str = "global"):
        """Initialize leaderboard

        Args:
            redis: Redis client instance
            name: Leaderboard name
        """
        self.redis = redis
        self.name = name
        self.key = f"leaderboard:{name}"

    def add_score(self, user: str, score: float, increment: bool = False) -> float:
        """Add or update user score

        Args:
            user: User identifier
            score: User score
            increment: If True, increment existing score

        Returns:
            New score
        """
        if increment:
            new_score = self.redis.zincrby(self.key, score, user)
        else:
            self.redis.zadd(self.key, {user: score})
            new_score = score

        return new_score

    def get_score(self, user: str) -> Optional[float]:
        """Get user's score

        Args:
            user: User identifier

        Returns:
            User's score or None
        """
        score = self.redis.zscore(self.key, user)
        return score

    def get_rank(self, user: str, reverse: bool = True) -> Optional[int]:
        """Get user's rank (1-based)

        Args:
            user: User identifier
            reverse: If True, highest score = rank 1

        Returns:
            User's rank (1-based) or None
        """
        if reverse:
            rank = self.redis.zrevrank(self.key, user)
        else:
            rank = self.redis.zrank(self.key, user)

        return rank + 1 if rank is not None else None

    def get_top(self, count: int = 10, with_scores: bool = True) -> List:
        """Get top N players

        Args:
            count: Number of players to return
            with_scores: Include scores in result

        Returns:
            List of (user, score) tuples or just users
        """
        results = self.redis.zrevrange(self.key, 0, count-1, withscores=with_scores)

        if with_scores:
            return [(user.decode() if isinstance(user, bytes) else user, score)
                    for user, score in results]
        else:
            return [user.decode() if isinstance(user, bytes) else user
                    for user in results]

    def get_bottom(self, count: int = 10, with_scores: bool = True) -> List:
        """Get bottom N players

        Args:
            count: Number of players to return
            with_scores: Include scores in result

        Returns:
            List of (user, score) tuples or just users
        """
        results = self.redis.zrange(self.key, 0, count-1, withscores=with_scores)

        if with_scores:
            return [(user.decode() if isinstance(user, bytes) else user, score)
                    for user, score in results]
        else:
            return [user.decode() if isinstance(user, bytes) else user
                    for user in results]

    def get_around_user(self, user: str, count: int = 5, with_scores: bool = True) -> List:
        """Get players around a user's rank

        Args:
            user: User identifier
            count: Number of players above and below
            with_scores: Include scores in result

        Returns:
            List of (user, score) tuples or just users
        """
        rank = self.redis.zrevrank(self.key, user)
        if rank is None:
            return []

        start = max(0, rank - count)
        end = rank + count

        results = self.redis.zrevrange(self.key, start, end, withscores=with_scores)

        if with_scores:
            return [(user.decode() if isinstance(user, bytes) else user, score)
                    for user, score in results]
        else:
            return [user.decode() if isinstance(user, bytes) else user
                    for user in results]

    def get_by_score_range(self, min_score: float, max_score: float,
                            with_scores: bool = True) -> List:
        """Get players in score range

        Args:
            min_score: Minimum score
            max_score: Maximum score
            with_scores: Include scores in result

        Returns:
            List of (user, score) tuples or just users
        """
        results = self.redis.zrevrangebyscore(
            self.key, max_score, min_score, withscores=with_scores
        )

        if with_scores:
            return [(user.decode() if isinstance(user, bytes) else user, score)
                    for user, score in results]
        else:
            return [user.decode() if isinstance(user, bytes) else user
                    for user in results]

    def get_percentile(self, user: str) -> Optional[float]:
        """Get user's percentile rank

        Args:
            user: User identifier

        Returns:
            Percentile (0-100)
        """
        rank = self.redis.zrevrank(self.key, user)
        if rank is None:
            return None

        total = self.redis.zcard(self.key)
        percentile = ((total - rank) / total) * 100

        return percentile

    def get_total_players(self) -> int:
        """Get total number of players

        Returns:
            Player count
        """
        return self.redis.zcard(self.key)

    def remove_player(self, user: str) -> bool:
        """Remove player from leaderboard

        Args:
            user: User identifier

        Returns:
            True if removed
        """
        return self.redis.zrem(self.key, user) > 0


class LeaderboardApp:
    """Leaderboard application"""

    def __init__(self, host: str = "localhost", port: int = 6379):
        """Initialize leaderboard app

        Args:
            host: Redis host
            port: Redis port
        """
        self.redis = OptimizedRedis(host=host, port=port)
        self.leaderboard = Leaderboard(self.redis, "game")
        print(f"✓ Connected to Redis at {host}:{port}")

    def show_top(self, count: int = 10):
        """Display top players"""
        print(f"\n🏆 Top {count} Players")
        print("="*60)

        top_players = self.leaderboard.get_top(count)

        if not top_players:
            print("No players yet")
            return

        for i, (user, score) in enumerate(top_players, 1):
            medal = ["🥇", "🥈", "🥉"][i-1] if i <= 3 else "  "
            print(f"{medal} {i:2d}. {user:20s} {score:10.0f} points")

    def show_user_stats(self, user: str):
        """Display user statistics"""
        print(f"\n👤 Stats for {user}")
        print("="*60)

        score = self.leaderboard.get_score(user)
        if score is None:
            print(f"User '{user}' not found on leaderboard")
            return

        rank = self.leaderboard.get_rank(user)
        percentile = self.leaderboard.get_percentile(user)
        total = self.leaderboard.get_total_players()

        print(f"Score:      {score:10.0f} points")
        print(f"Rank:       #{rank} of {total}")
        print(f"Percentile: {percentile:.1f}%")

        # Show players around this user
        print(f"\n📊 Players around {user}:")
        around = self.leaderboard.get_around_user(user, count=3)

        for player, player_score in around:
            if player == user:
                print(f"  → {player:20s} {player_score:10.0f} ← YOU")
            else:
                print(f"    {player:20s} {player_score:10.0f}")

    def simulate_game(self, num_players: int = 50, num_rounds: int = 20):
        """Simulate a game with random scores"""
        print(f"\n🎮 Simulating game with {num_players} players, {num_rounds} rounds")
        print("="*60)

        # Generate player names
        players = [f"player_{i:03d}" for i in range(num_players)]

        # Initialize with random starting scores
        print("Initializing players...")
        for player in players:
            initial_score = random.randint(0, 1000)
            self.leaderboard.add_score(player, initial_score)

        print(f"✓ {num_players} players initialized\n")

        # Simulate rounds
        for round_num in range(1, num_rounds + 1):
            print(f"Round {round_num}/{num_rounds}...")

            # Random players score points
            for _ in range(num_players // 2):
                player = random.choice(players)
                points = random.randint(10, 200)
                self.leaderboard.add_score(player, points, increment=True)

            # Show top 5 after each round
            if round_num % 5 == 0:
                print(f"\n  Top 5 after round {round_num}:")
                top5 = self.leaderboard.get_top(5)
                for i, (user, score) in enumerate(top5, 1):
                    print(f"    {i}. {user:15s} {score:8.0f}")
                print()

            time.sleep(0.2)

        # Final standings
        print("\n" + "="*60)
        print("🏁 Final Standings")
        print("="*60)
        self.show_top(10)

    def demo(self):
        """Run demonstration"""
        print("\n" + "="*60)
        print("Real-Time Leaderboard Demo")
        print("="*60)

        # Add some players
        print("\n1. Adding players...")
        players_scores = [
            ("alice", 2500),
            ("bob", 1800),
            ("charlie", 3200),
            ("diana", 2100),
            ("eve", 1500),
            ("frank", 2800),
            ("grace", 1900),
            ("henry", 2300),
            ("iris", 3000),
            ("jack", 1700)
        ]

        for user, score in players_scores:
            self.leaderboard.add_score(user, score)
            print(f"  ✓ Added {user} with {score} points")

        # Show top players
        self.show_top(10)

        # Show user stats
        self.show_user_stats("alice")

        # Increment score
        print("\n2. Alice scores 500 more points!")
        new_score = self.leaderboard.add_score("alice", 500, increment=True)
        print(f"  ✓ Alice's new score: {new_score}")

        self.show_user_stats("alice")

        # Score range query
        print("\n3. Players with 2000-3000 points:")
        players = self.leaderboard.get_by_score_range(2000, 3000)
        for user, score in players:
            print(f"  - {user:15s} {score:8.0f}")

        # Total players
        total = self.leaderboard.get_total_players()
        print(f"\n✓ Total players: {total}")

        print("\n✓ Demo complete")

    def close(self):
        """Close Redis connection"""
        self.redis.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Real-Time Leaderboard")
    subparsers = parser.add_subparsers(dest="command")

    # Add command
    add_parser = subparsers.add_parser("add", help="Add/update score")
    add_parser.add_argument("--user", required=True, help="User name")
    add_parser.add_argument("--score", type=float, required=True, help="Score")
    add_parser.add_argument("--increment", action="store_true", help="Increment existing score")

    # Top command
    top_parser = subparsers.add_parser("top", help="Show top players")
    top_parser.add_argument("--count", type=int, default=10, help="Number of players")

    # Rank command
    rank_parser = subparsers.add_parser("rank", help="Get user rank and stats")
    rank_parser.add_argument("--user", required=True, help="User name")

    # Simulate command
    sim_parser = subparsers.add_parser("simulate", help="Simulate game")
    sim_parser.add_argument("--players", type=int, default=50, help="Number of players")
    sim_parser.add_argument("--rounds", type=int, default=20, help="Number of rounds")

    # Demo command
    subparsers.add_parser("demo", help="Run demo")

    parser.add_argument("--host", default="localhost", help="Redis host")
    parser.add_argument("--port", type=int, default=6379, help="Redis port")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    try:
        app = LeaderboardApp(host=args.host, port=args.port)

        if args.command == "add":
            app.leaderboard.add_score(args.user, args.score, increment=args.increment)
            app.show_user_stats(args.user)

        elif args.command == "top":
            app.show_top(args.count)

        elif args.command == "rank":
            app.show_user_stats(args.user)

        elif args.command == "simulate":
            app.simulate_game(args.players, args.rounds)

        elif args.command == "demo":
            app.demo()

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
