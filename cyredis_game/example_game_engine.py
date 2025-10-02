#!/usr/bin/env python3
"""
Example demonstrating CyRedis Game Engine
Authoritative ECS on Redis with zones, ticks, and scaling
"""

import time
import threading
import asyncio
from .game_engine import GameEngine, run_zone_worker

def demonstrate_basic_game_setup():
    """Demonstrate basic game engine setup and entity spawning."""
    print("🎮 CyRedis Game Engine Demo")
    print("=" * 40)

    # Create game engine
    engine = GameEngine()
    print("✓ Created game engine")

    # Load game functions
    engine.load_functions()

    # Get a world
    world = engine.get_world("demo_world")
    print("✓ Created demo world")

    # Get a zone
    zone = world.get_zone("zone_0")
    print("✓ Created zone 0")

    # Spawn some entities
    print("\n🏃 Spawning entities...")

    entities = [
        ("player_1", "player", 100.0, 200.0, 0.0, 0.0),
        ("enemy_1", "enemy", 150.0, 250.0, -10.0, 5.0),
        ("npc_1", "npc", 50.0, 150.0, 0.0, 0.0),
    ]

    for eid, etype, x, y, vx, vy in entities:
        result = zone.spawn_entity(eid, etype, x, y, vx, vy)
        status = "✓" if result['success'] else "✗"
        print(f"  {status} Spawned {eid} ({etype}) at ({x}, {y})")

    # Send some intents (using fast MessagePack serialization)
    print("\n🎯 Sending intents with fast binary serialization...")

    intents = [
        ("player_1", "move", {"direction": "right", "speed": 50, "timestamp": int(time.time()*1000)}),
        ("enemy_1", "attack", {"target": "player_1", "damage": 25, "crit_chance": 0.15}),
        ("npc_1", "talk", {"message": "Welcome, adventurer!", "emotion": "friendly"}),
    ]

    for eid, intent_type, payload in intents:
        success = zone.send_intent(eid, intent_type, payload)
        status = "✓" if success else "✗"
        print(f"  {status} Sent {intent_type} intent for {eid} (binary serialized)")

    # Execute a tick
    print("\n⏰ Executing game tick...")
    tick_result = zone.step_tick(int(time.time() * 1000), 100, 10)
    print(f"✓ Tick result: {tick_result}")

    # Read events
    print("\n📡 Reading events...")
    events = zone.read_events("0", 20)
    for event in events:
        event_id, event_data = event
        eid = event_data.get('eid', 'unknown')
        event_type = event_data.get('type', 'unknown')
        print(f"  📝 Event {event_id[:8]}: {eid} {event_type}")

        if event_type == 'pos':
            x = event_data.get('x', 0)
            y = event_data.get('y', 0)
            print(f"      Position: ({x}, {y})")

    # Apply some damage
    print("\n💥 Applying damage...")
    damage_result = zone.apply_damage("player_1", 30)
    print(f"✓ Damage result: {damage_result}")

    # Schedule some jobs
    print("\n⏰ Scheduling jobs...")
    future_time = int(time.time() * 1000) + 5000  # 5 seconds from now

    jobs = [
        ("respawn_enemy", future_time, '{"enemy_id": "enemy_1", "location": [150, 250]}'),
        ("heal_player", future_time + 2000, '{"player_id": "player_1", "amount": 50}'),
    ]

    for job_id, run_at, payload in jobs:
        result = zone.schedule_job(job_id, run_at, payload)
        status = "✓" if result.get('success') else "✗"
        print(f"  {status} Scheduled {job_id} for {run_at}")

    # Check due jobs
    print("\n📋 Checking due jobs...")
    due_jobs = zone.get_due_jobs(int(time.time() * 1000) + 10000, 10)
    print(f"✓ Found {len(due_jobs)} due jobs")

    # Show engine stats
    stats = engine.get_stats()
    print(f"\n📊 Engine stats: {stats}")

    print("\n✅ Basic game setup demo completed!")

def demonstrate_zone_transfers():
    """Demonstrate cross-zone entity transfers."""
    print("\n🌐 Cross-Zone Entity Transfers")
    print("=" * 35)

    engine = GameEngine()
    engine.load_functions()

    world = engine.get_world("transfer_demo")

    # Create two zones
    zone_a = world.get_zone("zone_a")
    zone_b = world.get_zone("zone_b")

    # Spawn entity in zone A
    result = zone_a.spawn_entity("transfer_me", "player", 100.0, 200.0)
    print(f"✓ Spawned entity in zone A: {result}")

    # Initiate transfer
    transfer_result = zone_a.transfer_entity("transfer_me", "zone_b")
    print(f"✓ Transfer initiated: {transfer_result}")

    # Process cross-zone transfers
    xfer_result = world.process_cross_zone_transfers()
    print(f"✓ Processed {xfer_result['processed']} transfers")

    # Check if entity is now in zone B
    # (In a real implementation, you'd check zone B's entities)

    print("✅ Zone transfer demo completed!")

def demonstrate_tick_worker():
    """Demonstrate running a tick worker for a zone."""
    print("\n⚙️  Zone Tick Worker Demo")
    print("=" * 25)

    engine = GameEngine()
    engine.load_functions()

    world = engine.get_world("worker_demo")
    zone = world.get_zone("worker_zone")

    # Spawn a moving entity
    zone.spawn_entity("moving_entity", "player", 0.0, 0.0, 50.0, 30.0)

    # Send some movement intents
    for i in range(5):
        zone.send_intent("moving_entity", "move", f'{{"step": {i}}}')

    print("✓ Set up zone with moving entity and intents")
    print("✓ Starting tick worker for 10 seconds...")

    # Run worker in a separate thread for a limited time
    def run_worker_limited():
        start_time = time.time()
        while time.time() - start_time < 10:  # Run for 10 seconds
            result = engine.tick_zone("worker_demo", "worker_zone", 100, 5)
            if result.get('tick') != 'not_due':
                consumed = result.get('intents_consumed', 0)
                if consumed > 0:
                    print(f"  🎯 Tick {result.get('tick', 0)}: processed {consumed} intents")

            time.sleep(0.1)  # Don't spam

        print("✓ Worker completed 10-second run")

    worker_thread = threading.Thread(target=run_worker_limited)
    worker_thread.start()
    worker_thread.join()

    # Check final position
    events = zone.read_events("0", 50)
    final_pos = None
    for event in events:
        event_data = event[1]
        if event_data.get('type') == 'pos':
            final_pos = (event_data.get('x', 0), event_data.get('y', 0))

    if final_pos:
        print(f"✓ Entity moved to position: {final_pos}")

    print("✅ Tick worker demo completed!")

async def demonstrate_async_gameplay():
    """Demonstrate async gameplay with multiple zones."""
    print("\n🔄 Async Multi-Zone Gameplay")
    print("=" * 30)

    engine = GameEngine()
    engine.load_functions()

    # Create multiple zones
    world = engine.get_world("async_demo")
    zones = [world.get_zone(f"zone_{i}") for i in range(3)]

    # Spawn entities in each zone
    for i, zone in enumerate(zones):
        zone.spawn_entity(f"player_{i}", "player", i * 100.0, i * 50.0, 10.0, 5.0)

        # Send intents
        for j in range(3):
            zone.send_intent(f"player_{i}", "move", f'{{"zone": {i}, "step": {j}}}')

    print("✓ Set up 3 zones with entities and intents")

    # Run ticks asynchronously
    print("✓ Running async ticks...")

    tasks = []
    for i, zone in enumerate(zones):
        tasks.append(engine.tick_zone_async("async_demo", f"zone_{i}", 200, 3))

    results = await asyncio.gather(*tasks)

    total_consumed = sum(r.get('intents_consumed', 0) for r in results if r.get('tick') != 'not_due')
    print(f"✓ Async ticks completed, processed {total_consumed} total intents")

    print("✅ Async gameplay demo completed!")

def run_comprehensive_demo():
    """Run all game engine demonstrations."""
    print("🚀 CyRedis Game Engine - Comprehensive Demo")
    print("=" * 50)
    print("Demonstrating authoritative ECS on Redis with:")
    print("✅ Zone-based sharding with hash tags")
    print("✅ Atomic operations via Redis Functions")
    print("✅ Tick-based simulation")
    print("✅ Entity-Component-System architecture")
    print("✅ Cross-zone entity transfers")
    print("✅ Real-time event streaming")
    print("✅ Intent-based simulation communication")
    print()

    try:
        # Run synchronous demos
        demonstrate_basic_game_setup()
        demonstrate_zone_transfers()
        demonstrate_tick_worker()

        # Run async demo
        asyncio.run(demonstrate_async_gameplay())

        print("\n🎉 All Game Engine Demonstrations Completed!")
        print("\n🎯 Key Achievements:")
        print("• Authoritative server-side simulation in Redis")
        print("• Horizontal scaling across cluster nodes")
        print("• Atomic operations with one RTT")
        print("• Real-time event streaming")
        print("• Cross-zone entity transfers")
        print("• Deterministic tick-based execution")
        print("• Cluster-safe with hash tags")

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_comprehensive_demo()
