# CyRedis Game Engine

A **distributed game simulation engine** built on Redis/Valkey, implementing authoritative Entity-Component-System (ECS) with horizontal scaling across cluster nodes.

## 🏗️ Architecture Overview

The CyRedis Game Engine transforms Redis/Valkey from a database/cache into an **authoritative simulation engine** where the server decides truth. This enables deterministic, scalable game simulation with atomic operations and full cluster support.

### Core Principles

- **Authoritative Simulation**: Server-side ECS with atomic state changes
- **Horizontal Scaling**: Zone-based sharding across Redis/Valkey cluster nodes
- **Deterministic Execution**: Discrete tick-based simulation
- **One RTT Operations**: Complex game logic via Redis Functions
- **Real-time Communication**: Stream-based intent processing and event broadcasting
- **Cluster Native**: Designed for Redis/Valkey clusters with hash tag safety

### Cluster Compatibility

**✅ Redis Cluster**: Full support with hash-tagged keys for co-location
**✅ Valkey Cluster**: Compatible with Valkey (Redis fork)
**✅ Single Node**: Works on standalone Redis/Valkey instances
**✅ Redis Functions**: Requires Redis 7.0+ or Valkey with functions support

All operations are cluster-safe using hash tags `{world:zone}` ensuring zone data stays on the same cluster node.

## 🗂️ Key Components

### 1. World Model & Partitioning

```
World → Zones → Entities → Components
     ↓       ↓       ↓        ↓
   Global  Sharded  Game     Data
   State   Space   Objects   Storage
```

#### World Structure
- **Worlds**: Logical game universes (e.g., "my_mmo", "battle_royale")
- **Zones**: Sharded simulation spaces (hash-tagged for cluster safety)
- **Entities**: Game objects with components (players, NPCs, items)
- **Components**: Data attached to entities (position, health, inventory)

#### Redis Key Structure
```redis
# Zone data (hash-tagged for cluster co-location)
cy:ent:{world:zone}:entity_id      → Entity hash {type, x, y, hp, ...}
cy:comp:{world:zone}:component     → Component hashes
cy:spatial:{world:zone}            → ZSET for spatial queries
cy:type:{world:zone}:entity_type   → SET of entity IDs

# Zone simulation state
cy:tick:{world:zone}               → Hash {t, last_ms}
cy:intents:{world:zone}            → STREAM of entity intents
cy:events:{world:zone}             → STREAM of simulation events
cy:sched:{world:zone}              → ZSET of scheduled jobs

# Cross-zone communication
cy:xmsg:{world}                    → STREAM for zone transfers
cy:world:{world}:zones             → SET of active zones
```

### 2. Execution Model

#### Tick-Based Simulation
```python
# Each zone runs independent ticks
tick_result = zone.step_tick(now_ms, dt_ms=50, budget=256)
# Result: {'tick': 42, 'intents_consumed': 8}
```

#### Distributed Workers
```python
# Multiple workers can process different zones
run_zone_worker(engine, "my_world", "zone_0", tick_ms=50)
run_zone_worker(engine, "my_world", "zone_1", tick_ms=50)
```

#### Atomic Operations
All game logic executes as **Redis Functions** (Lua scripts) for atomicity:

- **Entity Operations**: Spawn, destroy, component updates
- **Simulation Systems**: Movement, combat, AI, physics
- **Intent Processing**: Client action validation and application
- **Event Generation**: State change notifications

### 3. Communication Flow

#### Client → Server (Intents) - Fast Binary Serialization
```python
# Client sends movement intent (MessagePack serialized for speed)
zone.send_intent("player_1", "move", {
    "dx": 10,
    "dy": 5,
    "timestamp": int(time.time()*1000)
})
```

#### Server Processing
```lua
-- Redis Function processes intent atomically
redis.register_function('tick.step', function(keys, args)
    -- 1. Read intents from cy:intents:{world:zone}
    -- 2. Validate and apply movement
    -- 3. Update spatial index
    -- 4. Emit position events to cy:events:{world:zone}
    -- 5. Advance tick counter
end)
```

#### Server → Client (Events)
```python
# Client reads simulation events
events = zone.read_events(last_id="0")
# [{'eid': 'player_1', 'type': 'pos', 'x': 150.5, 'y': 200.3}]
```

## 🎮 Game Engine Features

### Entity-Component-System (ECS)

#### Entity Management
```python
# Spawn entities with atomic operations
zone.spawn_entity("player_1", "player", x=100, y=200, vx=0, vy=0)

# Apply damage atomically
zone.apply_damage("player_1", 25)  # Returns death event if HP <= 0
```

#### Component Storage
```redis
# Entity core data
HGETALL cy:ent:{world:zone}:player_1
# {"type": "player", "x": "150.5", "y": "200.3", "hp": "75", "level": "5"}

# Spatial component (ZSET score = Morton key)
ZADD cy:spatial:{world:zone} 1505002003 player_1
```

### Spatial Systems

#### Collision Detection
```python
# Query entities in rectangular area
nearby = zone.query_spatial(x_min=100, x_max=200, y_min=150, y_max=250)
```

#### Area of Interest (AoI)
```python
# Maintain per-entity area of interest
zone.update_aoi("player_1", center_x=150, center_y=200, radius=50)
```

### Combat & Damage
```python
# Atomic damage application with death logic
result = zone.apply_damage("enemy_1", 50)
# Returns: {'success': True, 'status': 'hp:25'} or {'success': True, 'status': 'dead'}
```

### Job Scheduling
```python
# Schedule delayed operations (DoT, respawns, etc.)
zone.schedule_job("poison_tick", now_ms + 3000, '{"target": "player_1", "dmg": 5}')

# Process due jobs
due_jobs = zone.get_due_jobs(now_ms)
for job in due_jobs:
    # Apply poison damage, respawn entities, etc.
```

## 🚀 Scaling Architecture

### Zone-Based Sharding

#### Hash Tag Safety
Redis/Valkey keys use hash tags `{world:zone}` ensuring all zone data lives on the same cluster node:

```redis
# All these keys co-locate to the same Redis/Valkey node/slot
cy:ent:{my_world:zone_0}:player_1
cy:intents:{my_world:zone_0}
cy:events:{my_world:zone_0}
cy:spatial:{my_world:zone_0}
```

#### Worker Distribution
```python
# Workers can be assigned to specific zones
# Zone failure → Redis moves hash slot → workers reconnect
workers = [
    ("worker_1", ["zone_0", "zone_1"]),
    ("worker_2", ["zone_2", "zone_3"]),
    ("worker_3", ["zone_4", "zone_5"]),
]
```

### Cross-Zone Transfers

#### Entity Movement Between Zones
```python
# Transfer entity to different zone
zone_a.transfer_entity("player_1", "zone_b")

# Background process applies transfers
world.process_cross_zone_transfers()
```

#### Transfer Flow
1. **Serialize**: Entity state packed into transfer message
2. **Queue**: Message sent to `cy:xmsg:{world}` stream
3. **Apply**: Target zone creates entity with transferred state
4. **Cleanup**: Source zone deletes entity

## ⚡ Performance Characteristics

### One RTT Operations
- **Entity spawning**: Single Redis Function call
- **Intent processing**: Atomic batch processing per tick
- **Damage application**: Lock-free atomic updates
- **Spatial queries**: O(log n) ZSET range queries
- **Serialization**: MessagePack for 5-10x faster than JSON

### Memory Efficiency
- **Minimal state**: Only active entity data in Redis
- **Component storage**: Efficient hash packing
- **Spatial indexing**: Compact Morton/Peano keys
- **Stream trimming**: Automatic cleanup of processed events

### Scaling Limits
- **Entities per zone**: Thousands (limited by tick budget)
- **Zones per world**: Hundreds (cluster slot distribution)
- **Tick rate**: 20-100Hz depending on complexity
- **Concurrent players**: Thousands per zone, millions total

## 🛠️ Redis Functions Library

The engine ships with a complete library of atomic game operations:

### Core Functions
- **`tick.fetch_due`**: Check if zone tick is ready
- **`tick.step`**: Process intents, run systems, emit events
- **`ent.spawn`**: Create entity with spatial registration
- **`ent.apply_damage`**: Atomic HP changes with death logic

### Advanced Functions
- **`sched.enqueue`**: Schedule delayed jobs
- **`sched.due`**: Retrieve and remove expired jobs
- **`xfer.request`**: Initiate cross-zone transfer
- **`xfer.apply`**: Apply incoming entity transfer

### Custom Systems
Add your own Redis Functions for game-specific logic:
- **Combat systems**: Critical hits, status effects
- **AI behaviors**: Pathfinding, decision trees
- **Physics**: Collision resolution, projectile simulation
- **Economy**: Item drops, currency transactions

## 🔧 Usage Examples

### Basic Game Loop
```python
from cyredis_game import GameEngine

# Initialize engine
engine = GameEngine()
engine.load_functions()

# Create world and zones
world = engine.get_world("my_game")
zone = world.get_zone("zone_0")

# Game loop
while True:
    now = int(time.time() * 1000)

    # Execute tick if due
    if zone.is_tick_due(now, 50):  # 50ms ticks
        result = zone.step_tick(now, 50, 100)  # 100 intent budget
        print(f"Tick {result['tick']}: {result['intents_consumed']} intents")

    time.sleep(0.01)  # Prevent busy waiting
```

### Multi-Zone Server
```python
# Run multiple zone workers
import threading

def zone_worker(world_id, zone_id):
    engine = GameEngine()
    engine.load_functions()
    world = engine.get_world(world_id)
    zone = world.get_zone(zone_id)

    run_zone_worker(engine, world_id, zone_id, tick_ms=50)

# Start workers for different zones
threads = []
for zone_id in ["zone_0", "zone_1", "zone_2"]:
    t = threading.Thread(target=zone_worker, args=("my_game", zone_id))
    t.start()
    threads.append(t)
```

### Client Integration - Fast Binary Serialization
```python
# Client sends intents (MessagePack automatically used)
def send_movement(entity_id, dx, dy):
    zone.send_intent(entity_id, "move", {
        "dx": dx,
        "dy": dy,
        "speed": 50.0,
        "timestamp": int(time.time()*1000)
    })

# Client reads events (MessagePack automatically deserialized)
def process_events(last_id="0"):
    events = zone.read_events(last_id)
    for event in events:
        event_id, event_data = event
        if event_data['type'] == 'pos':
            # Position data is automatically deserialized dict
            pos_data = event_data.get('payload', {})
            update_entity_position(event_data['eid'],
                                 pos_data.get('x', 0), pos_data.get('y', 0))
        elif event_data['type'] == 'death':
            # Handle entity death
            remove_entity(event_data['eid'])
```

## 🎯 Design Philosophy

### Why Redis for Game Servers?

1. **Atomic Operations**: Redis Functions provide ACID transactions for game state
2. **Horizontal Scaling**: Cluster enables massive worlds across nodes
3. **Performance**: Sub-millisecond operations for real-time games
4. **Durability**: Persistence ensures game state survives crashes
5. **Pub/Sub & Streams**: Built-in real-time communication primitives

### ECS on Redis Benefits

1. **Authoritative**: Server-side simulation prevents cheating
2. **Consistent**: Atomic operations ensure game state integrity
3. **Scalable**: Independent zones enable massive worlds
4. **Flexible**: Component-based design supports any game type
5. **Real-time**: Stream-based communication for responsive gameplay

### Trade-offs

- **Network Latency**: All operations require Redis round-trips
- **Memory Usage**: Entity state stored in Redis (not local)
- **Complexity**: Distributed systems require careful design
- **Cost**: Redis Cluster infrastructure investment

## 🚀 Getting Started

1. **Install CyRedis Game Engine**
   ```bash
   pip install cyredis-game  # Future package
   # Or build from source
   python setup.py build_ext --inplace
   ```

2. **Start Redis/Valkey Cluster**
   ```bash
   # Redis 7+ or Valkey with Functions support required
   redis-server --cluster-enabled yes
   # Or for Valkey:
   valkey-server --cluster-enabled yes
   ```

3. **Initialize Game Engine**
   ```python
   from cyredis_game import GameEngine

   engine = GameEngine()
   engine.load_functions()  # Load Redis Functions
   ```

4. **Create Your First Zone**
   ```python
   world = engine.get_world("tutorial")
   zone = world.get_zone("zone_0")

   # Spawn a player
   zone.spawn_entity("hero", "player", x=0, y=0)
   ```

5. **Run Simulation**
   ```python
   # Start zone worker
   run_zone_worker(engine, "tutorial", "zone_0", tick_ms=50)
   ```

---

**CyRedis Game Engine**: Where Redis becomes your game server! 🎮⚡

This architecture enables **scalable game simulation** with **deterministic execution**, **horizontal scaling**, and **authoritative state management** - all built on Redis as the core simulation engine.
