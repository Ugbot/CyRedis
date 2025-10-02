# CyRedis Examples

This directory contains comprehensive examples demonstrating CyRedis features, usage patterns, and best practices.

## 📚 Available Examples

### Core Functionality
- **`example_usage.py`** - Basic Redis operations, async/sync usage, threading
- **`example_protocol_support.py`** - RESP2/3 protocol features, pipelining, connection pooling
- **`example_production_redis.py`** - Production-ready configurations and patterns

### Advanced Features
- **`example_redis_functions.py`** - Redis Functions library usage, atomic operations
- **`example_lua_scripts.py`** - Lua script management, custom scripting
- **`example_atomic_script_deployment.py`** - Script deployment and testing patterns
- **`example_shared_dict.py`** - Distributed shared dictionaries with concurrency control

### Migration & Integration
- **`migration_example.py`** - Migration patterns from other Redis clients

## 🚀 Running Examples

Each example is self-contained and can be run independently:

```bash
# Basic usage example
python examples/example_usage.py

# Redis Functions example
python examples/example_redis_functions.py

# Production patterns
python examples/example_production_redis.py
```

## 📖 Example Categories

### 🔰 Beginner
- `example_usage.py` - Start here for basic operations
- `example_protocol_support.py` - Understand protocol features

### 🔧 Intermediate
- `example_lua_scripts.py` - Custom scripting
- `example_shared_dict.py` - Distributed data structures

### 🏭 Advanced
- `example_redis_functions.py` - Atomic operations at scale
- `example_atomic_script_deployment.py` - Production deployment
- `example_production_redis.py` - Enterprise patterns

## 🏗️ Architecture Patterns

### Basic Client Usage
```python
from cy_redis import RedisClient

# Synchronous
client = RedisClient()
result = client.get("key")

# Asynchronous
async with client:
    result = await client.get_async("key")
```

### Redis Functions
```python
from cy_redis.functions import RedisFunctionsManager

functions = RedisFunctionsManager(client)
functions.load_library("cy:atomic")
result = functions.atomic.cas("key", "old", "new")
```

### Game Engine
```python
from cyredis_game import GameEngine

engine = GameEngine()
world = engine.get_world("my_game")
zone = world.get_zone("zone_0")
```

## 🎯 Learning Path

1. **Start**: `example_usage.py` - Basic operations
2. **Protocol**: `example_protocol_support.py` - Advanced features
3. **Production**: `example_production_redis.py` - Real-world usage
4. **Functions**: `example_redis_functions.py` - Atomic operations
5. **Game Engine**: `cyredis_game/example_game_engine.py` - Distributed simulation

## 🔍 Troubleshooting

### Common Issues
- **Connection errors**: Check Redis server is running
- **Import errors**: Ensure CyRedis is properly installed
- **Async issues**: Use `asyncio.run()` for async examples

### Dependencies
```bash
pip install cy-redis msgpack hiredis
# For game engine examples:
pip install cyredis-game
```

## 🤝 Contributing

When adding new examples:
1. Follow naming convention: `example_<feature>.py`
2. Include comprehensive docstrings
3. Add error handling and logging
4. Update this README with the new example

## 📞 Support

- Check individual example docstrings for usage details
- See main README for installation and setup
- File issues for bugs or feature requests
