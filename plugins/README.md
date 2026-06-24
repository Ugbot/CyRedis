# CyRedis Plugins

This directory contains optional plugins and extensions that extend CyRedis functionality. Plugins are designed to be modular and can be installed independently.

## 🏗️ Plugin Architecture

### Plugin Structure
Each plugin follows a consistent structure:
```
plugins/
├── plugin_name/
│   ├── __init__.py          # Python package initialization
│   ├── README.md            # Plugin documentation
│   ├── source_files...      # Plugin implementation
│   └── build scripts...     # Build/installation scripts
```

### Plugin Types
- **Redis Modules**: Native Redis extensions (C/C++)
- **Python Extensions**: Pure Python plugins
- **Hybrid**: Combined C extensions with Python wrappers

## 📦 Available Plugins

### 🐘 PostgreSQL Cache (`pgcache/`)
**Redis module for read-through caching from PostgreSQL**

- **Purpose**: Automatic caching layer between Redis and PostgreSQL
- **Technology**: Redis module written in C
- **Features**:
  - Transparent read-through caching
  - Configurable TTL and invalidation
  - Connection pooling for PostgreSQL
  - SQL query result caching

**Installation:**
```bash
cd plugins/pgcache
git submodule update --init --recursive   # vendored psqlodbc
./build_module.sh                          # produces pgcache.so
redis-server --loadmodule ./pgcache.so pg_host localhost pg_database myapp \
    pg_user postgres pg_password secret
```

**Usage:** the module adds server-side commands (`PGCACHE.READ`, `PGCACHE.WRITE`,
`PGCACHE.INVALIDATE`, `PGCACHE.MULTIREAD`). Call them through the CyRedis client's
raw command interface (`execute_command` takes a single list of arguments):

```python
from cy_redis import CyRedisClient

client = CyRedisClient(host="localhost", port=6379)

# Read-through: cache miss queries PostgreSQL, hit serves from Redis
user = client.execute_command(["PGCACHE.READ", "users", '{"id": 123}'])

# Manual write / invalidate
client.execute_command(["PGCACHE.WRITE", "users", '{"id": 123}', '{"name": "John"}'])
client.execute_command(["PGCACHE.INVALIDATE", "users", '{"id": 123}'])
```

There is no Python `PGCacheManager` class — the module is pure C and is driven
entirely through these Redis commands.

## 🚀 Developing New Plugins

### Plugin Template
```bash
# Create new plugin directory
mkdir plugins/your_plugin
cd plugins/your_plugin

# Create basic structure
touch __init__.py
touch README.md
# Add your plugin files...
```

### Plugin Guidelines
1. **Modular**: Plugins should be self-contained
2. **Documented**: Include comprehensive README
3. **Tested**: Include tests in `tests/` directory
4. **Versioned**: Use semantic versioning
5. **Compatible**: Work with both Redis and Valkey

### Plugin Categories
- **Database Integrations**: PostgreSQL, MySQL, MongoDB, etc.
- **Protocol Extensions**: Custom protocols, message formats
- **Performance Tools**: Monitoring, profiling, optimization
- **Domain Specific**: Game features, IoT, analytics, etc.

## 🔧 Plugin Management

### Installation
```bash
# Declare the pgcache intent extra (no extra Python deps; the module is C)
uv pip install -e ".[pgcache]"

# Or pull in every optional layer
uv pip install -e ".[all]"
```

### Loading and checking a module
```python
from cy_redis import CyRedisClient

client = CyRedisClient()

# The pgcache module is loaded into the Redis server (via --loadmodule or
# MODULE LOAD), not into the Python process. Check what is loaded:
modules = client.execute_command(["MODULE", "LIST"])
```

## 🤝 Contributing Plugins

### Submission Process
1. Create plugin in `plugins/your_plugin/`
2. Add comprehensive tests in `tests/test_your_plugin.py`
3. Update this README with plugin information
4. Submit pull request with documentation

### Plugin Requirements
- **License**: Compatible with CyRedis (MIT)
- **Dependencies**: Clearly documented
- **Testing**: >80% code coverage
- **Documentation**: Usage examples and API docs
- **Maintenance**: Actively maintained

## 📊 Plugin Metrics

| Plugin | Type | Status | Downloads |
|--------|------|--------|-----------|
| pgcache | Redis Module | Stable | - |
| (future plugins) | - | - | - |

## 🔍 Troubleshooting

### Common Issues
- **Module not found**: Check Redis module path
- **Build failures**: Verify build dependencies
- **Import errors**: Ensure plugin is installed
- **Version conflicts**: Check compatibility matrix

### Getting Help
- Check individual plugin READMEs
- File issues on GitHub
- Check community discussions

---

**CyRedis Plugins: Extend functionality without complexity!** 🔌✨
