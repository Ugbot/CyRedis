# Experimental subsystems

Nothing in this directory is part of the `cy-redis` distribution on PyPI. It
is excluded from the wheel and the sdist (`pyproject.toml` package discovery
and `MANIFEST.in` both prune it), and `scripts/check_dist_contents.py` fails
the packaging job if any of it leaks in.

These subsystems are kept in the repository so they can be fixed in the open,
but the review that preceded the first release found each of them unsafe or
incomplete. Do not deploy them.

| Package | What it is | Known blockers |
| --- | --- | --- |
| `cyredis_experimental.auth` | Sessions, tokens, TOTP, password reset | No password verification; JWT secret defaults to a random per-process value; refresh tokens accepted as access tokens |
| `cyredis_experimental.web` | WebSocket channels, web cache, FastAPI glue, `WebAppSupport`, `SharedStateManager` | WebSocket auth fails open; depends on the auth package above |
| `cyredis_experimental.workers` | Worker lifecycle, coordination, queues, session tracking | `WorkerQueue` uses one key as both a list and a hash |
| `cyredis_experimental.communication` | Reliable queue and RPC | No reclamation of messages from dead consumers |
| `cyredis_experimental.extras` | Advanced wrapper, probabilistic C++ structures, AI/vector layer, ClickHouse | Untested duplicates of supported functionality |
| `cyredis_experimental.game` | ECS game engine and the `cy_game` Redis module | Intent protocol differs between Python, C and Lua |
| `pgcache/` | PostgreSQL read-through cache Redis module | Synchronous libpq calls on Redis's main thread |

## Building

The Cython extensions here `cimport` from `cy_redis` and link the vendored
hiredis, so build the supported package first:

```bash
uv pip install -e .                                   # repository root
uv pip install --no-build-isolation -e ./experimental  # or ./experimental[game]
```

The Redis server modules build with `make module` (cy_game) and
`make -C experimental/pgcache/src` (pgcache); pgcache needs libpq and jansson
headers.

## Tests

```bash
uv run pytest experimental/tests
```

Run from the repository root so the shared `tests.server_env` settings
(`REDIS_HOST`, `REDIS_PORT`, `CY_GAME_REDIS_PORT`, `PGCACHE_REDIS_PORT`)
apply. `.github/workflows/modules.yml` runs the module-backed subset in CI.
