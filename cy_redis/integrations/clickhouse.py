"""
CyRedis ClickHouse integration.

Bridges ClickHouse query results into Redis using three patterns:

1. **Live cache** — generates ClickHouse Redis-engine table + MaterializedView DDL
   so ClickHouse pushes row-level changes into Redis automatically. No Python
   polling loop required after setup.

2. **Stream dump** — one-shot: runs a ClickHouse query, streams JSONEachRow rows
   into a Redis Stream via XADD. Good for backfilling history.

3. **Watch/incremental** — async generator that polls ClickHouse for new rows
   above a watermark column and XADDs each batch to a Redis Stream. Good for
   tables that grow but are not backed by a ReplicatedMergeTree that can feed an MV.

4. **Channel broadcast** — consumes a Redis Stream and publishes each entry to a
   CyChannelManager channel so WebSocket subscribers see live events.

All Redis I/O goes through the CyRedisClient passed at construction time.
Zero new Python dependencies — urllib only.

ClickHouse Redis engine caveat
-------------------------------
The Redis table engine (available since ClickHouse 23.6) stores the PRIMARY KEY
column as the Redis key and all remaining columns as a single msgpack binary value.
Only point-lookups by primary key are efficient — range scans or full-table reads
hit every Redis key. Keep the materialized projection narrow.

Refreshable MV caveat
----------------------
``refresh_interval_sec > 0`` uses ``CREATE MATERIALIZED VIEW ... REFRESH EVERY N SECOND``
which requires ClickHouse 23.4+. For older versions use ``refresh_interval_sec=0``
(incremental MV) which works on 22.x+.
"""

import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Optional


class CyClickHouseClient:
    """
    Async ClickHouse HTTP client.

    Uses the ClickHouse HTTP interface at ``http://host:port/``.
    All blocking I/O is offloaded to the default executor so this integrates
    cleanly with asyncio without adding any third-party dependencies.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        user: str = "default",
        password: str = "",
        database: str = "default",
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self._base_url = f"http://{host}:{port}/"

    def _build_params(self, fmt: Optional[str] = None) -> Dict[str, str]:
        params: Dict[str, str] = {
            "user": self.user,
            "database": self.database,
        }
        if self.password:
            params["password"] = self.password
        if fmt:
            params["default_format"] = fmt
        return params

    def _post_sync(self, sql: str, fmt: Optional[str] = None) -> bytes:
        params = self._build_params(fmt)
        url = self._base_url + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(
            url,
            data=sql.encode(),
            method="POST",
            headers={"Content-Type": "text/plain; charset=utf-8"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")
            raise RuntimeError(f"ClickHouse error {exc.code}: {body}") from exc

    def _post_stream_sync(self, sql: str, fmt: str) -> List[bytes]:
        """Returns list of raw lines (caller decodes JSON)."""
        params = self._build_params(fmt)
        url = self._base_url + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(
            url,
            data=sql.encode(),
            method="POST",
            headers={"Content-Type": "text/plain; charset=utf-8"},
        )
        lines: List[bytes] = []
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                for raw in resp:
                    raw = raw.rstrip(b"\n")
                    if raw:
                        lines.append(raw)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")
            raise RuntimeError(f"ClickHouse error {exc.code}: {body}") from exc
        return lines

    async def execute(self, sql: str) -> None:
        """Run a DDL or DML statement (no result set expected)."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._post_sync, sql, None)

    async def query(self, sql: str, fmt: str = "JSONEachRow") -> List[Dict[str, Any]]:
        """Run a SELECT and return all rows as a list of dicts."""
        loop = asyncio.get_event_loop()
        raw_lines = await loop.run_in_executor(None, self._post_stream_sync, sql, fmt)
        rows = []
        for line in raw_lines:
            if line:
                rows.append(json.loads(line))
        return rows

    async def query_stream(
        self, sql: str, fmt: str = "JSONEachRow"
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Async generator — yields one dict per row.

        Buffers the full response in a thread (urllib limitation) then yields
        lazily so callers can process row-by-row without holding a large list.
        """
        loop = asyncio.get_event_loop()
        raw_lines = await loop.run_in_executor(None, self._post_stream_sync, sql, fmt)
        for line in raw_lines:
            if line:
                yield json.loads(line)

    async def ping(self) -> bool:
        """Return True if ClickHouse responds to /ping."""
        loop = asyncio.get_event_loop()

        def _ping() -> bool:
            url = f"http://{self.host}:{self.port}/ping"
            try:
                with urllib.request.urlopen(url, timeout=5) as resp:
                    return resp.read().strip() == b"Ok."
            except Exception:
                return False

        return await loop.run_in_executor(None, _ping)

    async def server_version(self) -> str:
        """Return the ClickHouse server version string."""
        rows = await self.query("SELECT version() AS v")
        if rows:
            return rows[0].get("v", "unknown")
        return "unknown"


class CyClickHouseBridge:
    """
    Orchestrates ClickHouse → Redis data pipelines.

    Parameters
    ----------
    ch_client:
        A ``CyClickHouseClient`` connected to the source ClickHouse instance.
    redis_client:
        A ``CyRedisClient`` (or compatible duck-type).  All Redis I/O uses
        the async ``*_async`` methods.
    """

    def __init__(self, ch_client: CyClickHouseClient, redis_client: Any):
        self._ch = ch_client
        self._redis = redis_client

    # ------------------------------------------------------------------
    # Mode 1 — Live cache via ClickHouse Redis engine + MaterializedView
    # ------------------------------------------------------------------

    async def create_live_cache(
        self,
        name: str,
        source_query: str,
        key_column: str,
        redis_host: str,
        redis_port: int = 6379,
        redis_key_prefix: str = "",
        refresh_interval_sec: int = 0,
    ) -> Dict[str, Any]:
        """
        Auto-DDL a Redis-engine cache table and a MaterializedView that keeps
        it up-to-date whenever the source table receives new data.

        The Redis key for each row is ``{redis_key_prefix}{key_column_value}``.
        Non-key columns are stored as a msgpack blob — only point-lookups by
        primary key are efficient on the Redis side.

        Parameters
        ----------
        name:
            Prefix used for the generated table and view names.
        source_query:
            A SELECT query that defines the shape of the cached data.  This
            becomes the body of the MaterializedView.
        key_column:
            Which column in ``source_query`` becomes the Redis key.
        redis_host / redis_port:
            Connection details for the Redis instance ClickHouse will write to.
        redis_key_prefix:
            Optional string prepended to every Redis key.
        refresh_interval_sec:
            0 = incremental MV (fires per INSERT batch, ClickHouse 22.x+)
            >0 = REFRESH EVERY N SECOND (atomic full-replace, ClickHouse 23.4+)

        Returns
        -------
        dict with keys ``table``, ``view``, ``ddl`` (list of executed DDL strings).
        """
        version_str = await self._ch.server_version()
        version_major = self._parse_ch_major(version_str)

        if refresh_interval_sec > 0 and version_major < 23:
            raise RuntimeError(
                f"REFRESH EVERY requires ClickHouse 23.4+; server is {version_str}"
            )

        table_name = f"cy_cache_{name}"
        view_name = f"cy_cache_{name}_mv"
        redis_addr = f"{redis_host}:{redis_port}"

        # Derive column list from source_query by running it with LIMIT 0
        probe_rows = await self._ch.query(f"{source_query} LIMIT 0")
        # With LIMIT 0 the result is empty but we need column names from schema
        # Use DESCRIBE on a subquery instead
        schema_rows = await self._ch.query(
            f"DESCRIBE TABLE (SELECT * FROM ({source_query}) LIMIT 0)"
        )
        if not schema_rows:
            # Fallback: run with LIMIT 1 to get column names from actual data
            sample = await self._ch.query(f"{source_query} LIMIT 1")
            if not sample:
                raise RuntimeError("source_query returned no rows — cannot infer schema")
            col_names = list(sample[0].keys())
        else:
            col_names = [r["name"] for r in schema_rows]

        if key_column not in col_names:
            raise ValueError(
                f"key_column '{key_column}' not found in source_query columns: {col_names}"
            )

        non_key_cols = [c for c in col_names if c != key_column]

        # Build Redis engine CREATE TABLE
        # ClickHouse Redis engine signature:
        #   ENGINE = Redis('host:port', db_index, 'password', 'storage_type')
        # storage_type: 'simple' (default) = single string value per key
        col_defs = f"{key_column} String"
        if non_key_cols:
            col_defs += ", " + ", ".join(f"{c} String" for c in non_key_cols)

        table_ddl = (
            f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs})\n"
            f"ENGINE = Redis('{redis_addr}', 0)\n"
            f"PRIMARY KEY {key_column}"
        )
        if redis_key_prefix:
            table_ddl += f"\nSETTINGS redis_key_prefix = '{redis_key_prefix}'"

        # Build MaterializedView DDL
        if refresh_interval_sec == 0:
            # Incremental MV: fires per INSERT batch
            randomize_sec = max(1, refresh_interval_sec // 2)
            view_ddl = (
                f"CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}\n"
                f"TO {table_name}\n"
                f"AS {source_query}"
            )
        else:
            randomize_sec = max(1, refresh_interval_sec // 2)
            view_ddl = (
                f"CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}\n"
                f"REFRESH EVERY {refresh_interval_sec} SECOND "
                f"RANDOMIZE FOR {randomize_sec} SECOND\n"
                f"TO {table_name}\n"
                f"AS {source_query}"
            )

        await self._ch.execute(table_ddl)
        await self._ch.execute(view_ddl)

        return {
            "table": table_name,
            "view": view_name,
            "ddl": [table_ddl, view_ddl],
        }

    # ------------------------------------------------------------------
    # Mode 2 — One-shot stream dump
    # ------------------------------------------------------------------

    async def dump_to_stream(
        self,
        query: str,
        stream_key: str,
        batch_size: int = 500,
        maxlen: int = 100_000,
    ) -> int:
        """
        Run *query* against ClickHouse and XADD every row to *stream_key*.

        All non-string column values are cast to strings before XADD since
        Redis Stream field values must be strings.

        Returns the total number of rows written.
        """
        total = 0
        batch: List[Dict[str, str]] = []

        async for row in self._ch.query_stream(query):
            str_row = {k: str(v) for k, v in row.items()}
            batch.append(str_row)
            if len(batch) >= batch_size:
                for r in batch:
                    await self._xadd_maxlen(stream_key, r, maxlen)
                total += len(batch)
                batch = []

        for r in batch:
            await self._xadd_maxlen(stream_key, r, maxlen)
        total += len(batch)

        return total

    # ------------------------------------------------------------------
    # Mode 3 — Incremental watch → Redis Stream
    # ------------------------------------------------------------------

    async def watch_as_stream(
        self,
        query: str,
        watermark_column: str,
        stream_key: str,
        poll_interval_sec: float = 5.0,
        maxlen: int = 100_000,
        initial_watermark: str = "1970-01-01 00:00:00",
    ) -> AsyncIterator[int]:
        """
        Async generator — polls ClickHouse for new rows and XADDs them to
        *stream_key*.  Yields the count of rows added in each poll cycle.

        *query* must contain a ``{watermark}`` placeholder that will be
        substituted with the current high-water mark on each iteration.
        The watermark is updated to the maximum value of *watermark_column*
        seen so far, so the query must order or filter by that column.

        Example query::

            SELECT symbol, price, volume, ts
            FROM trades
            WHERE ts > '{watermark}'
            ORDER BY ts
            LIMIT 10000

        The generator runs until the calling coroutine cancels it (``task.cancel()``
        or the enclosing ``async with`` / ``async for`` is exited).
        """
        watermark = initial_watermark

        while True:
            sql = query.format(watermark=watermark)
            rows = await self._ch.query(sql)
            count = 0

            for row in rows:
                str_row = {k: str(v) for k, v in row.items()}
                await self._xadd_maxlen(stream_key, str_row, maxlen)
                count += 1
                # Advance watermark
                wm_val = str(row.get(watermark_column, watermark))
                if wm_val > watermark:
                    watermark = wm_val

            yield count
            await asyncio.sleep(poll_interval_sec)

    # ------------------------------------------------------------------
    # Mode 4 — Broadcast a Redis Stream to a CyChannelManager channel
    # ------------------------------------------------------------------

    async def stream_to_channel(
        self,
        stream_key: str,
        channel: str,
        channel_manager: Any,
        batch_size: int = 100,
        poll_interval_sec: float = 1.0,
    ) -> None:
        """
        Read new entries from *stream_key* and publish each one to *channel*
        via *channel_manager* (a ``CyChannelManager`` instance).

        The last-consumed stream entry ID is persisted in Redis under
        ``cy:ch_bridge:{stream_key}:cursor`` so restarts do not replay
        already-delivered entries.

        Runs until cancelled.
        """
        cursor_key = f"cy:ch_bridge:{stream_key}:cursor"

        # Resume from persisted cursor or start from current tail
        saved = await self._redis.get_async(cursor_key)
        cursor = saved if saved else "$"

        while True:
            entries = await self._redis.xread_async(
                {stream_key: cursor}, count=batch_size, block=0
            )
            for _stream_name, entry_id, fields in entries:
                # fields are already str→str from xread_async
                payload = dict(fields)
                await channel_manager.publish(channel, payload)
                cursor = entry_id

            if entries:
                # Persist cursor — fire-and-forget (best-effort durability)
                loop = asyncio.get_event_loop()
                loop.create_task(
                    self._redis.set_async(cursor_key, cursor)
                )

            await asyncio.sleep(poll_interval_sec)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _xadd_maxlen(
        self, stream_key: str, data: Dict[str, str], maxlen: int
    ) -> str:
        """
        XADD with approximate MAXLEN trimming.

        CyRedisClient.xadd() does not expose MAXLEN so we build the raw
        command via the pool's execute_command directly.
        """
        # xadd_async doesn't support MAXLEN — fall back to sync xadd wrapped
        # in executor with the raw args built here via execute_command on pool.
        loop = asyncio.get_event_loop()

        def _xadd_sync():
            args = ["XADD", stream_key, "MAXLEN", "~", str(maxlen), "*"]
            for k, v in data.items():
                args.extend([k, v])
            conn = self._redis.pool.get_connection()
            try:
                return conn.execute_command(args)
            finally:
                self._redis.pool.return_connection(conn)

        return await loop.run_in_executor(None, _xadd_sync)

    @staticmethod
    def _parse_ch_major(version_str: str) -> int:
        """Extract major version number from ClickHouse version string."""
        try:
            return int(version_str.split(".")[0])
        except (ValueError, IndexError):
            return 0
