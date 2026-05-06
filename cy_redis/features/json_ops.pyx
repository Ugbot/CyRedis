# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True

"""
High-performance JSON operations for Redis/Valkey
Supports RedisJSON module commands with optimized parsing
"""

import json
import asyncio
from typing import Any, Dict, List, Optional, Union
from concurrent.futures import ThreadPoolExecutor

from cy_redis.core.cy_redis_client cimport CyRedisConnection, CyRedisConnectionPool


cdef class CyRedisJSON:
    """
    High-performance JSON operations for Redis/Valkey

    Supports RedisJSON module commands:
    - JSON.SET, JSON.GET, JSON.DEL
    - JSON.MGET, JSON.NUMINCRBY, JSON.NUMMULTBY
    - JSON.STRAPPEND, JSON.STRLEN, JSON.ARRAPPEND
    - JSON.ARRINDEX, JSON.ARRINSERT, JSON.ARRLEN
    - JSON.ARRPOP, JSON.ARRTRIM, JSON.OBJKEYS, JSON.OBJLEN
    - JSON.TYPE, JSON.RESP, JSON.DEBUG
    """

    cdef CyRedisConnectionPool pool
    cdef object _executor

    def __init__(self, str host='localhost', int port=6379, int max_connections=10,
                 int timeout=5, str password=None, int db=0):
        """Initialize JSON operations with connection pool"""
        self.pool = CyRedisConnectionPool(
            host=host,
            port=port,
            max_connections=max_connections,
            timeout=timeout,
            password=password,
            db=db
        )
        self._executor = ThreadPoolExecutor(max_workers=max_connections)

    @property
    def executor(self):
        """Get thread pool executor"""
        return self._executor

    def __dealloc__(self):
        """Cleanup resources"""
        if self._executor:
            self._executor.shutdown(wait=False)

    # ===== BASIC JSON OPERATIONS =====

    def json_set(self, key: str, path: str, value: Any, nx: bool = False,
                 xx: bool = False) -> Optional[str]:
        """Set JSON value at path"""
        conn = self.pool.get_connection()
        if conn is None:
            raise ConnectionError("No available connections")

        try:
            args = ['JSON.SET', key, path, json.dumps(value)]
            if nx:
                args.append('NX')
            elif xx:
                args.append('XX')

            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def json_get(self, key: str, *paths, indent: str = None, newline: str = None,
                 space: str = None, noescape: bool = False) -> Optional[Any]:
        """Get JSON value at path(s)"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['JSON.GET', key]

            if indent:
                args.extend(['INDENT', indent])
            if newline:
                args.extend(['NEWLINE', newline])
            if space:
                args.extend(['SPACE', space])
            if noescape:
                args.append('NOESCAPE')

            if paths:
                args.extend(paths)
            else:
                args.append('$')

            result = conn.execute_command(args)
            if result:
                try:
                    return json.loads(result)
                except (json.JSONDecodeError, TypeError):
                    return result
            return None
        finally:
            self.pool.return_connection(conn)

    def json_mget(self, keys: List[str], path: str = '$') -> List[Optional[Any]]:
        """Get JSON values from multiple keys"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['JSON.MGET']
            args.extend(keys)
            args.append(path)

            result = conn.execute_command(args)
            if result:
                return [json.loads(r) if r else None for r in result]
            return []
        finally:
            self.pool.return_connection(conn)

    def json_del(self, key: str, path: str = '$') -> int:
        """Delete JSON value at path"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['JSON.DEL', key, path])
        finally:
            self.pool.return_connection(conn)

    def json_type(self, key: str, path: str = '$') -> Optional[str]:
        """Get JSON value type at path"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            result = conn.execute_command(['JSON.TYPE', key, path])
            if isinstance(result, list) and result:
                return result[0]
            return result
        finally:
            self.pool.return_connection(conn)

    # ===== NUMERIC OPERATIONS =====

    def json_numincrby(self, key: str, path: str, value: Union[int, float]) -> Optional[float]:
        """Increment numeric value at path"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            result = conn.execute_command(['JSON.NUMINCRBY', key, path, str(value)])
            if result:
                try:
                    return json.loads(result)
                except (json.JSONDecodeError, TypeError):
                    return float(result)
            return None
        finally:
            self.pool.return_connection(conn)

    def json_nummultby(self, key: str, path: str, value: Union[int, float]) -> Optional[float]:
        """Multiply numeric value at path"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            result = conn.execute_command(['JSON.NUMMULTBY', key, path, str(value)])
            if result:
                try:
                    return json.loads(result)
                except (json.JSONDecodeError, TypeError):
                    return float(result)
            return None
        finally:
            self.pool.return_connection(conn)

    # ===== STRING OPERATIONS =====

    def json_strappend(self, key: str, path: str, value: str) -> Optional[int]:
        """Append string to JSON string at path"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            result = conn.execute_command(['JSON.STRAPPEND', key, path, json.dumps(value)])
            if isinstance(result, list) and result:
                return result[0]
            return result
        finally:
            self.pool.return_connection(conn)

    def json_strlen(self, key: str, path: str = '$') -> Optional[int]:
        """Get string length at path"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            result = conn.execute_command(['JSON.STRLEN', key, path])
            if isinstance(result, list) and result:
                return result[0]
            return result
        finally:
            self.pool.return_connection(conn)

    # ===== ARRAY OPERATIONS =====

    def json_arrappend(self, key: str, path: str, *values) -> Optional[int]:
        """Append values to JSON array at path"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['JSON.ARRAPPEND', key, path]
            args.extend([json.dumps(v) for v in values])
            result = conn.execute_command(args)
            if isinstance(result, list) and result:
                return result[0]
            return result
        finally:
            self.pool.return_connection(conn)

    def json_arrindex(self, key: str, path: str, value: Any, start: int = 0,
                      stop: int = None) -> Optional[int]:
        """Find index of value in JSON array"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['JSON.ARRINDEX', key, path, json.dumps(value)]
            if start != 0:
                args.append(str(start))
                if stop is not None:
                    args.append(str(stop))

            result = conn.execute_command(args)
            if isinstance(result, list) and result:
                return result[0]
            return result
        finally:
            self.pool.return_connection(conn)

    def json_arrinsert(self, key: str, path: str, index: int, *values) -> Optional[int]:
        """Insert values into JSON array at index"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['JSON.ARRINSERT', key, path, str(index)]
            args.extend([json.dumps(v) for v in values])
            result = conn.execute_command(args)
            if isinstance(result, list) and result:
                return result[0]
            return result
        finally:
            self.pool.return_connection(conn)

    def json_arrlen(self, key: str, path: str = '$') -> Optional[int]:
        """Get JSON array length at path"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            result = conn.execute_command(['JSON.ARRLEN', key, path])
            if isinstance(result, list) and result:
                return result[0]
            return result
        finally:
            self.pool.return_connection(conn)

    def json_arrpop(self, key: str, path: str = '$', index: int = -1) -> Optional[Any]:
        """Remove and return element from JSON array"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            result = conn.execute_command(['JSON.ARRPOP', key, path, str(index)])
            if result:
                try:
                    return json.loads(result)
                except (json.JSONDecodeError, TypeError):
                    return result
            return None
        finally:
            self.pool.return_connection(conn)

    def json_arrtrim(self, key: str, path: str, start: int, stop: int) -> Optional[int]:
        """Trim JSON array to specified range"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            result = conn.execute_command(['JSON.ARRTRIM', key, path, str(start), str(stop)])
            if isinstance(result, list) and result:
                return result[0]
            return result
        finally:
            self.pool.return_connection(conn)

    # ===== OBJECT OPERATIONS =====

    def json_objkeys(self, key: str, path: str = '$') -> Optional[List[str]]:
        """Get JSON object keys at path"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            result = conn.execute_command(['JSON.OBJKEYS', key, path])
            if isinstance(result, list) and result:
                if isinstance(result[0], list):
                    return result[0]
            return result
        finally:
            self.pool.return_connection(conn)

    def json_objlen(self, key: str, path: str = '$') -> Optional[int]:
        """Get JSON object field count at path"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            result = conn.execute_command(['JSON.OBJLEN', key, path])
            if isinstance(result, list) and result:
                return result[0]
            return result
        finally:
            self.pool.return_connection(conn)

    # ===== UTILITY OPERATIONS =====

    def json_clear(self, key: str, path: str = '$') -> int:
        """Clear JSON value at path"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['JSON.CLEAR', key, path])
        finally:
            self.pool.return_connection(conn)

    def json_resp(self, key: str, path: str = '$') -> Any:
        """Get JSON value in RESP format"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['JSON.RESP', key, path])
        finally:
            self.pool.return_connection(conn)

    def json_debug_memory(self, key: str, path: str = '$') -> Optional[int]:
        """Get memory usage of JSON value"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            result = conn.execute_command(['JSON.DEBUG', 'MEMORY', key, path])
            if isinstance(result, list) and result:
                return result[0]
            return result
        finally:
            self.pool.return_connection(conn)

    # ===== ASYNC OPERATIONS =====

    async def json_set_async(self, key: str, path: str, value: Any, nx: bool = False,
                            xx: bool = False) -> Optional[str]:
        """Async set JSON value"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.json_set, key, path, value, nx, xx)

    async def json_get_async(self, key: str, *paths, indent: str = None, newline: str = None,
                            space: str = None, noescape: bool = False) -> Optional[Any]:
        """Async get JSON value"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.json_get, key, *paths,
                                         indent, newline, space, noescape)

    async def json_mget_async(self, keys: List[str], path: str = '$') -> List[Optional[Any]]:
        """Async get multiple JSON values"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.json_mget, keys, path)

    async def json_del_async(self, key: str, path: str = '$') -> int:
        """Async delete JSON value"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.json_del, key, path)

    async def json_numincrby_async(self, key: str, path: str, value: Union[int, float]) -> Optional[float]:
        """Async increment numeric value"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.json_numincrby, key, path, value)

    async def json_arrappend_async(self, key: str, path: str, *values) -> Optional[int]:
        """Async append to JSON array"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.json_arrappend, key, path, *values)

    async def json_objkeys_async(self, key: str, path: str = '$') -> Optional[List[str]]:
        """Async get JSON object keys"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.json_objkeys, key, path)
