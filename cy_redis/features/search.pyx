# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True

"""
High-performance RediSearch module for full-text search

Supports RediSearch module commands:
- FT.CREATE: Create search index
- FT.SEARCH: Full-text search
- FT.AGGREGATE: Aggregation queries
- FT.SUGADD/SUGGET: Auto-complete
- FT.DICTADD/DICTDEL: Dictionary operations
"""

import asyncio
from typing import Any, Dict, List, Optional, Tuple, Union
from concurrent.futures import ThreadPoolExecutor

from cy_redis.core.cy_redis_client cimport CyRedisConnection, CyRedisConnectionPool


cdef class CyRedisSearch:
    """
    High-performance RediSearch operations

    Provides full-text search capabilities with:
    - Index creation and management
    - Complex queries with filters
    - Aggregation and grouping
    - Auto-complete suggestions
    - Fuzzy matching
    """

    cdef CyRedisConnectionPool pool
    cdef object _executor

    def __init__(self, str host='localhost', int port=6379, int max_connections=10,
                 int timeout=5, str password=None, int db=0):
        """Initialize search operations with connection pool"""
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
        return self._executor

    def __dealloc__(self):
        if self._executor:
            self._executor.shutdown(wait=False)

    # ===== INDEX MANAGEMENT =====

    def ft_create(self, index: str, schema: List[Tuple[str, str, Dict]],
                  on: str = 'HASH', prefix: List[str] = None,
                  language: str = None, score: float = None,
                  score_field: str = None, payload_field: str = None) -> str:
        """
        Create a search index

        Args:
            index: Index name
            schema: List of (field_name, field_type, options) tuples
                   field_type: TEXT, TAG, NUMERIC, GEO, VECTOR
            on: Index type (HASH or JSON)
            prefix: List of key prefixes to index
            language: Default language for stemming
            score: Default document score
            score_field: Field containing document score
            payload_field: Field containing document payload
        """
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['FT.CREATE', index]

            if on:
                args.extend(['ON', on])

            if prefix:
                args.append('PREFIX')
                args.append(str(len(prefix)))
                args.extend(prefix)

            if language:
                args.extend(['LANGUAGE', language])

            if score is not None:
                args.extend(['SCORE', str(score)])

            if score_field:
                args.extend(['SCORE_FIELD', score_field])

            if payload_field:
                args.extend(['PAYLOAD_FIELD', payload_field])

            # Add schema
            args.append('SCHEMA')
            for field_name, field_type, options in schema:
                args.append(field_name)
                args.append(field_type)

                # Add field options
                if 'sortable' in options and options['sortable']:
                    args.append('SORTABLE')
                if 'nostem' in options and options['nostem']:
                    args.append('NOSTEM')
                if 'noindex' in options and options['noindex']:
                    args.append('NOINDEX')
                if 'phonetic' in options:
                    args.extend(['PHONETIC', options['phonetic']])
                if 'weight' in options:
                    args.extend(['WEIGHT', str(options['weight'])])
                if 'separator' in options:
                    args.extend(['SEPARATOR', options['separator']])

            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def ft_dropindex(self, index: str, delete_docs: bool = False) -> str:
        """Drop a search index"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['FT.DROPINDEX', index]
            if delete_docs:
                args.append('DD')
            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def ft_info(self, index: str) -> Dict[str, Any]:
        """Get index information"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            result = conn.execute_command(['FT.INFO', index])
            # Parse result into dict
            if result and isinstance(result, list):
                return {result[i]: result[i+1] for i in range(0, len(result), 2)}
            return {}
        finally:
            self.pool.return_connection(conn)

    def ft_alter(self, index: str, schema_add: List[Tuple[str, str, Dict]]) -> str:
        """Add fields to existing index"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['FT.ALTER', index, 'SCHEMA', 'ADD']

            for field_name, field_type, options in schema_add:
                args.append(field_name)
                args.append(field_type)

                if 'sortable' in options and options['sortable']:
                    args.append('SORTABLE')
                if 'noindex' in options and options['noindex']:
                    args.append('NOINDEX')

            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    # ===== SEARCH OPERATIONS =====

    def ft_search(self, index: str, query: str, nocontent: bool = False,
                  verbatim: bool = False, nostopwords: bool = False,
                  withscores: bool = False, withpayloads: bool = False,
                  withsortkeys: bool = False, filter: List[Tuple[str, int, int]] = None,
                  geofilter: Tuple[str, float, float, float, str] = None,
                  inkeys: List[str] = None, infields: List[str] = None,
                  return_fields: List[str] = None, summarize: Dict = None,
                  highlight: Dict = None, slop: int = None, timeout: int = None,
                  inorder: bool = False, language: str = None,
                  expander: str = None, scorer: str = None,
                  explainscore: bool = False, payload: str = None,
                  sortby: str = None, asc: bool = True,
                  limit: Tuple[int, int] = None) -> Dict[str, Any]:
        """
        Full-text search with advanced options

        Args:
            index: Index name
            query: Search query string
            nocontent: Return only document IDs
            verbatim: Don't use stemming
            nostopwords: Don't filter stopwords
            withscores: Return document scores
            withpayloads: Return document payloads
            withsortkeys: Return sort keys
            filter: Numeric filter [(field, min, max)]
            geofilter: Geographic filter (field, lon, lat, radius, unit)
            inkeys: Limit search to specific keys
            infields: Limit search to specific fields
            return_fields: Fields to return
            summarize: Text summarization options
            highlight: Highlighting options
            slop: Slop for phrase queries
            timeout: Query timeout in milliseconds
            inorder: Match terms in order
            language: Query language
            expander: Query expander
            scorer: Scoring function
            explainscore: Return score explanation
            payload: Document payload
            sortby: Field to sort by
            asc: Sort ascending (default True)
            limit: Result pagination (offset, num)
        """
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['FT.SEARCH', index, query]

            if nocontent:
                args.append('NOCONTENT')
            if verbatim:
                args.append('VERBATIM')
            if nostopwords:
                args.append('NOSTOPWORDS')
            if withscores:
                args.append('WITHSCORES')
            if withpayloads:
                args.append('WITHPAYLOADS')
            if withsortkeys:
                args.append('WITHSORTKEYS')

            if filter:
                for field, min_val, max_val in filter:
                    args.extend(['FILTER', field, str(min_val), str(max_val)])

            if geofilter:
                field, lon, lat, radius, unit = geofilter
                args.extend(['GEOFILTER', field, str(lon), str(lat), str(radius), unit])

            if inkeys:
                args.append('INKEYS')
                args.append(str(len(inkeys)))
                args.extend(inkeys)

            if infields:
                args.append('INFIELDS')
                args.append(str(len(infields)))
                args.extend(infields)

            if return_fields:
                args.append('RETURN')
                args.append(str(len(return_fields)))
                args.extend(return_fields)

            if summarize:
                args.append('SUMMARIZE')
                if 'fields' in summarize:
                    args.append('FIELDS')
                    args.append(str(len(summarize['fields'])))
                    args.extend(summarize['fields'])
                if 'frags' in summarize:
                    args.extend(['FRAGS', str(summarize['frags'])])
                if 'len' in summarize:
                    args.extend(['LEN', str(summarize['len'])])
                if 'separator' in summarize:
                    args.extend(['SEPARATOR', summarize['separator']])

            if highlight:
                args.append('HIGHLIGHT')
                if 'fields' in highlight:
                    args.append('FIELDS')
                    args.append(str(len(highlight['fields'])))
                    args.extend(highlight['fields'])
                if 'tags' in highlight:
                    open_tag, close_tag = highlight['tags']
                    args.extend(['TAGS', open_tag, close_tag])

            if slop is not None:
                args.extend(['SLOP', str(slop)])

            if timeout is not None:
                args.extend(['TIMEOUT', str(timeout)])

            if inorder:
                args.append('INORDER')

            if language:
                args.extend(['LANGUAGE', language])

            if expander:
                args.extend(['EXPANDER', expander])

            if scorer:
                args.extend(['SCORER', scorer])

            if explainscore:
                args.append('EXPLAINSCORE')

            if payload:
                args.extend(['PAYLOAD', payload])

            if sortby:
                args.extend(['SORTBY', sortby])
                args.append('ASC' if asc else 'DESC')

            if limit:
                offset, num = limit
                args.extend(['LIMIT', str(offset), str(num)])

            result = conn.execute_command(args)
            return self._parse_search_result(result)
        finally:
            self.pool.return_connection(conn)

    cdef dict _parse_search_result(self, result):
        """Parse FT.SEARCH result into structured format"""
        if not result or not isinstance(result, list) or len(result) < 1:
            return {'total': 0, 'docs': []}

        cdef int total = result[0]
        cdef list docs = []

        # Parse documents
        cdef int i = 1
        while i < len(result):
            doc_id = result[i]
            i += 1

            if i < len(result) and isinstance(result[i], list):
                fields = result[i]
                doc = {'id': doc_id}

                # Parse field-value pairs
                for j in range(0, len(fields), 2):
                    if j + 1 < len(fields):
                        doc[fields[j]] = fields[j + 1]

                docs.append(doc)
                i += 1

        return {'total': total, 'docs': docs}

    # ===== AGGREGATION =====

    def ft_aggregate(self, index: str, query: str, load: List[str] = None,
                    groupby: Dict = None, sortby: List[Tuple[str, str]] = None,
                    apply: List[Tuple[str, str]] = None,
                    filter: str = None, limit: Tuple[int, int] = None) -> List[Dict]:
        """
        Perform aggregation query

        Args:
            index: Index name
            query: Base query
            load: Fields to load
            groupby: Group by configuration
            sortby: Sort configuration [(field, direction)]
            apply: Apply transformations [(expr, as_field)]
            filter: Filter expression
            limit: Result pagination (offset, num)
        """
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['FT.AGGREGATE', index, query]

            if load:
                args.append('LOAD')
                args.append(str(len(load)))
                args.extend(load)

            if groupby:
                args.append('GROUPBY')
                args.append(str(len(groupby.get('fields', []))))
                args.extend(groupby.get('fields', []))

                for reducer in groupby.get('reduce', []):
                    func, *reducer_args = reducer
                    args.append('REDUCE')
                    args.append(func)
                    args.append(str(len(reducer_args)))
                    args.extend(reducer_args)

            if sortby:
                args.append('SORTBY')
                args.append(str(len(sortby) * 2))
                for field, direction in sortby:
                    args.extend([field, direction])

            if apply:
                for expr, as_field in apply:
                    args.extend(['APPLY', expr, 'AS', as_field])

            if filter:
                args.extend(['FILTER', filter])

            if limit:
                offset, num = limit
                args.extend(['LIMIT', str(offset), str(num)])

            result = conn.execute_command(args)
            return self._parse_aggregate_result(result)
        finally:
            self.pool.return_connection(conn)

    cdef list _parse_aggregate_result(self, result):
        """Parse FT.AGGREGATE result"""
        if not result or not isinstance(result, list):
            return []

        cdef list docs = []
        cdef int i = 1  # Skip count

        while i < len(result):
            if isinstance(result[i], list):
                fields = result[i]
                doc = {}

                for j in range(0, len(fields), 2):
                    if j + 1 < len(fields):
                        doc[fields[j]] = fields[j + 1]

                docs.append(doc)
            i += 1

        return docs

    # ===== SUGGESTION (AUTO-COMPLETE) =====

    def ft_sugadd(self, key: str, string: str, score: float,
                  incr: bool = False, payload: str = None) -> int:
        """Add suggestion to auto-complete dictionary"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['FT.SUGADD', key, string, str(score)]
            if incr:
                args.append('INCR')
            if payload:
                args.extend(['PAYLOAD', payload])

            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def ft_sugget(self, key: str, prefix: str, fuzzy: bool = False,
                  withscores: bool = False, withpayloads: bool = False,
                  max: int = None) -> List[Union[str, Tuple]]:
        """Get auto-complete suggestions"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['FT.SUGGET', key, prefix]
            if fuzzy:
                args.append('FUZZY')
            if withscores:
                args.append('WITHSCORES')
            if withpayloads:
                args.append('WITHPAYLOADS')
            if max is not None:
                args.extend(['MAX', str(max)])

            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def ft_sugdel(self, key: str, string: str) -> int:
        """Delete suggestion from auto-complete dictionary"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['FT.SUGDEL', key, string])
        finally:
            self.pool.return_connection(conn)

    def ft_suglen(self, key: str) -> int:
        """Get number of suggestions in dictionary"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['FT.SUGLEN', key])
        finally:
            self.pool.return_connection(conn)

    # ===== ASYNC OPERATIONS =====

    async def ft_search_async(self, index: str, query: str, **kwargs) -> Dict[str, Any]:
        """Async full-text search"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.ft_search, index, query, **kwargs)

    async def ft_aggregate_async(self, index: str, query: str, **kwargs) -> List[Dict]:
        """Async aggregation"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.ft_aggregate, index, query, **kwargs)

    async def ft_sugget_async(self, key: str, prefix: str, **kwargs) -> List[Union[str, Tuple]]:
        """Async get suggestions"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.ft_sugget, key, prefix, **kwargs)
