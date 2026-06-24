# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True

"""
High-performance RedisGraph module for graph database operations

Supports:
- Cypher query execution
- Graph creation and manipulation
- Node and edge operations
- Path queries
- Graph statistics
"""

import asyncio
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor

from cy_redis.core.cy_redis_client cimport CyRedisConnection, CyRedisConnectionPool


cdef class CyRedisGraph:
    """
    High-performance graph database operations

    Provides graph database capabilities with:
    - Cypher query language support
    - Node and relationship operations
    - Path finding
    - Graph algorithms
    """

    cdef CyRedisConnectionPool pool
    cdef object _executor

    def __init__(self, str host='localhost', int port=6379, int max_connections=10,
                 int timeout=5, str password=None, int db=0):
        """Initialize graph operations with connection pool"""
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

    # ===== GRAPH OPERATIONS =====

    def query(self, graph_name: str, query: str, params: Dict = None,
              timeout: int = None, readonly: bool = False) -> Dict[str, Any]:
        """
        Execute Cypher query

        Args:
            graph_name: Name of the graph
            query: Cypher query string
            params: Query parameters
            timeout: Query timeout in milliseconds
            readonly: Use read-only query (GRAPH.RO_QUERY)
        """
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            cmd = 'GRAPH.RO_QUERY' if readonly else 'GRAPH.QUERY'
            args = [cmd, graph_name, query]

            if timeout is not None:
                args.extend(['TIMEOUT', str(timeout)])

            result = conn.execute_command(args)
            return self._parse_query_result(result)
        finally:
            self.pool.return_connection(conn)

    cdef dict _parse_query_result(self, result):
        """Parse query result"""
        if not result or not isinstance(result, list):
            return {'results': [], 'statistics': {}, 'metadata': []}

        cdef dict parsed = {
            'results': [],
            'statistics': {},
            'metadata': []
        }
        cdef int reply_length = len(result)
        # Precondition: guarded above to be a non-empty list.
        assert reply_length > 0, "non-empty reply expected past the guard"

        # Parse result sets
        if isinstance(result[0], list):
            # First element is metadata (column names)
            if result[0]:
                parsed['metadata'] = result[0]

            # Following elements are result rows. Bounded by len(result[1]).
            if reply_length > 1:
                for row in result[1]:
                    if isinstance(row, list):
                        parsed['results'].append(row)

        # Last element is statistics. Bounded by len(result[-1]).
        if reply_length > 1 and isinstance(result[-1], list):
            for stat in result[-1]:
                if isinstance(stat, str) and ': ' in stat:
                    key, value = stat.split(': ', 1)
                    parsed['statistics'][key] = value

        # Postcondition: we never invent rows beyond those present in the reply.
        assert len(parsed['results']) <= reply_length, \
            "parsed rows cannot exceed reply length"
        return parsed

    def delete(self, graph_name: str) -> str:
        """Delete a graph"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['GRAPH.DELETE', graph_name])
        finally:
            self.pool.return_connection(conn)

    def explain(self, graph_name: str, query: str) -> List[str]:
        """Get query execution plan"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['GRAPH.EXPLAIN', graph_name, query])
        finally:
            self.pool.return_connection(conn)

    def profile(self, graph_name: str, query: str) -> List[str]:
        """Profile query execution"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['GRAPH.PROFILE', graph_name, query])
        finally:
            self.pool.return_connection(conn)

    def slowlog(self, graph_name: str) -> List[Any]:
        """Get slow query log"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['GRAPH.SLOWLOG', graph_name])
        finally:
            self.pool.return_connection(conn)

    def config_set(self, config_name: str, value: Any) -> str:
        """Set configuration value"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['GRAPH.CONFIG', 'SET', config_name, str(value)])
        finally:
            self.pool.return_connection(conn)

    def config_get(self, config_name: str) -> Any:
        """Get configuration value"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            result = conn.execute_command(['GRAPH.CONFIG', 'GET', config_name])
            if isinstance(result, list) and len(result) > 1:
                return result[1]
            return result
        finally:
            self.pool.return_connection(conn)

    def list_graphs(self) -> List[str]:
        """List all graphs"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['GRAPH.LIST'])
        finally:
            self.pool.return_connection(conn)

    # ===== CONVENIENCE METHODS =====

    def create_node(self, graph_name: str, label: str, properties: Dict = None) -> Dict:
        """Create a node"""
        props_str = ''
        if properties:
            props_list = [f"{k}: {self._format_value(v)}" for k, v in properties.items()]
            props_str = ' {' + ', '.join(props_list) + '}'

        query = f"CREATE (n:{label}{props_str}) RETURN n"
        return self.query(graph_name, query)

    def create_edge(self, graph_name: str, from_id: int, to_id: int,
                   edge_type: str, properties: Dict = None) -> Dict:
        """Create an edge between nodes"""
        props_str = ''
        if properties:
            props_list = [f"{k}: {self._format_value(v)}" for k, v in properties.items()]
            props_str = ' {' + ', '.join(props_list) + '}'

        query = f"""
        MATCH (a), (b)
        WHERE ID(a) = {from_id} AND ID(b) = {to_id}
        CREATE (a)-[r:{edge_type}{props_str}]->(b)
        RETURN r
        """
        return self.query(graph_name, query)

    def find_nodes(self, graph_name: str, label: str = None,
                  properties: Dict = None, limit: int = None) -> Dict:
        """Find nodes by label and/or properties"""
        match_clause = f"(n:{label})" if label else "(n)"

        where_clauses = []
        if properties:
            for k, v in properties.items():
                where_clauses.append(f"n.{k} = {self._format_value(v)}")

        where_str = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        limit_str = f" LIMIT {limit}" if limit else ""

        query = f"MATCH {match_clause}{where_str} RETURN n{limit_str}"
        return self.query(graph_name, query)

    def find_shortest_path(self, graph_name: str, from_id: int, to_id: int,
                          edge_type: str = None, max_hops: int = None) -> Dict:
        """Find shortest path between two nodes"""
        edge_pattern = f"[:{edge_type}*1..{max_hops}]" if edge_type else "[*1..]"
        if max_hops and not edge_type:
            edge_pattern = f"[*1..{max_hops}]"

        query = f"""
        MATCH (a), (b), p = shortestPath((a)-{edge_pattern}-(b))
        WHERE ID(a) = {from_id} AND ID(b) = {to_id}
        RETURN p
        """
        return self.query(graph_name, query)

    def delete_node(self, graph_name: str, node_id: int) -> Dict:
        """Delete a node"""
        query = f"MATCH (n) WHERE ID(n) = {node_id} DELETE n"
        return self.query(graph_name, query)

    def delete_edge(self, graph_name: str, edge_id: int) -> Dict:
        """Delete an edge"""
        query = f"MATCH ()-[r]->() WHERE ID(r) = {edge_id} DELETE r"
        return self.query(graph_name, query)

    def get_neighbors(self, graph_name: str, node_id: int,
                     edge_type: str = None, direction: str = 'both') -> Dict:
        """Get neighboring nodes"""
        if direction == 'outgoing':
            pattern = f"(n)-[:{edge_type}]->(m)" if edge_type else "(n)-->(m)"
        elif direction == 'incoming':
            pattern = f"(n)<-[:{edge_type}]-(m)" if edge_type else "(n)<--(m)"
        else:  # both
            pattern = f"(n)-[:{edge_type}]-(m)" if edge_type else "(n)--(m)"

        query = f"""
        MATCH {pattern}
        WHERE ID(n) = {node_id}
        RETURN m
        """
        return self.query(graph_name, query)

    cdef str _format_value(self, value):
        """Format a value as a Cypher literal.

        String values are escaped (backslash and single-quote) before being
        wrapped in quotes, so a value containing a quote cannot break out of the
        literal and inject Cypher.
        """
        cdef str formatted
        cdef str escaped
        if isinstance(value, str):
            escaped = (<str>value).replace('\\', '\\\\').replace("'", "\\'")
            formatted = f"'{escaped}'"
        elif isinstance(value, bool):
            formatted = 'true' if value else 'false'
        elif value is None:
            formatted = 'null'
        else:
            formatted = str(value)
        # Postcondition: every branch yields a non-empty Cypher literal.
        assert formatted is not None and len(formatted) > 0, \
            "formatted value must be a non-empty literal"
        return formatted

    # ===== ASYNC OPERATIONS =====

    async def query_async(self, graph_name: str, query: str, params: Dict = None,
                         timeout: int = None, readonly: bool = False) -> Dict[str, Any]:
        """Async execute query"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.query, graph_name,
                                         query, params, timeout, readonly)

    async def create_node_async(self, graph_name: str, label: str,
                               properties: Dict = None) -> Dict:
        """Async create node"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.create_node,
                                         graph_name, label, properties)

    async def find_nodes_async(self, graph_name: str, label: str = None,
                              properties: Dict = None, limit: int = None) -> Dict:
        """Async find nodes"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.find_nodes,
                                         graph_name, label, properties, limit)

    async def find_shortest_path_async(self, graph_name: str, from_id: int, to_id: int,
                                      edge_type: str = None, max_hops: int = None) -> Dict:
        """Async find shortest path"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.find_shortest_path,
                                         graph_name, from_id, to_id, edge_type, max_hops)
