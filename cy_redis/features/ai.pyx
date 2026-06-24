# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# distutils: language = c++

"""
High-performance RedisAI module for AI/ML model inference

Supports:
- Model loading (TensorFlow, PyTorch, ONNX)
- Tensor operations with NumPy integration
- Model execution
- Script execution (TorchScript)
- Zero-copy tensor operations
"""

import numpy as np
cimport numpy as cnp
import asyncio
from typing import Any, Dict, List, Optional, Union
from concurrent.futures import ThreadPoolExecutor


from cy_redis.core.cy_redis_client cimport CyRedisConnection, CyRedisConnectionPool


cdef class CyRedisAI:
    """
    High-performance AI/ML operations with NumPy integration

    Provides:
    - Model management (TF, PyTorch, ONNX)
    - Tensor operations with zero-copy NumPy arrays
    - Model inference
    - Script execution
    - DAG (Directed Acyclic Graph) for complex workflows
    """

    cdef CyRedisConnectionPool pool
    cdef object _executor

    def __init__(self, str host='localhost', int port=6379, int max_connections=10,
                 int timeout=5, str password=None, int db=0):
        """Initialize AI operations with connection pool"""
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

    # ===== MODEL OPERATIONS =====

    def modelstore(self, key: str, backend: str, device: str, data: bytes,
                  batch: int = None, minbatch: int = None,
                  minbatchtimeout: int = None, tag: str = None,
                  inputs: List[str] = None, outputs: List[str] = None) -> str:
        """
        Store AI model

        Args:
            key: Model key
            backend: Backend type (TF, TORCH, ONNX, TFLITE)
            device: Device (CPU, GPU)
            data: Model binary data
            batch: Batch size
            minbatch: Minimum batch size for batching
            minbatchtimeout: Timeout for batching in ms
            tag: Model tag/version
            inputs: Input tensor names
            outputs: Output tensor names
        """
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['AI.MODELSTORE', key, backend, device]

            if tag:
                args.extend(['TAG', tag])

            if batch is not None:
                args.extend(['BATCHSIZE', str(batch)])

            if minbatch is not None:
                args.extend(['MINBATCHSIZE', str(minbatch)])

            if minbatchtimeout is not None:
                args.extend(['MINBATCHTIMEOUT', str(minbatchtimeout)])

            if inputs:
                args.append('INPUTS')
                args.append(str(len(inputs)))
                args.extend(inputs)

            if outputs:
                args.append('OUTPUTS')
                args.append(str(len(outputs)))
                args.extend(outputs)

            args.append('BLOB')
            args.append(data)

            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def modelget(self, key: str, meta: bool = False, blob: bool = False) -> Dict[str, Any]:
        """Get model metadata and/or blob"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['AI.MODELGET', key]
            if meta:
                args.append('META')
            if blob:
                args.append('BLOB')

            result = conn.execute_command(args)
            return self._parse_model_info(result)
        finally:
            self.pool.return_connection(conn)

    def modeldel(self, key: str) -> str:
        """Delete a model"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['AI.MODELDEL', key])
        finally:
            self.pool.return_connection(conn)

    def modelexecute(self, key: str, inputs: List[str], outputs: List[str],
                    timeout: int = None) -> str:
        """
        Execute model inference

        Args:
            key: Model key
            inputs: Input tensor keys
            outputs: Output tensor keys
            timeout: Execution timeout in ms
        """
        # Precondition: RedisAI requires at least one input and one output;
        # an empty list would emit a count of 0 and a malformed command.
        if not inputs:
            raise ValueError("modelexecute requires at least one input")
        if not outputs:
            raise ValueError("modelexecute requires at least one output")

        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['AI.MODELEXECUTE', key, 'INPUTS']
            args.append(str(len(inputs)))
            args.extend(inputs)
            args.append('OUTPUTS')
            args.append(str(len(outputs)))
            args.extend(outputs)

            # Invariant: token count is fixed wrt the input/output cardinalities
            # (3 header + 1 inputs-count + n_in + 1 OUTPUTS + 1 out-count + n_out).
            assert len(args) == 6 + len(inputs) + len(outputs), \
                "arg count must match declared input/output sizes"

            if timeout is not None:
                args.extend(['TIMEOUT', str(timeout)])

            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    # ===== TENSOR OPERATIONS =====

    def tensorset(self, key: str, dtype: str, shape: List[int],
                 values: Union[cnp.ndarray, List] = None, blob: bytes = None) -> str:
        """
        Set tensor with NumPy array

        Args:
            key: Tensor key
            dtype: Data type (FLOAT, DOUBLE, INT8, INT16, INT32, INT64, UINT8, UINT16)
            shape: Tensor shape
            values: NumPy array or list of values
            blob: Raw binary data
        """
        cdef int dims_appended = 0

        # Precondition: shape dimensions must be non-negative (a negative dim is
        # a caller error and would make the element-count product meaningless).
        if shape is None:
            raise ValueError("shape must not be None")
        for dim in shape:
            if dim < 0:
                raise ValueError("tensor shape dimensions must be non-negative")

        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['AI.TENSORSET', key, dtype]

            # Add shape
            for dim in shape:
                args.append(str(dim))
                dims_appended += 1
            # Invariant: emitted exactly one token per declared dimension.
            assert dims_appended == len(shape), "must emit one token per dim"

            if values is not None:
                # Convert to NumPy if needed
                if not isinstance(values, np.ndarray):
                    values = np.array(values)

                # Flatten and convert to bytes
                args.append('BLOB')
                args.append(values.tobytes())
            elif blob is not None:
                args.append('BLOB')
                args.append(blob)
            else:
                args.append('VALUES')

            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def tensorget(self, key: str, meta: bool = False, blob: bool = True) -> Dict[str, Any]:
        """
        Get tensor as NumPy array

        Args:
            key: Tensor key
            meta: Include metadata
            blob: Include tensor data

        Returns:
            Dict with 'dtype', 'shape', and optionally 'values' (as NumPy array)
        """
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['AI.TENSORGET', key]
            if meta:
                args.append('META')
            if blob:
                args.append('BLOB')

            result = conn.execute_command(args)
            return self._parse_tensor_result(result, blob)
        finally:
            self.pool.return_connection(conn)

    cdef dict _parse_tensor_result(self, result, bint with_blob):
        """Parse tensor result into NumPy array"""
        if not result or not isinstance(result, list):
            return {}

        cdef dict parsed = {}
        cdef int i = 0
        cdef int reply_length = len(result)
        # Precondition: the reply is a non-empty list (guaranteed above).
        assert reply_length > 0, "non-empty reply expected past the guard"

        # Bounded by reply_length: every branch advances `i` by >=1, so the
        # outer loop terminates in at most `reply_length` iterations.
        while i < reply_length:
            if result[i] == b'dtype':
                # NOTE(latent bug): a trailing lone b'dtype' indexes result[i+1]
                # out of range; preserved as-is to keep behavior unchanged.
                parsed['dtype'] = result[i + 1].decode('utf-8') if isinstance(result[i + 1], bytes) else result[i + 1]
                i += 2
            elif result[i] == b'shape':
                i += 1
                shape = []
                while i < reply_length and isinstance(result[i], int):
                    shape.append(result[i])
                    i += 1
                parsed['shape'] = shape
            elif result[i] == b'values' or result[i] == b'blob':
                if with_blob and i + 1 < reply_length:
                    blob_data = result[i + 1]
                    if isinstance(blob_data, bytes) and 'dtype' in parsed and 'shape' in parsed:
                        # Convert to NumPy array. Reconstructed element count must
                        # equal the product of the declared shape, else the reply
                        # is malformed (truncated/oversized blob) -> raise.
                        np_dtype = self._redis_dtype_to_numpy(parsed['dtype'])
                        flat = np.frombuffer(blob_data, dtype=np_dtype)
                        expected_elements = 1
                        for dim in parsed['shape']:
                            assert dim >= 0, "tensor dimension must be non-negative"
                            expected_elements *= dim
                        if flat.size != expected_elements:
                            raise ValueError(
                                "tensor blob length %d does not match shape %r "
                                "(expected %d elements)"
                                % (flat.size, parsed['shape'], expected_elements)
                            )
                        parsed['values'] = flat.reshape(parsed['shape'])
                i += 2
            else:
                i += 1

        # Postcondition: the scan consumed the whole reply, never ran past it.
        assert i >= reply_length, "parser must consume the full reply"
        return parsed

    cdef object _redis_dtype_to_numpy(self, str dtype):
        """Convert Redis AI dtype to NumPy dtype"""
        assert dtype is not None, "dtype must not be None"
        dtype_map = {
            'FLOAT': np.float32,
            'DOUBLE': np.float64,
            'INT8': np.int8,
            'INT16': np.int16,
            'INT32': np.int32,
            'INT64': np.int64,
            'UINT8': np.uint8,
            'UINT16': np.uint16,
        }
        return dtype_map.get(dtype.upper(), np.float32)

    cdef dict _parse_model_info(self, result):
        """Parse model info result"""
        if not result or not isinstance(result, list):
            return {}

        cdef dict parsed = {}
        cdef int i = 0
        cdef int reply_length = len(result)

        # Bounded by reply_length: `i` advances by 2 each iteration, so the loop
        # runs at most ceil(reply_length / 2) times.
        while i < reply_length:
            if i + 1 < reply_length:
                key = result[i].decode('utf-8') if isinstance(result[i], bytes) else result[i]
                value = result[i + 1]
                if isinstance(value, bytes):
                    value = value.decode('utf-8')
                parsed[key] = value
            i += 2

        # Postcondition: scan never reads before the start and stops past the end.
        assert i >= reply_length, "parser must consume the full reply"
        assert len(parsed) <= (reply_length + 1) // 2, "at most one entry per pair"
        return parsed

    # ===== SCRIPT OPERATIONS =====

    def scriptstore(self, key: str, device: str, tag: str, script: str,
                   entry_points: List[str] = None) -> str:
        """
        Store TorchScript

        Args:
            key: Script key
            device: Device (CPU, GPU)
            tag: Script tag/version
            script: Script source code
            entry_points: Function entry points
        """
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['AI.SCRIPTSTORE', key, device]

            if tag:
                args.extend(['TAG', tag])

            if entry_points:
                args.append('ENTRY_POINTS')
                args.append(str(len(entry_points)))
                args.extend(entry_points)

            args.append('SOURCE')
            args.append(script)

            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def scriptexecute(self, key: str, function: str, inputs: List[str],
                     outputs: List[str], timeout: int = None) -> str:
        """
        Execute script function

        Args:
            key: Script key
            function: Function name to execute
            inputs: Input tensor keys
            outputs: Output tensor keys
            timeout: Execution timeout in ms
        """
        # Precondition: a script execution needs at least one input and output.
        if not inputs:
            raise ValueError("scriptexecute requires at least one input")
        if not outputs:
            raise ValueError("scriptexecute requires at least one output")

        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['AI.SCRIPTEXECUTE', key, function, 'INPUTS']
            args.append(str(len(inputs)))
            args.extend(inputs)
            args.append('OUTPUTS')
            args.append(str(len(outputs)))
            args.extend(outputs)

            # Invariant: 4 header tokens + 1 inputs-count + n_in + 1 OUTPUTS kw
            # + 1 outputs-count + n_out.
            assert len(args) == 7 + len(inputs) + len(outputs), \
                "arg count must match declared input/output sizes"

            if timeout is not None:
                args.extend(['TIMEOUT', str(timeout)])

            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    def scriptdel(self, key: str) -> str:
        """Delete a script"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            return conn.execute_command(['AI.SCRIPTDEL', key])
        finally:
            self.pool.return_connection(conn)

    # ===== DAG OPERATIONS =====

    def dagexecute(self, commands: List[List[str]], load: List[str] = None,
                  persist: List[str] = None, timeout: int = None) -> List[Any]:
        """
        Execute DAG (Directed Acyclic Graph) of operations

        Args:
            commands: List of AI commands
            load: Tensors to load
            persist: Tensors to persist
            timeout: Execution timeout in ms
        """
        cdef int commands_emitted = 0

        # Precondition: a DAG must contain at least one command to execute.
        if not commands:
            raise ValueError("dagexecute requires at least one command")

        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['AI.DAGEXECUTE']

            if load:
                args.append('LOAD')
                args.append(str(len(load)))
                args.extend(load)

            if persist:
                args.append('PERSIST')
                args.append(str(len(persist)))
                args.extend(persist)

            # Add commands. Bounded by len(commands); each command contributes a
            # '|>' separator plus its own tokens.
            for cmd in commands:
                args.append('|>')
                args.extend(cmd)
                commands_emitted += 1
            assert commands_emitted == len(commands), "must emit every command"

            if timeout is not None:
                args.extend(['TIMEOUT', str(timeout)])

            return conn.execute_command(args)
        finally:
            self.pool.return_connection(conn)

    # ===== INFO =====

    def info(self, key: str = None, reset: bool = False) -> Dict[str, Any]:
        """Get AI module info"""
        conn = self.pool.get_connection()
        if conn == None:
            raise ConnectionError("No available connections")

        try:
            args = ['AI.INFO']
            if key:
                args.append(key)
            if reset:
                args.append('RESETSTAT')

            result = conn.execute_command(args)
            return self._parse_model_info(result)
        finally:
            self.pool.return_connection(conn)

    # ===== ASYNC OPERATIONS =====

    async def modelexecute_async(self, key: str, inputs: List[str], outputs: List[str],
                                timeout: int = None) -> str:
        """Async model execution"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.modelexecute,
                                         key, inputs, outputs, timeout)

    async def tensorset_async(self, key: str, dtype: str, shape: List[int],
                             values: Union[cnp.ndarray, List] = None, blob: bytes = None) -> str:
        """Async tensor set"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.tensorset,
                                         key, dtype, shape, values, blob)

    async def tensorget_async(self, key: str, meta: bool = False, blob: bool = True) -> Dict[str, Any]:
        """Async tensor get"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.tensorget, key, meta, blob)

    async def dagexecute_async(self, commands: List[List[str]], load: List[str] = None,
                              persist: List[str] = None, timeout: int = None) -> List[Any]:
        """Async DAG execution"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.dagexecute,
                                         commands, load, persist, timeout)
