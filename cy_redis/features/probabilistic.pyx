# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# distutils: language = c++
# distutils: include_dirs = cy_redis/cpp

"""
High-performance probabilistic data structures for Redis/Valkey

Implements:
- Bloom Filter: Space-efficient set membership testing
- Cuckoo Filter: Bloom filter with deletion support
- Count-Min Sketch: Frequency estimation for data streams
- Top-K: Track most frequent items

All backed by optimized C++ implementations for maximum performance.
"""

import asyncio
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor
from libcpp.string cimport string
from libcpp cimport bool as cpp_bool

# Import C++ classes
cdef extern from "cy_redis/cpp/bloom_filter.hpp" namespace "cyredis":
    cdef cppclass BloomFilter:
        BloomFilter(size_t capacity, double false_positive_rate) except +
        void add(const string& item)
        cpp_bool contains(const string& item)
        void clear()
        size_t size()
        size_t capacity()
        size_t num_hashes()
        double estimated_fpr()
        size_t memory_usage()

cdef extern from "cy_redis/cpp/cuckoo_filter.hpp" namespace "cyredis":
    cdef cppclass CuckooFilter:
        CuckooFilter(size_t capacity) except +
        cpp_bool add(const string& item)
        cpp_bool remove(const string& item)
        cpp_bool contains(const string& item)
        size_t size()
        size_t num_buckets()
        double load_factor()
        size_t memory_usage()
        void clear()

cdef extern from "cy_redis/cpp/count_min_sketch.hpp" namespace "cyredis":
    cdef cppclass CountMinSketch:
        CountMinSketch(double epsilon, double delta) except +
        CountMinSketch(size_t width, size_t depth) except +
        void add(const string& item, unsigned int count)
        unsigned int estimate(const string& item)
        void merge(const CountMinSketch& other) except +
        void clear()
        unsigned long long total_count()
        size_t width()
        size_t depth()
        size_t memory_usage()
        unsigned long long inner_product(const CountMinSketch& other) except +
        void conservative_add(const string& item, unsigned int count)


# ===== BLOOM FILTER =====

cdef class CyBloomFilter:
    """
    High-performance Bloom Filter with C++ backend

    A space-efficient probabilistic data structure for testing set membership.
    False positives are possible, but false negatives are not.

    Example:
        bloom = CyBloomFilter(capacity=10000, false_positive_rate=0.01)
        bloom.add("item1")
        bloom.add("item2")
        if bloom.contains("item1"):
            print("item1 might be in set")
    """
    cdef BloomFilter* _filter
    cdef object _executor

    def __cinit__(self, size_t capacity=10000, double false_positive_rate=0.01):
        """
        Initialize Bloom filter
        @param capacity: Expected number of elements
        @param false_positive_rate: Target false positive rate (0.0 to 1.0)
        """
        self._filter = new BloomFilter(capacity, false_positive_rate)
        self._executor = ThreadPoolExecutor(max_workers=4)

    def __dealloc__(self):
        if self._filter != NULL:
            del self._filter
        if self._executor:
            self._executor.shutdown(wait=False)

    @property
    def executor(self):
        return self._executor

    def add(self, str item):
        """Add an item to the filter"""
        cdef string cpp_item = item.encode('utf-8')
        self._filter.add(cpp_item)

    def contains(self, str item) -> bool:
        """Check if item might be in the set"""
        cdef string cpp_item = item.encode('utf-8')
        return self._filter.contains(cpp_item)

    def clear(self):
        """Clear all items from the filter"""
        self._filter.clear()

    @property
    def size(self) -> int:
        """Get number of items added"""
        return self._filter.size()

    @property
    def capacity(self) -> int:
        """Get total number of bits"""
        return self._filter.capacity()

    @property
    def num_hashes(self) -> int:
        """Get number of hash functions"""
        return self._filter.num_hashes()

    @property
    def estimated_fpr(self) -> float:
        """Get estimated false positive rate"""
        return self._filter.estimated_fpr()

    @property
    def memory_usage(self) -> int:
        """Get memory usage in bytes"""
        return self._filter.memory_usage()

    # Async operations
    async def add_async(self, str item):
        """Async add item"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self.executor, self.add, item)

    async def contains_async(self, str item) -> bool:
        """Async check membership"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.contains, item)


# ===== CUCKOO FILTER =====

cdef class CyCuckooFilter:
    """
    High-performance Cuckoo Filter with C++ backend

    A probabilistic data structure that supports adding and removing items
    with better space efficiency than Bloom filters.

    Example:
        cuckoo = CyCuckooFilter(capacity=10000)
        cuckoo.add("item1")
        if cuckoo.contains("item1"):
            cuckoo.remove("item1")
    """
    cdef CuckooFilter* _filter
    cdef object _executor

    def __cinit__(self, size_t capacity=10000):
        """
        Initialize Cuckoo filter
        @param capacity: Expected number of elements
        """
        self._filter = new CuckooFilter(capacity)
        self._executor = ThreadPoolExecutor(max_workers=4)

    def __dealloc__(self):
        if self._filter != NULL:
            del self._filter
        if self._executor:
            self._executor.shutdown(wait=False)

    @property
    def executor(self):
        return self._executor

    def add(self, str item) -> bool:
        """
        Add an item to the filter
        @return: True if successfully added, False if filter is full
        """
        cdef string cpp_item = item.encode('utf-8')
        return self._filter.add(cpp_item)

    def remove(self, str item) -> bool:
        """
        Remove an item from the filter
        @return: True if item was found and removed, False otherwise
        """
        cdef string cpp_item = item.encode('utf-8')
        return self._filter.remove(cpp_item)

    def contains(self, str item) -> bool:
        """Check if item might be in the set"""
        cdef string cpp_item = item.encode('utf-8')
        return self._filter.contains(cpp_item)

    def clear(self):
        """Clear all items from the filter"""
        self._filter.clear()

    @property
    def size(self) -> int:
        """Get number of items in filter"""
        return self._filter.size()

    @property
    def num_buckets(self) -> int:
        """Get number of buckets"""
        return self._filter.num_buckets()

    @property
    def load_factor(self) -> float:
        """Get load factor (0.0 to 1.0)"""
        return self._filter.load_factor()

    @property
    def memory_usage(self) -> int:
        """Get memory usage in bytes"""
        return self._filter.memory_usage()

    # Async operations
    async def add_async(self, str item) -> bool:
        """Async add item"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.add, item)

    async def remove_async(self, str item) -> bool:
        """Async remove item"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.remove, item)

    async def contains_async(self, str item) -> bool:
        """Async check membership"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.contains, item)


# ===== COUNT-MIN SKETCH =====

cdef class CyCountMinSketch:
    """
    High-performance Count-Min Sketch with C++ backend

    A probabilistic data structure for estimating frequencies of elements
    in a data stream with sub-linear space complexity.

    Example:
        cms = CyCountMinSketch(epsilon=0.01, delta=0.01)
        cms.add("item1", count=5)
        cms.add("item2", count=3)
        freq = cms.estimate("item1")  # Returns >= 5
    """
    cdef CountMinSketch* _sketch
    cdef object _executor

    def __cinit__(self, double epsilon=0.01, double delta=0.01):
        """
        Initialize Count-Min Sketch
        @param epsilon: Error rate (smaller = more accurate, more memory)
        @param delta: Confidence (1 - delta is probability of correct estimate)
        """
        self._sketch = new CountMinSketch(epsilon, delta)
        self._executor = ThreadPoolExecutor(max_workers=4)

    def __dealloc__(self):
        if self._sketch != NULL:
            del self._sketch
        if self._executor:
            self._executor.shutdown(wait=False)

    @property
    def executor(self):
        return self._executor

    def add(self, str item, unsigned int count=1):
        """Add an item with given count"""
        cdef string cpp_item = item.encode('utf-8')
        self._sketch.add(cpp_item, count)

    def estimate(self, str item) -> int:
        """Estimate frequency of an item (always >= actual)"""
        cdef string cpp_item = item.encode('utf-8')
        return self._sketch.estimate(cpp_item)

    def clear(self):
        """Clear all counts"""
        self._sketch.clear()

    @property
    def total_count(self) -> int:
        """Get total count of all items"""
        return self._sketch.total_count()

    @property
    def width(self) -> int:
        """Get width (number of columns)"""
        return self._sketch.width()

    @property
    def depth(self) -> int:
        """Get depth (number of hash functions)"""
        return self._sketch.depth()

    @property
    def memory_usage(self) -> int:
        """Get memory usage in bytes"""
        return self._sketch.memory_usage()

    def conservative_add(self, str item, unsigned int count=1):
        """Conservative update: only update if new value is larger"""
        cdef string cpp_item = item.encode('utf-8')
        self._sketch.conservative_add(cpp_item, count)

    # Async operations
    async def add_async(self, str item, unsigned int count=1):
        """Async add item"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self.executor, self.add, item, count)

    async def estimate_async(self, str item) -> int:
        """Async estimate frequency"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.estimate, item)


# ===== TOP-K (Python implementation using Count-Min Sketch) =====

cdef class CyTopK:
    """
    Track the K most frequent items in a stream

    Uses Count-Min Sketch for frequency estimation and a min-heap
    to track the top K items efficiently.

    Example:
        topk = CyTopK(k=10)
        topk.add("item1")
        topk.add("item2")
        topk.add("item1")  # item1 has freq 2
        top_items = topk.get_top_k()  # [(item1, 2), (item2, 1)]
    """
    cdef CyCountMinSketch _sketch
    cdef dict _heap_dict
    cdef int _k
    cdef object _executor

    def __init__(self, int k=10, double epsilon=0.001, double delta=0.01):
        """
        Initialize Top-K tracker
        @param k: Number of top items to track
        @param epsilon: Count-Min Sketch error rate
        @param delta: Count-Min Sketch confidence
        """
        self._sketch = CyCountMinSketch(epsilon, delta)
        self._heap_dict = {}
        self._k = k
        self._executor = ThreadPoolExecutor(max_workers=4)

    def add(self, str item):
        """Add an item to the stream"""
        # Update Count-Min Sketch
        self._sketch.add(item, 1)

        # Get estimated frequency
        cdef unsigned int freq = self._sketch.estimate(item)

        # Update heap
        self._heap_dict[item] = freq

        # Keep only top K
        if len(self._heap_dict) > self._k:
            # Remove item with minimum frequency
            min_item = min(self._heap_dict, key=self._heap_dict.get)
            del self._heap_dict[min_item]

    def get_top_k(self) -> List[Tuple[str, int]]:
        """Get top K items sorted by frequency (descending)"""
        return sorted(self._heap_dict.items(), key=lambda x: x[1], reverse=True)

    def clear(self):
        """Clear all items"""
        self._sketch.clear()
        self._heap_dict.clear()

    @property
    def k(self) -> int:
        """Get K value"""
        return self._k

    @property
    def size(self) -> int:
        """Get number of items being tracked"""
        return len(self._heap_dict)

    # Async operations
    async def add_async(self, str item):
        """Async add item"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self._executor, self.add, item)

    async def get_top_k_async(self) -> List[Tuple[str, int]]:
        """Async get top K items"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self.get_top_k)
