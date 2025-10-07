# cython: language_level=3
# distutils: language=c

"""
Header declarations for MessagingCore
"""

from libc.stdint cimport uint64_t

# Forward declarations
cdef class CythonReliableQueue
cdef class CythonStreamProcessor
cdef class CythonPubSubHub

# C-level structures
cdef struct QueueMessage:
    char *id
    char *data
    int priority
    uint64_t created_at
    uint64_t delay_until
    int retry_count
    int state

cdef struct StreamMessage:
    char *id
    char **fields
    char **values
    size_t num_fields
    uint64_t timestamp

cdef struct PubSubMessage:
    char *channel
    char *message
    char *pattern
    uint64_t timestamp

# Class declarations
cdef class CythonReliableQueue:
    cdef:
        void *redis_client  # AsyncRedisClient*
        char *queue_name
        char *pending_key
        char *processing_key
        char *failed_key
        char *dead_key
        int visibility_timeout
        int max_retries
        void *message_queue  # AsyncMessageQueue*

    cdef int push_message(self, const char *message_data, int priority=*, int delay=*) nogil
    cdef char* _serialize_queue_message(self, QueueMessage *msg) nogil
    cdef QueueMessage* _deserialize_queue_message(self, const char *data) nogil

cdef class CythonStreamProcessor:
    cdef:
        void *redis_client  # AsyncRedisClient*
        char *stream_name
        void *stream_queue  # AsyncMessageQueue*
        uint64_t last_id
        int batch_size

    cdef int add_to_stream(self, const char *data, const char *message_id=*, int maxlen=*) nogil
    cdef int read_from_stream(self, uint64_t start_id=*, int count=*) nogil

cdef class CythonPubSubHub:
    cdef:
        void *redis_client  # AsyncRedisClient*
        void *pubsub_queue  # AsyncMessageQueue*
        void *subscriptions

    cdef int publish_message(self, const char *channel, const char *message) nogil
