# cython: language_level=3
# distutils: language=c

"""
Minimal test for hiredis integration
"""

cdef extern from "hiredis/hiredis.h":
    ctypedef struct redisContext:
        int err
        char errstr[128]

    ctypedef struct redisReply:
        int type
        long long integer
        size_t len
        char *str
        size_t elements
        redisReply **element

    redisContext *redisConnect(const char *ip, int port)
    void redisFree(redisContext *c)
    redisReply *redisCommand(redisContext *c, const char *format, ...)
    void freeReplyObject(redisReply *reply)

def test_connection():
    """Test basic hiredis connection"""
    cdef redisContext *ctx = redisConnect("127.0.0.1", 6379)
    if ctx == NULL or ctx.err:
        if ctx:
            print(f"Connection error: {ctx.errstr.decode('utf-8')}")
            redisFree(ctx)
        else:
            print("Connection failed: NULL context")
        return False

    # Try a simple command
    cdef redisReply *reply = redisCommand(ctx, "PING")
    if reply == NULL:
        print(f"Command error: {ctx.errstr.decode('utf-8')}")
        redisFree(ctx)
        return False

    try:
        if reply.type == 5:  # REDIS_REPLY_STATUS
            result = reply.str.decode('utf-8') if reply.str else ""
            print(f"PING result: {result}")
            return result == "PONG"
        else:
            print(f"Unexpected reply type: {reply.type}")
            return False
    finally:
        freeReplyObject(reply)
        redisFree(ctx)
