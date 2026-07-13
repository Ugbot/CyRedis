# cython: language_level=3

"""TLS support for CyRedis connections, backed by hiredis_ssl + OpenSSL.

This module is compiled and linked against ``libhiredis_ssl.a`` and OpenSSL
only when OpenSSL development headers are present at build time (setup.py
drops the extension otherwise). The core client imports it lazily the first
time a connection is created with ``use_tls=True``, so installs built without
OpenSSL still work for plain-TCP connections and fail with a clear error only
when TLS is actually requested.

The redisSSLContext is wrapped in a PyCapsule whose destructor frees it, so
context lifetime is tied to the owning connection object with no manual
cleanup path to forget.
"""

from cpython.pycapsule cimport (
    PyCapsule_Destructor,
    PyCapsule_GetPointer,
    PyCapsule_IsValid,
    PyCapsule_New,
)


cdef extern from "hiredis.h":
    ctypedef struct redisContext:
        int err
        char errstr[128]


cdef extern from "hiredis_ssl.h":
    ctypedef struct redisSSLContext
    ctypedef enum redisSSLContextError:
        REDIS_SSL_CTX_NONE

    int redisInitOpenSSL()
    const char *redisSSLContextGetError(redisSSLContextError error)
    redisSSLContext *redisCreateSSLContext(
        const char *cacert_filename, const char *capath,
        const char *cert_filename, const char *private_key_filename,
        const char *server_name, redisSSLContextError *error)
    void redisFreeSSLContext(redisSSLContext *redis_ssl_ctx)
    int redisInitiateSSLWithContext(redisContext *c, redisSSLContext *redis_ssl_ctx)


# The capsule name doubles as a type check: initiate_tls refuses capsules
# that did not come from create_ssl_context.
cdef const char *_CAPSULE_NAME = b"cy_redis.core.tls_support.redisSSLContext"

# hiredis requires OpenSSL global init exactly once per process, before any
# SSL context is created.
redisInitOpenSSL()


cdef void _free_ssl_context_capsule(object capsule) noexcept:
    cdef redisSSLContext *ssl_ctx
    if PyCapsule_IsValid(capsule, _CAPSULE_NAME):
        ssl_ctx = <redisSSLContext *>PyCapsule_GetPointer(capsule, _CAPSULE_NAME)
        if ssl_ctx != NULL:
            redisFreeSSLContext(ssl_ctx)


def create_ssl_context(ca_certs=None, ca_path=None, certfile=None,
                       keyfile=None, server_name=None):
    """Create a redisSSLContext and return it wrapped in a PyCapsule.

    All arguments are optional file-system paths (str). With no CA arguments
    the system certificate store is used. ``certfile``/``keyfile`` enable
    mutual TLS and must be given together. ``server_name`` sets SNI.

    Raises ConnectionError with hiredis's own diagnostic if the context
    cannot be created (missing file, bad key, cert/key mismatch, ...).
    """
    # Keep the encoded bytes objects alive across the C call.
    cdef bytes ca_b = ca_certs.encode() if ca_certs else None
    cdef bytes capath_b = ca_path.encode() if ca_path else None
    cdef bytes cert_b = certfile.encode() if certfile else None
    cdef bytes key_b = keyfile.encode() if keyfile else None
    cdef bytes sni_b = server_name.encode() if server_name else None

    cdef redisSSLContextError err = REDIS_SSL_CTX_NONE
    cdef redisSSLContext *ssl_ctx = redisCreateSSLContext(
        <const char *>ca_b if ca_b is not None else NULL,
        <const char *>capath_b if capath_b is not None else NULL,
        <const char *>cert_b if cert_b is not None else NULL,
        <const char *>key_b if key_b is not None else NULL,
        <const char *>sni_b if sni_b is not None else NULL,
        &err)
    cdef str reason
    if ssl_ctx == NULL:
        reason = redisSSLContextGetError(err).decode('utf-8', errors='replace')
        raise ConnectionError(f"Failed to create TLS context: {reason}")
    return PyCapsule_New(<void *>ssl_ctx, _CAPSULE_NAME,
                         <PyCapsule_Destructor>_free_ssl_context_capsule)


def initiate_tls(size_t ctx_addr, object ssl_context_capsule):
    """Upgrade an already-connected redisContext to TLS.

    ``ctx_addr`` is the address of a live redisContext (owned by the calling
    CyRedisConnection — this module never frees it). Raises ConnectionError
    with the handshake diagnostic on failure; the redisContext is left in an
    errored state and the caller must tear it down.
    """
    assert ctx_addr != 0, "initiate_tls on NULL redisContext"
    if not PyCapsule_IsValid(ssl_context_capsule, _CAPSULE_NAME):
        raise TypeError("ssl_context_capsule is not a cy-redis TLS context")

    cdef redisContext *ctx = <redisContext *><void *>ctx_addr
    cdef redisSSLContext *ssl_ctx = <redisSSLContext *>PyCapsule_GetPointer(
        ssl_context_capsule, _CAPSULE_NAME)
    cdef bytes err_bytes
    cdef str reason
    if redisInitiateSSLWithContext(ctx, ssl_ctx) != 0:
        if ctx.err:
            err_bytes = ctx.errstr  # char[128] → bytes up to the first NUL
            reason = err_bytes.decode('utf-8', errors='replace')
        else:
            reason = "unknown handshake error"
        raise ConnectionError(f"TLS handshake failed: {reason}")
