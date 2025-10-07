# Token Manager declarations

cdef class TokenManager:
    cdef object redis_client
    cdef str secret_key
    cdef int access_token_expiry
    cdef int refresh_token_expiry
    cdef str blacklist_key
    cdef str refresh_tokens_key

    cdef str _create_jwt_token(self, dict payload)
    cdef dict _decode_jwt_token(self, str token)
