# Password Reset Manager declarations

cdef class PasswordResetManager:
    cdef object redis_client
    cdef int token_expiry
    cdef str tokens_key
