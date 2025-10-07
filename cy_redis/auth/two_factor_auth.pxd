# Two Factor Authentication declarations

cdef class TwoFactorAuth:
    cdef object redis_client
    cdef str backup_codes_key
    cdef str totp_secrets_key

    cdef bint _verify_totp_token(self, str secret, str token)
    cdef str _generate_qr_url(self, str user_id, str secret)
