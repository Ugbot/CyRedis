"""PickleCoder must not be an RCE vector: payloads are HMAC-signed and the
signature is verified before unpickling, so cache-poisoned/forged blobs are
rejected.
"""
import pickle

import pytest

from cy_redis.web.web_cache import PickleCoder, JsonCoder


def test_pickle_coder_requires_secret_or_explicit_optin():
    with pytest.raises(ValueError):
        PickleCoder()  # neither secret_key nor allow_unsigned


def test_signed_round_trip():
    pc = PickleCoder(secret_key="server-secret")
    blob = pc.encode({"a": 1, "b": [1, 2, 3]})
    assert pc.decode(blob) == {"a": 1, "b": [1, 2, 3]}


def test_tampered_payload_is_rejected():
    pc = PickleCoder(secret_key="server-secret")
    blob = bytearray(pc.encode({"a": 1}))
    blob[-1] ^= 0xFF  # flip a byte in the pickled body
    with pytest.raises(ValueError):
        pc.decode(bytes(blob))


def test_foreign_unsigned_blob_is_rejected():
    """An attacker-written plain pickle (no valid HMAC) must not deserialize."""
    pc = PickleCoder(secret_key="server-secret")
    evil = pickle.dumps({"x": 1})
    with pytest.raises(ValueError):
        pc.decode(evil)


def test_wrong_secret_is_rejected():
    a = PickleCoder(secret_key="secret-a")
    b = PickleCoder(secret_key="secret-b")
    with pytest.raises(ValueError):
        b.decode(a.encode({"a": 1}))


def test_allow_unsigned_opt_in_round_trips():
    pu = PickleCoder(allow_unsigned=True)
    assert pu.decode(pu.encode([9, 8, 7])) == [9, 8, 7]


def test_json_coder_is_safe_default_round_trip():
    jc = JsonCoder()
    assert jc.decode(jc.encode({"k": "v"})) == {"k": "v"}
