"""Module command behaviour shared by Redis and Valkey.

Redis Stack and valkey-bundle load different builds of the JSON and search
modules, so these tests run against whichever server is configured and skip
the families it does not carry. Point ``REDIS_PORT`` at a module-bearing
server (redis-stack-server or valkey-bundle) to exercise them.
"""

import struct

import pytest

from cy_redis.core.cy_redis_client import CyRedisClient
from cy_redis.features.capabilities import ModuleUnavailableError, module_names
from cy_redis.features.json_ops import CyRedisJSON
from cy_redis.features.search import CyRedisSearch
from tests.server_env import REDIS_HOST, REDIS_PORT

JSON_MODULES = {"rejson", "json"}
SEARCH_MODULES = {"search", "searchlight"}


@pytest.fixture(scope="module")
def loaded_modules():
    client = CyRedisClient(host=REDIS_HOST, port=REDIS_PORT)
    conn = client.pool.get_connection()
    try:
        return module_names(conn)
    finally:
        client.pool.return_connection(conn)


@pytest.fixture
def json_client(loaded_modules):
    if not JSON_MODULES & loaded_modules:
        pytest.skip("server has no JSON module")
    return CyRedisJSON(host=REDIS_HOST, port=REDIS_PORT)


@pytest.fixture
def search_client(loaded_modules):
    if not SEARCH_MODULES & loaded_modules:
        pytest.skip("server has no search module")
    return CyRedisSearch(host=REDIS_HOST, port=REDIS_PORT)


def vector(values):
    return struct.pack("%sf" % len(values), *values)


@pytest.mark.redis
def test_numeric_ops_answer_with_a_number_for_both_path_syntaxes(json_client):
    """JSONPath answers with a one-element array, legacy paths with a scalar."""
    json_client.json_set("parity:num", "$", {"a": 1})

    assert json_client.json_numincrby("parity:num", "$.a", 2) == 3.0
    assert json_client.json_nummultby("parity:num", "$.a", 2) == 6.0
    assert json_client.json_numincrby("parity:num", ".a", 1) == 7.0


@pytest.mark.redis
def test_numeric_ops_on_an_unmatched_path_answer_none(json_client):
    json_client.json_set("parity:nomatch", "$", {"a": 1})

    assert json_client.json_numincrby("parity:nomatch", "$.missing", 1) is None


@pytest.mark.redis
def test_vector_index_and_knn_query(search_client):
    """The one search feature both RediSearch and valkey-search implement."""
    index = "parity:vidx"
    try:
        search_client.ft_dropindex(index)
    except Exception:
        pass

    search_client.ft_create(
        index,
        [
            ("tag", "TAG", {}),
            (
                "vec",
                "VECTOR",
                {
                    "algorithm": "HNSW",
                    "type": "FLOAT32",
                    "dim": 3,
                    "distance_metric": "COSINE",
                },
            ),
        ],
        on="HASH",
        prefix=["parity:doc:"],
    )

    client = CyRedisClient(host=REDIS_HOST, port=REDIS_PORT)
    for i, values in enumerate([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.9, 0.1, 0.0]]):
        client.execute_command(
            ["HSET", "parity:doc:%d" % i, "tag", "t", "vec", vector(values)]
        )

    try:
        result = search_client.ft_search(
            index,
            "*=>[KNN 2 @vec $q AS score]",
            params={"q": vector([1.0, 0.0, 0.0])},
            dialect=2,
            return_fields=["tag", "score"],
        )
        assert result["total"] == 2
    finally:
        search_client.ft_dropindex(index)
        for i in range(3):
            client.execute_command(["DEL", "parity:doc:%d" % i])


@pytest.mark.redis
def test_absent_module_commands_name_the_module_that_provides_them(loaded_modules):
    if JSON_MODULES & loaded_modules:
        pytest.skip("server has a JSON module")

    json_client = CyRedisJSON(host=REDIS_HOST, port=REDIS_PORT)
    with pytest.raises(ModuleUnavailableError) as raised:
        json_client.json_set("parity:absent", "$", {"a": 1})

    assert raised.value.command == "JSON.SET"
    assert "JSON documents" in str(raised.value)


@pytest.mark.redis
def test_search_commands_valkey_lacks_report_the_gap(loaded_modules):
    if not SEARCH_MODULES & loaded_modules:
        pytest.skip("server has no search module")

    search_client = CyRedisSearch(host=REDIS_HOST, port=REDIS_PORT)
    try:
        search_client.ft_sugadd("parity:sug", "hello", 1.0)
    except ModuleUnavailableError as error:
        assert "valkey-search" in str(error)
    else:
        search_client.ft_sugdel("parity:sug", "hello")
