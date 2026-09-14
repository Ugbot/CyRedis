#!/usr/bin/env python3
"""
Redis Cluster with the native CyRedisCluster client.

Needs a running cluster; the six-node layout used by CI works:

    for port in 7000 7001 7002 7003 7004 7005; do
      docker run -d --name cluster-$port --network host redis:7 \
        redis-server --port $port --cluster-enabled yes \
        --cluster-config-file /data/$port.conf --appendonly no
    done
    docker run --rm --network host redis:7 redis-cli --cluster create \
      127.0.0.1:700{0,1,2,3,4,5} --cluster-replicas 1 --cluster-yes

    REDIS_CLUSTER_NODES=127.0.0.1:7000 uv run python examples/cluster_aware_demo.py
"""

import os
from collections import Counter

from cy_redis import CyRedisCluster
from cy_redis.core.cluster import key_slot

NODES = os.environ.get("REDIS_CLUSTER_NODES", "127.0.0.1:7000").split(",")


def banner(title: str) -> None:
    print(f"\n{title}\n{'=' * len(title)}")


def demo_topology(cluster: CyRedisCluster) -> None:
    banner("Topology")
    info = cluster.cluster_info()
    print(f"state        : {info.get('cluster_state')}")
    print(f"slots ok     : {info.get('cluster_slots_ok')}")
    print(f"masters      : {cluster.nodes}")
    replicas = [
        f"{node['host']}:{node['port']}"
        for node in cluster.cluster_nodes().values()
        if not node["master"]
    ]
    print(f"replicas     : {sorted(replicas)}")


def demo_key_routing(cluster: CyRedisCluster) -> None:
    banner("Key routing")
    keys = [f"user:{i}" for i in range(12)]
    for key in keys[:3]:
        print(f"{key:10} -> slot {key_slot(key):5} on {cluster.node_for_key(key)}")

    distribution = Counter(cluster.node_for_key(key) for key in keys)
    print(f"12 keys over masters: {dict(distribution)}")

    # Hash tags pin related keys to the same slot so multi-key commands work.
    tagged = ["{order:42}:items", "{order:42}:total", "{order:42}:status"]
    slots = {key_slot(key) for key in tagged}
    print(f"hash-tagged keys share slot {slots}")


def demo_cross_slot_commands(cluster: CyRedisCluster) -> None:
    banner("Multi-key commands across slots")
    mapping = {f"demo:cluster:{i}": str(i * i) for i in range(20)}
    cluster.mset(mapping)  # fanned out per slot, one MSET per bucket
    values = cluster.mget(list(mapping))
    print(
        f"MSET/MGET of 20 keys spanning {len(cluster.nodes)} masters: {values[:5]}..."
    )
    print(f"EXISTS over all 20: {cluster.exists(*mapping)}")
    print(f"DEL   over all 20: {cluster.delete(*mapping)}")


def demo_pipeline(cluster: CyRedisCluster) -> None:
    banner("Cluster pipeline")
    with cluster.pipeline() as pipe:
        for i in range(30):
            pipe.set(f"demo:pipe:{i}", str(i))
        for i in range(0, 30, 10):
            pipe.get(f"demo:pipe:{i}")
        results = pipe.execute()
    print(f"33 commands grouped by owning node; sampled gets = {results[-3:]}")
    cluster.delete(*[f"demo:pipe:{i}" for i in range(30)])


def demo_raw_commands(cluster: CyRedisCluster) -> None:
    banner("Raw commands follow MOVED/ASK redirects")
    cluster.execute_command("HSET", "demo:profile", "name", "ada", "lang", "cython")
    print(f"HGETALL -> {cluster.execute_command('HGETALL', 'demo:profile')}")
    slot = cluster.cluster_keyslot("demo:profile")
    print(
        f"CLUSTER KEYSLOT demo:profile = {slot}; keys in slot: "
        f"{cluster.cluster_countkeysinslot(slot)}"
    )
    cluster.delete("demo:profile")


def main() -> None:
    with CyRedisCluster(nodes=NODES) as cluster:
        demo_topology(cluster)
        demo_key_routing(cluster)
        demo_cross_slot_commands(cluster)
        demo_pipeline(cluster)
        demo_raw_commands(cluster)


if __name__ == "__main__":
    main()
