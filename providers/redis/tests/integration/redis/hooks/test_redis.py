# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import json

import pytest
from redis.cluster import RedisCluster
from redis.exceptions import MovedError

from airflow.providers.redis.hooks.redis import RedisHook

CLUSTER_HOST = "redis-cluster"
CLUSTER_SEED_PORT = 7001
# Nothing listens here; used to prove `startup_nodes` is what establishes the connection.
CLUSTER_DEAD_PORT = 7009


@pytest.mark.integration("redis")
class TestRedisHook:
    def test_real_ping(self):
        hook = RedisHook(redis_conn_id="redis_default")
        redis = hook.get_conn()

        assert redis.ping(), "Connection to Redis with PING works."

    def test_real_get_and_set(self):
        hook = RedisHook(redis_conn_id="redis_default")
        redis = hook.get_conn()

        assert redis.set("test_key", "test_value"), "Connection to Redis with SET works."
        assert redis.get("test_key") == b"test_value", "Connection to Redis with GET works."
        assert redis.delete("test_key") == 1, "Connection to Redis with DELETE works."


@pytest.mark.integration("redis")
class TestRedisHookClusterMode:
    @pytest.fixture(autouse=True)
    def cluster_connections(self, monkeypatch):
        seed = {"conn_type": "redis", "host": CLUSTER_HOST, "port": CLUSTER_SEED_PORT}
        monkeypatch.setenv("AIRFLOW_CONN_REDIS_STANDALONE_TEST", json.dumps(seed))
        monkeypatch.setenv(
            "AIRFLOW_CONN_REDIS_CLUSTER_TEST", json.dumps({**seed, "extra": {"cluster": True}})
        )
        monkeypatch.setenv(
            "AIRFLOW_CONN_REDIS_CLUSTER_SEEDS_TEST",
            json.dumps(
                {
                    **seed,
                    "port": CLUSTER_DEAD_PORT,
                    "extra": {
                        "cluster": True,
                        "startup_nodes": f"{CLUSTER_HOST}:7002,{CLUSTER_HOST}:7003",
                    },
                }
            ),
        )

    def test_cluster_mode_follows_moved_redirect(self):
        """Both connections point at the same seed node; only the cluster client can reach the key."""
        cluster = RedisHook(redis_conn_id="redis_cluster_test").get_conn()
        assert isinstance(cluster, RedisCluster)

        remote_keys = [
            key
            for key in (f"cluster_key_{i}" for i in range(20))
            if cluster.get_node_from_key(key).port != CLUSTER_SEED_PORT
        ]
        assert remote_keys, "expected at least one key owned by a node other than the seed node"
        remote_key = remote_keys[0]

        standalone = RedisHook(redis_conn_id="redis_standalone_test").get_conn()
        with pytest.raises(MovedError):
            standalone.set(remote_key, "value")

        try:
            assert cluster.set(remote_key, "value")
            assert cluster.get(remote_key) == b"value"
        finally:
            cluster.delete(remote_key)

    def test_startup_nodes_connect_when_the_seed_node_is_unreachable(self):
        """The connection's own host/port is dead, so only `startup_nodes` can bootstrap it."""
        conn = RedisHook(redis_conn_id="redis_cluster_seeds_test").get_conn()

        try:
            assert conn.set("cluster_startup_nodes_key", "value")
            assert conn.get("cluster_startup_nodes_key") == b"value"
        finally:
            conn.delete("cluster_startup_nodes_key")
