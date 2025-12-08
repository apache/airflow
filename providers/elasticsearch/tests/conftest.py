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

import pytest
from elasticsearch import Elasticsearch
from testcontainers.elasticsearch import ElasticSearchContainer

pytest_plugins = "tests_common.pytest_plugin"


def _wait_for_cluster_ready(es: Elasticsearch, timeout_s: int = 120) -> None:
    es.cluster.health(wait_for_status="yellow", timeout=f"{timeout_s}s")


def _ensure_index(es: Elasticsearch, index: str, timeout_s: int = 120) -> None:
    if not es.indices.exists(index=index):
        es.indices.create(
            index=index,
            settings={
                "index": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                }
            },
        )
    # Wait until the index primary is active
    es.cluster.health(index=index, wait_for_status="yellow", timeout=f"{timeout_s}s")


@pytest.fixture(scope="session")
def es_8_container_url() -> str:
    es = (
        ElasticSearchContainer("docker.elastic.co/elasticsearch/elasticsearch:8.19.0")
        .with_env("discovery.type", "single-node")
        .with_env("cluster.routing.allocation.disk.threshold_enabled", "false")
    )
    with es:
        url = es.get_url()
        client = Elasticsearch(
            url,
            request_timeout=120,
            retry_on_timeout=True,
            max_retries=5,
        )
        _wait_for_cluster_ready(client, timeout_s=120)
        _ensure_index(client, "airflow-logs", timeout_s=120)
        yield url
