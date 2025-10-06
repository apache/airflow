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

import time

import pytest
from elasticsearch import Elasticsearch
from testcontainers.elasticsearch import ElasticSearchContainer


@pytest.fixture(scope="session")
def es_8_container_url():
    with ElasticSearchContainer("docker.elastic.co/elasticsearch/elasticsearch:8.19.0") as es:
        url = es.get_url()

        client = Elasticsearch(url, request_timeout=60, retry_on_timeout=True, max_retries=5)
        client.cluster.health(wait_for_status="yellow", timeout="60s")
        client.indices.create(
            index="airflow-logs",
            settings={"index": {"number_of_shards": 1, "number_of_replicas": 0}},
        )

        end = time.time() + 120
        attempt, last = 0, None
        while time.time() < end:
            try:
                client.index(index="airflow-logs", id=f"probe-{attempt}", document={"ok": True, "i": attempt})
                client.indices.refresh(index="airflow-logs", request_timeout=30, ignore_unavailable=True)
                break
            except Exception as e:
                last = e
                attempt += 1
                time.sleep(min(0.25 * attempt, 3.0))
        else:
            raise RuntimeError(f"Elasticsearch write readiness failed: {last}")

        yield url


pytest_plugins = "tests_common.pytest_plugin"
