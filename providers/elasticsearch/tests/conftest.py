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
        client = Elasticsearch(
            url,
            request_timeout=30,
            retry_on_timeout=True,
            max_retries=5,
        )
        # Give the node time to be ready. In CI this can take a while.
        deadline = time.time() + 120  # up to 2 minutes on slow runners
        last_err = None
        while time.time() < deadline:
            try:
                # Wait until at least yellow (all primaries assigned)
                client.cluster.health(wait_for_status="yellow", timeout="5s")
                break
            except Exception as e:
                last_err = e
                time.sleep(1)
        else:
            raise RuntimeError(f"Elasticsearch did not become ready: {last_err}")

        yield url


pytest_plugins = "tests_common.pytest_plugin"
