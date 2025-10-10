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
from testcontainers.elasticsearch import ElasticSearchContainer

pytest_plugins = "tests_common.pytest_plugin"


@pytest.fixture(scope="session")
def es_8_container_url():
    # with ElasticSearchContainer(
    #     "docker.elastic.co/elasticsearch/elasticsearch:8.19.0",
    #     # mem_reservation="512m",
    #     mem_limit="512m",
    #     nano_cpus=2_000_000_000,  # 0.5 CPU; reduce to 100_000_000 for ~0.1 CPU and more slowdown
    # ) as es:
    es = (
        ElasticSearchContainer("docker.elastic.co/elasticsearch/elasticsearch:8.19.0", mem_reservation="512m")
        .with_env("discovery.type", "single-node")
        .with_env("xpack.security.enabled", "false")
        .with_env("xpack.ml.enabled", "false")
        .with_env("xpack.watcher.enabled", "false")
        .with_env("xpack.monitoring.collection.enabled", "false")
        .with_env("ingest.geoip.downloader.enabled", "false")
        .with_env("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
    )
    with es:
        yield es.get_url()
