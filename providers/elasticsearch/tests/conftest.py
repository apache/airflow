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
def es_8_container_url() -> str:
    es = (
        ElasticSearchContainer("docker.elastic.co/elasticsearch/elasticsearch:8.19.0")
        .with_env("discovery.type", "single-node")
        .with_env("cluster.routing.allocation.disk.threshold_enabled", "false")
    )
    with es:
        yield es.get_url()
