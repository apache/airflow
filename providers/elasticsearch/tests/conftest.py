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

_container = None


class EarlyEnvPlugin:
    def pytest_sessionstart(self, session):
        global _container

        _container = ElasticSearchContainer("docker.elastic.co/elasticsearch/elasticsearch:8.19.0")
        _container.start()

        es_host = _container.get_container_host_ip()
        es_port = _container.get_exposed_port(_container.port)
        es_url = f"http://{es_host}:{es_port}"

        session.config._es_url = es_url

    def pytest_sessionfinish(self, session, exitstatus):
        global _container
        if _container:
            _container.stop()


def pytest_configure(config):
    plugin = EarlyEnvPlugin()
    config.pluginmanager.register(plugin, name="early_env_plugin")


@pytest.fixture(scope="session")
def elasticsearch_8_url(request):
    """Provides ES URL and client after early bootstrapping."""
    return request.config._es_url


pytest_plugins = "tests_common.pytest_plugin"
