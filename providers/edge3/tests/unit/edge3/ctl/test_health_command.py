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

from argparse import Namespace
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from airflow.providers.edge3.ctl import health_command


@pytest.fixture
def fake_client():
    """API client with a base_url that mimics ``ClientKind.AUTH``."""
    client = MagicMock(name="api_client")
    # The real client carries an ``httpx.URL``; a SimpleNamespace mimics the
    # ``.scheme`` / ``.netloc`` access pattern without dragging httpx in.
    client.base_url = SimpleNamespace(scheme="http", netloc="localhost:8080")

    def _resp(payload):
        r = MagicMock()
        r.json.return_value = payload
        return r

    client._resp = _resp
    return client


class TestHealth:
    def test_targets_absolute_edge_worker_health_url(self, fake_client):
        fake_client.get.return_value = fake_client._resp({"status": "healthy"})
        health_command.health(Namespace(output="json"), api_client=fake_client)
        fake_client.get.assert_called_once_with("http://localhost:8080/edge_worker/v1/health")

    def test_handles_bytes_scheme_and_netloc(self, fake_client):
        # httpx's URL exposes scheme/netloc as bytes in some paths; the helper
        # decodes them. This is the regression guard for that path.
        fake_client.base_url = SimpleNamespace(scheme=b"https", netloc=b"airflow.example.com:443")
        fake_client.get.return_value = fake_client._resp({"status": "healthy"})
        health_command.health(Namespace(output="json"), api_client=fake_client)
        fake_client.get.assert_called_once_with("https://airflow.example.com:443/edge_worker/v1/health")
