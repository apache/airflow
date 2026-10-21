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

from datetime import timedelta

import pytest
from uuid6 import uuid7

from airflow.api_fastapi.execution_api.in_process import InProcessExecutionAPI
from airflow.sdk.api.client import Client
from airflow.sdk.exceptions import WebhookWaitUnsupported

pytestmark = pytest.mark.db_test

REGISTER_URL = "/execution/inbound-waits/00000000-0000-0000-0000-000000000000"
WAIT = {"webhook": "unlisted", "timeout": "PT1H"}


class TestInboundWaitEndpointVersioning:
    """The inbound-waits endpoint didn't exist before the 2027-02-28 API version."""

    def test_old_version_returns_404(self, client):
        """Before 2027-02-28 the route is absent, so routing itself 404s, without a reason."""
        client.headers["Airflow-API-Version"] = "2026-10-30"

        response = client.post(REGISTER_URL, json=WAIT)

        assert response.status_code == 404
        assert response.json() == {"detail": "Not Found"}

    def test_head_version_routes_to_endpoint(self, client):
        """At head the route exists: the same request reaches the endpoint, whose 404 has a reason."""
        response = client.post(REGISTER_URL, json=WAIT)

        assert response.status_code == 404
        assert response.json()["detail"]["reason"] == "webhook_not_found"

    def test_sdk_client_raises_webhook_wait_unsupported_on_old_version(self):
        """The Task SDK reads the old version's routing 404 as a server without webhook waits."""
        api = InProcessExecutionAPI()
        with Client(base_url="http://in-process.invalid./", token="", transport=api.transport) as client:
            client.headers["Airflow-API-Version"] = "2026-10-30"

            with pytest.raises(WebhookWaitUnsupported):
                client.inbound_waits.register(uuid7(), webhook="unlisted", timeout=timedelta(hours=1))
