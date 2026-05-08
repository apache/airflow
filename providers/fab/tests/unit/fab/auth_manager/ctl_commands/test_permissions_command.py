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
from unittest.mock import MagicMock

import pytest

from airflow.providers.fab.auth_manager.ctl_commands import permissions_command


@pytest.fixture
def fake_client():
    client = MagicMock(name="api_client")

    def _resp(payload):
        r = MagicMock()
        r.json.return_value = payload
        return r

    client._resp = _resp
    return client


def _ns(**kwargs):
    return Namespace(**kwargs)


class TestListPermissions:
    def test_calls_api_with_pagination(self, fake_client):
        fake_client.get.return_value = fake_client._resp(
            {"actions": [{"action": {"name": "can_read"}, "resource": {"name": "Dag"}}]}
        )
        permissions_command.list_permissions(
            _ns(limit=10, offset=0, order_by="id", output="json"),
            api_client=fake_client,
        )
        fake_client.get.assert_called_once_with(
            "/fab/v1/permissions", params={"limit": 10, "offset": 0, "order_by": "id"}
        )

    def test_falls_back_to_permissions_key(self, fake_client):
        # Older payload shape, just in case the route serialisation alias changes.
        fake_client.get.return_value = fake_client._resp({"permissions": [{"x": 1}]})
        # Should not raise.
        permissions_command.list_permissions(
            _ns(limit=10, offset=0, order_by="id", output="json"),
            api_client=fake_client,
        )
