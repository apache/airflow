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

from types import SimpleNamespace
from unittest.mock import create_autospec, patch

from airflowctl.api.operations import PoolsOperations

from airflow.api.client.local_client import Client
from airflow.models.pool import Pool


@patch.object(Client, "_create_api_token", return_value="test-token")
def test_get_pool_uses_airflowctl_client(_mock_create_api_token):
    client = Client()
    pools_api = create_autospec(PoolsOperations, instance=True)
    pools_api.get.return_value = SimpleNamespace(
        name="default_pool",
        slots=128,
        description="Default pool",
        include_deferred=False,
    )
    api_client = SimpleNamespace(pools=pools_api)

    result = client.get_pool("default_pool", api_client=api_client)

    pools_api.get.assert_called_once_with("default_pool")
    assert result == ("default_pool", 128, "Default pool", False)


@patch.object(Client, "_create_api_token", return_value="test-token")
def test_get_pools_uses_airflowctl_client(_mock_create_api_token):
    client = Client()
    pools_api = create_autospec(PoolsOperations, instance=True)
    pools_api.list.return_value = SimpleNamespace(
        pools=[
            SimpleNamespace(
                name="default_pool",
                slots=128,
                description="Default pool",
                include_deferred=False,
            ),
            SimpleNamespace(
                name="foo",
                slots=2,
                description="Test pool",
                include_deferred=True,
            ),
        ]
    )
    api_client = SimpleNamespace(pools=pools_api)

    result = client.get_pools(api_client=api_client)

    pools_api.list.assert_called_once_with()
    assert result == [
        ("default_pool", 128, "Default pool", False),
        ("foo", 2, "Test pool", True),
    ]


@patch.object(Client, "_create_api_token", return_value="test-token")
@patch.object(Pool, "get_pools")
def test_get_pools_falls_back_to_local_pool_model(mock_get_pools, _mock_create_api_token):
    client = Client()
    mock_get_pools.return_value = [
        SimpleNamespace(pool="default_pool", slots=128, description="Default pool", include_deferred=False),
        SimpleNamespace(pool="foo", slots=2, description="Test pool", include_deferred=True),
    ]

    result = client.get_pools(api_client=None)

    mock_get_pools.assert_called_once_with()
    assert result == [
        ("default_pool", 128, "Default pool", False),
        ("foo", 2, "Test pool", True),
    ]
