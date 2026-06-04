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
from unittest.mock import MagicMock, create_autospec, patch

import httpx
import pytest
from airflowctl.api.client import ServerResponseError
from airflowctl.api.operations import DagsOperations, PoolsOperations
from airflowctl.exceptions import AirflowCtlConnectionException

from airflow.api.client.local_client import Client
from airflow.exceptions import AirflowBadRequest, PoolNotFound


def _server_error(status_code: int, detail: str) -> ServerResponseError:
    """Build a ServerResponseError with the given status and detail."""
    req = httpx.Request("GET", "http://localhost:8080/api/v2/test")
    res = httpx.Response(status_code, json={"detail": detail}, request=req)
    return ServerResponseError(message=f"{status_code}: {detail}", request=req, response=res)


@patch.object(Client, "_create_api_token", return_value="test-token")
def test_get_pool_uses_airflowctl_client(_mock_token):
    client = Client()
    pools_api = create_autospec(PoolsOperations, instance=True)
    pools_api.get.return_value = SimpleNamespace(
        name="default_pool", slots=128, description="Default pool", include_deferred=False
    )

    result = client.get_pool("default_pool", api_client=SimpleNamespace(pools=pools_api))

    pools_api.get.assert_called_once_with("default_pool")
    assert result == ("default_pool", 128, "Default pool", False)


@patch.object(Client, "_create_api_token", return_value="test-token")
def test_get_pool_raises_pool_not_found_on_404(_mock_token):
    client = Client()
    pools_api = MagicMock()
    pools_api.get.side_effect = _server_error(404, "Pool not found")

    with pytest.raises(PoolNotFound):
        client.get_pool("missing", api_client=SimpleNamespace(pools=pools_api))


@patch.object(Client, "_create_api_token", return_value="test-token")
def test_get_pool_raises_bad_request_on_other_server_error(_mock_token):
    client = Client()
    pools_api = MagicMock()
    pools_api.get.side_effect = _server_error(500, "Internal error")

    with pytest.raises(AirflowBadRequest):
        client.get_pool("mypool", api_client=SimpleNamespace(pools=pools_api))


@patch.object(Client, "_create_api_token", return_value="test-token")
def test_get_pools_uses_airflowctl_client(_mock_token):
    client = Client()
    pools_api = create_autospec(PoolsOperations, instance=True)
    pools_api.list.return_value = SimpleNamespace(
        pools=[
            SimpleNamespace(
                name="default_pool", slots=128, description="Default pool", include_deferred=False
            ),
            SimpleNamespace(name="foo", slots=2, description="Test pool", include_deferred=True),
        ]
    )

    result = client.get_pools(api_client=SimpleNamespace(pools=pools_api))

    pools_api.list.assert_called_once_with()
    assert result == [
        ("default_pool", 128, "Default pool", False),
        ("foo", 2, "Test pool", True),
    ]


@patch.object(Client, "_create_api_token", return_value="test-token")
def test_create_pool_uses_airflowctl_client(_mock_token):
    client = Client()
    pools_api = create_autospec(PoolsOperations, instance=True)
    pools_api.create.return_value = SimpleNamespace(name="mypool", slots=5, description="my pool")

    result = client.create_pool(
        name="mypool",
        slots=5,
        description="my pool",
        include_deferred=False,
        api_client=SimpleNamespace(pools=pools_api),
    )

    assert result == ("mypool", 5, "my pool")


@patch.object(Client, "_create_api_token", return_value="test-token")
def test_create_pool_raises_bad_request_on_server_error(_mock_token):
    client = Client()
    pools_api = MagicMock()
    pools_api.create.side_effect = _server_error(422, "Invalid body")

    with pytest.raises(AirflowBadRequest):
        client.create_pool(
            name="mypool",
            slots=5,
            description="x",
            include_deferred=False,
            api_client=SimpleNamespace(pools=pools_api),
        )


@patch.object(Client, "_create_api_token", return_value="test-token")
def test_delete_pool_raises_pool_not_found_on_404(_mock_token):
    client = Client()
    pools_api = MagicMock()
    pools_api.delete.side_effect = _server_error(404, "Pool not found")

    with pytest.raises(PoolNotFound):
        client.delete_pool("missing", api_client=SimpleNamespace(pools=pools_api))


@patch.object(Client, "_create_api_token", return_value="test-token")
def test_delete_dag_uses_airflowctl_client(_mock_token):
    client = Client()
    dags_api = create_autospec(DagsOperations, instance=True)

    result = client.delete_dag("my_dag", api_client=SimpleNamespace(dags=dags_api))

    dags_api.delete.assert_called_once_with("my_dag")
    assert result == "Deleted DAG my_dag"


@patch.object(Client, "_create_api_token", return_value="test-token")
def test_delete_dag_raises_bad_request_on_server_error(_mock_token):
    client = Client()
    dags_api = MagicMock()
    dags_api.delete.side_effect = _server_error(400, "Dag has active runs")

    with pytest.raises(AirflowBadRequest, match="Dag has active runs"):
        client.delete_dag("my_dag", api_client=SimpleNamespace(dags=dags_api))


@patch.object(Client, "_create_api_token", return_value="test-token")
def test_trigger_dag_uses_airflowctl_client(_mock_token):
    client = Client()
    dags_api = MagicMock()
    fake_dag_run = MagicMock()
    fake_dag_run.model_dump.return_value = {"dag_id": "my_dag", "dag_run_id": "run1", "state": "queued"}
    dags_api.trigger.return_value = fake_dag_run

    result = client.trigger_dag("my_dag", run_id="run1", api_client=SimpleNamespace(dags=dags_api))

    dags_api.trigger.assert_called_once()
    assert result == {"dag_id": "my_dag", "dag_run_id": "run1", "state": "queued"}


@patch("airflow.api.client.local_client.trigger_dag.trigger_dag")
@patch.object(Client, "_create_api_token", return_value="test-token")
def test_trigger_dag_serializes_local_fallback_response(_mock_token, mock_trigger_dag):
    client = Client()
    dags_api = MagicMock()
    dags_api.trigger.side_effect = AirflowCtlConnectionException("unavailable")
    mock_trigger_dag.return_value = SimpleNamespace(
        conf={"key": "value"},
        dag_id="my_dag",
        run_id="run1",
        data_interval_start=None,
        data_interval_end=None,
        end_date=None,
        last_scheduling_decision=None,
        logical_date=None,
        run_type="manual",
        start_date=None,
        state="queued",
        triggering_user_name="test-user",
    )

    result = client.trigger_dag("my_dag", run_id="run1", api_client=SimpleNamespace(dags=dags_api))

    assert result == {
        "conf": {"key": "value"},
        "dag_id": "my_dag",
        "dag_run_id": "run1",
        "data_interval_start": None,
        "data_interval_end": None,
        "end_date": None,
        "last_scheduling_decision": None,
        "logical_date": None,
        "run_type": "manual",
        "start_date": None,
        "state": "queued",
        "triggering_user_name": "test-user",
    }


@patch.object(Client, "_create_api_token", return_value="test-token")
def test_trigger_dag_raises_value_error_on_invalid_json_conf(_mock_token):
    client = Client()
    dags_api = MagicMock()

    with pytest.raises(ValueError, match="Expecting value"):
        client.trigger_dag("my_dag", conf="not-valid-json{", api_client=SimpleNamespace(dags=dags_api))
