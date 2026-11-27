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
"""
Contract tests between the Task SDK client and this tree's Execution API.

Each test sends the client's real requests to the real routes in process and asserts on what
the client returns. They cover client methods that interpret a response beyond parsing it,
such as mapping a status code to an error. Behaviour against older servers still needs the
mocked tests in ``task-sdk/tests/task_sdk/api/test_client.py``.
"""

from __future__ import annotations

import json
from datetime import timedelta
from uuid import uuid4

import pytest
import time_machine
from sqlalchemy import delete

from airflow._shared.timezones import timezone
from airflow.api_fastapi.execution_api.security import require_auth
from airflow.executors.workloads.callback import HEARTBEAT_SUPPORTED_KEY
from airflow.models.asset import AssetActive, AssetModel
from airflow.models.asset_state_store import AssetStateStoreModel
from airflow.models.callback import CallbackFetchMethod, CallbackState, ExecutorCallback
from airflow.models.connection import Connection
from airflow.models.task_state_store import TaskStateStoreModel
from airflow.models.variable import Variable
from airflow.models.xcom import XCOM_RETURN_KEY, XComModelV2
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.api.client import ServerResponseError
from airflow.sdk.api.datamodels._generated import (
    AssetStateStoreResponse,
    ConnectionResponse,
    TaskStateStoreResponse,
    VariableResponse,
    XComResponse,
    XComSequenceIndexResponse,
    XComSequenceSliceResponse,
)
from airflow.sdk.definitions.callback import SyncCallback
from airflow.sdk.exceptions import CallbackAlreadyStartedError, ErrorType
from airflow.sdk.execution_time.comms import ErrorResponse, XComCountResponse
from airflow.utils.session import create_session

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_connections, clear_db_variables

pytestmark = pytest.mark.db_test

MAPPED_VALUES = ["a", "b"]
SLASHED_KEY = "nested/key"


@conf_vars({("scheduler", "task_instance_heartbeat_timeout"): "30"})
@pytest.mark.parametrize("scheduler_enrolled", [True, False], ids=["new-scheduler", "old-scheduler"])
def test_deadline_callback_heartbeat_and_duplicate_delivery(
    task_sdk_client, in_process_execution_api, monkeypatch, session, scheduler_enrolled
):
    # The in-process transport normally supplies a task token; token scopes are tested on the HTTP routes.
    monkeypatch.setitem(in_process_execution_api.app.dependency_overrides, require_auth, lambda: None)
    callback = ExecutorCallback(SyncCallback("builtins.print"), CallbackFetchMethod.IMPORT_PATH)
    if scheduler_enrolled:
        callback.data[HEARTBEAT_SUPPORTED_KEY] = True
    callback.state = CallbackState.QUEUED
    session.add(callback)
    session.commit()
    try:
        with time_machine.travel(timezone.datetime(2026, 10, 22), tick=False) as clock:
            if not scheduler_enrolled:
                assert task_sdk_client.callbacks.run(callback.id) is None
                session.refresh(callback)
                assert callback.state_deadline is None
                with pytest.raises(ServerResponseError) as exc_info:
                    task_sdk_client.callbacks.run(callback.id)
                assert exc_info.value.response.status_code == 409
                return
            assert task_sdk_client.callbacks.run(callback.id) == 30
            session.refresh(callback)
            assert callback.state_deadline == timezone.utcnow() + timedelta(seconds=30)
            clock.shift(timedelta(seconds=10))
            with pytest.raises(CallbackAlreadyStartedError):
                task_sdk_client.callbacks.run(callback.id)
            session.refresh(callback)
            assert callback.state_deadline == timezone.utcnow() + timedelta(seconds=20)
            task_sdk_client.callbacks.heartbeat(callback.id, timeout=5)
            session.refresh(callback)
            assert callback.state_deadline == timezone.utcnow() + timedelta(seconds=30)
    finally:
        session.delete(callback)
        session.commit()


class TestXComOperations:
    @pytest.fixture(autouse=True)
    def dag_run(self, dag_maker, session):
        with dag_maker(dag_id="dag", serialized=True):
            EmptyOperator(task_id="plain")
            PythonOperator.partial(task_id="mapped", python_callable=str).expand(op_args=[[1], [2]])
        dag_run = dag_maker.create_dagrun(run_id="runid")
        for ti in dag_run.get_task_instances(session=session):
            value = "plain" if ti.task_id == "plain" else MAPPED_VALUES[ti.region_index]
            session.add(XComModelV2(key=XCOM_RETURN_KEY, value=value, task_instance_id=ti.id))
            if ti.task_id == "mapped":
                session.add(XComModelV2(key=SLASHED_KEY, value=value, task_instance_id=ti.id))
        session.commit()
        return dag_run

    @pytest.mark.parametrize(
        ("task_id", "map_index", "expected"),
        [
            pytest.param("mapped", 1, "b", id="mapped-one"),
            pytest.param("mapped", 2, None, id="mapped-past-end"),
            pytest.param("plain", None, "plain", id="unmapped-omitted"),
            pytest.param("plain", 0, None, id="unmapped-with-index"),
        ],
    )
    def test_get(self, task_sdk_client, task_id, map_index, expected):
        result = task_sdk_client.xcoms.get("dag", "runid", task_id, XCOM_RETURN_KEY, map_index)
        assert result == XComResponse(key=XCOM_RETURN_KEY, value=expected)

    @pytest.mark.parametrize("key", [XCOM_RETURN_KEY, SLASHED_KEY])
    def test_get_mapped_omitted(self, task_sdk_client, key):
        result = task_sdk_client.xcoms.get("dag", "runid", "mapped", key)
        assert result == XComSequenceSliceResponse(MAPPED_VALUES)

    def test_get_missing_key(self, task_sdk_client):
        result = task_sdk_client.xcoms.get("dag", "runid", "plain", "missing")
        assert result == XComResponse(key="missing", value=None)

    @pytest.mark.parametrize(
        ("offset", "expected"),
        [
            pytest.param(0, "a", id="first"),
            pytest.param(-1, "b", id="last"),
        ],
    )
    def test_get_sequence_item(self, task_sdk_client, offset, expected):
        result = task_sdk_client.xcoms.get_sequence_item("dag", "runid", "mapped", XCOM_RETURN_KEY, offset)
        assert result == XComSequenceIndexResponse(expected)

    @pytest.mark.parametrize(
        ("key", "offset"),
        [
            pytest.param(XCOM_RETURN_KEY, 2, id="out-of-range"),
            pytest.param("missing", 0, id="missing-key"),
        ],
    )
    def test_get_sequence_item_not_found(self, task_sdk_client, key, offset):
        result = task_sdk_client.xcoms.get_sequence_item("dag", "runid", "mapped", key, offset)
        assert result == ErrorResponse(
            error=ErrorType.XCOM_NOT_FOUND,
            detail={"dag_id": "dag", "run_id": "runid", "task_id": "mapped", "key": key, "offset": offset},
        )

    @pytest.mark.parametrize(
        ("key", "start", "expected"),
        [
            pytest.param(XCOM_RETURN_KEY, None, MAPPED_VALUES, id="all"),
            pytest.param(XCOM_RETURN_KEY, 1, MAPPED_VALUES[1:], id="from-one"),
            pytest.param("missing", None, [], id="missing-key"),
        ],
    )
    def test_get_sequence_slice(self, task_sdk_client, key, start, expected):
        result = task_sdk_client.xcoms.get_sequence_slice("dag", "runid", "mapped", key, start, None, None)
        assert result == XComSequenceSliceResponse(expected)

    @pytest.mark.parametrize(
        ("task_id", "key", "expected"),
        [
            pytest.param("mapped", XCOM_RETURN_KEY, len(MAPPED_VALUES), id="mapped"),
            pytest.param("plain", XCOM_RETURN_KEY, 1, id="unmapped"),
            pytest.param("mapped", "missing", 0, id="missing-key"),
        ],
    )
    def test_head(self, task_sdk_client, task_id, key, expected):
        result = task_sdk_client.xcoms.head("dag", "runid", task_id, key)
        assert result == XComCountResponse(len=expected)


class TestVariableOperations:
    @pytest.fixture(autouse=True)
    def _clear_variables(self):
        clear_db_variables()
        yield
        clear_db_variables()

    def test_get(self, task_sdk_client, session):
        session.add(Variable(key="var", val="value"))
        session.commit()
        result = task_sdk_client.variables.get("var")
        assert result == VariableResponse(key="var", value="value")

    def test_get_not_found(self, task_sdk_client):
        result = task_sdk_client.variables.get("missing")
        assert result == ErrorResponse(error=ErrorType.VARIABLE_NOT_FOUND, detail={"key": "missing"})


class TestConnectionOperations:
    @pytest.fixture(autouse=True)
    def _clear_connections(self):
        clear_db_connections(add_default_connections_back=False)
        yield
        clear_db_connections(add_default_connections_back=False)

    def test_get(self, task_sdk_client, session):
        session.add(Connection(conn_id="conn", conn_type="http", host="localhost", port=8080))
        session.commit()
        result = task_sdk_client.connections.get("conn")
        assert result == ConnectionResponse(
            conn_id="conn",
            conn_type="http",
            host="localhost",
            schema=None,
            login=None,
            password=None,
            port=8080,
            extra=None,
        )

    def test_get_not_found(self, task_sdk_client):
        result = task_sdk_client.connections.get("missing")
        assert result == ErrorResponse(error=ErrorType.CONNECTION_NOT_FOUND, detail={"conn_id": "missing"})


class TestTaskStateStoreOperations:
    @pytest.fixture(autouse=True)
    def ti(self, create_task_instance, session):
        session.execute(delete(TaskStateStoreModel))
        ti = create_task_instance(session=session)
        for key, value in [("job_id", "spark_001"), ("stored_null", None)]:
            session.add(
                TaskStateStoreModel(
                    dag_run_id=ti.dag_run.id,
                    dag_id=ti.dag_id,
                    run_id=ti.run_id,
                    task_id=ti.task_id,
                    region_id=ti.region_id,
                    region_index=ti.region_index,
                    key=key,
                    value=json.dumps(value),
                    updated_at=timezone.utcnow(),
                )
            )
        session.commit()
        yield ti
        with create_session() as cleanup:
            cleanup.execute(delete(TaskStateStoreModel))

    def test_get(self, task_sdk_client, ti):
        result = task_sdk_client.task_state_store.get(ti.id, "job_id")
        assert result == TaskStateStoreResponse(value="spark_001")

    @pytest.mark.parametrize("key", ["missing", "stored_null"])
    def test_get_not_found(self, task_sdk_client, ti, key):
        result = task_sdk_client.task_state_store.get(ti.id, key)
        assert result == ErrorResponse(error=ErrorType.TASK_STORE_NOT_FOUND, detail={"key": key})

    def test_get_missing_task_instance(self, task_sdk_client):
        """A task instance that doesn't exist reads the same as a missing key."""
        result = task_sdk_client.task_state_store.get(uuid4(), "job_id")
        assert result == ErrorResponse(error=ErrorType.TASK_STORE_NOT_FOUND, detail={"key": "job_id"})


class TestAssetStateStoreOperations:
    @pytest.fixture(autouse=True)
    def asset(self, session):
        session.execute(delete(AssetStateStoreModel))
        session.execute(delete(AssetModel))
        asset = AssetModel(name="asset", uri="s3://bucket/asset", group="asset")
        session.add(asset)
        session.flush()
        session.add(AssetActive.for_asset(asset))
        session.add(AssetStateStoreModel(asset_id=asset.id, key="watermark", value=json.dumps("2026-04-29")))
        session.commit()
        yield asset
        with create_session() as cleanup:
            cleanup.execute(delete(AssetStateStoreModel))
            cleanup.execute(delete(AssetModel))

    @pytest.mark.parametrize("by", ["name", "uri"])
    def test_get(self, task_sdk_client, asset, by):
        result = task_sdk_client.asset_state_store.get("watermark", **{by: getattr(asset, by)})
        assert result == AssetStateStoreResponse(value="2026-04-29")

    def test_get_not_found(self, task_sdk_client, asset):
        result = task_sdk_client.asset_state_store.get("missing", name=asset.name)
        assert result == ErrorResponse(error=ErrorType.ASSET_STORE_NOT_FOUND, detail={"key": "missing"})

    @pytest.mark.parametrize("by", [{"name": "missing"}, {"uri": "s3://bucket/missing"}])
    def test_get_missing_asset(self, task_sdk_client, by):
        """An asset that doesn't exist reads the same as a missing key."""
        result = task_sdk_client.asset_state_store.get("watermark", **by)
        assert result == ErrorResponse(error=ErrorType.ASSET_STORE_NOT_FOUND, detail={"key": "watermark"})
