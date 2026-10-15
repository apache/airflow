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

import tempfile
import textwrap
import time
from pathlib import Path
from unittest import mock
from uuid import uuid4

import pytest
import structlog
from sqlalchemy import delete, select

from airflow._shared.timezones import timezone
from airflow.api_fastapi.execution_api.in_process import InProcessExecutionAPI
from airflow.assets.manager import AssetManager
from airflow.callbacks.callback_requests import DagCallbackRequest
from airflow.dag_processing.dagbag import DagBag
from airflow.dag_processing.processor import DagFileProcessorProcess
from airflow.models.asset import AssetEvent, AssetModel, AssetPartitionDagRun
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, PartitionedAssetTimetable
from airflow.sdk.api.client import Client, ServerResponseError
from airflow.sdk.execution_time.comms import EmitAssetEvent
from airflow.utils.state import DagRunState, TaskInstanceState

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_assets

pytestmark = pytest.mark.db_test
DATE = timezone.datetime(2026, 10, 1)
ASSET = Asset(name="immediate", uri="example://immediate")


@pytest.fixture(autouse=True)
def clean_assets(session):
    clear_db_assets()
    session.execute(delete(AssetPartitionDagRun))
    session.commit()
    yield
    session.rollback()
    clear_db_assets()
    session.execute(delete(AssetPartitionDagRun))
    session.commit()


@pytest.fixture
def source(dag_maker, session):
    with dag_maker(dag_id="immediate_source", schedule=None):
        EmptyOperator(task_id="fail")
    run = dag_maker.create_dagrun(state=DagRunState.FAILED)
    ti = run.get_task_instance("fail", session=session)
    ti.state = TaskInstanceState.FAILED
    session.commit()
    with dag_maker(
        dag_id="immediate_consumer",
        schedule=PartitionedAssetTimetable(assets=ASSET),
        is_paused_upon_creation=False,
    ):
        EmptyOperator(task_id="consume")
    return run, ti


@pytest.fixture
def api_client():
    api = InProcessExecutionAPI()
    with Client(base_url="http://in-process.invalid/", token="", transport=api.transport) as client:
        yield client


@pytest.mark.parametrize("scope", ["task", "dag"])
@pytest.mark.parametrize("partition_key", [None, "eu", "2026-10-01"])
def test_emit_partition_and_provenance(api_client, source, session, scope, partition_key):
    run, ti = source
    kwargs = {}
    if scope == "task":
        api_client.headers["X-Airflow-In-Process-Attempt-Id"] = str(ti.id)
    else:
        kwargs = {"source_dag_id": run.dag_id, "source_run_id": run.run_id}
    receipt = api_client.asset_events.emit(
        EmitAssetEvent(
            name=ASSET.name,
            uri=ASSET.uri,
            extra={"rows": 42},
            partition_key=partition_key,
            partition_date=DATE if partition_key else None,
        ),
        **kwargs,
    )
    session.expire_all()
    event = session.get(AssetEvent, receipt.id)
    assert event.source_dag_id == run.dag_id
    assert event.source_run_id == run.run_id
    assert event.source_task_id == (ti.task_id if scope == "task" else None)
    assert event.source_task_instance_id == (ti.id if scope == "task" else None)
    assert event.source_map_index == (ti.map_index if scope == "task" else None)
    assert event.partition_key == partition_key
    assert event.extra == {"rows": 42}
    assert run.state == DagRunState.FAILED
    assert ti.state == TaskInstanceState.FAILED
    queued = session.scalars(select(AssetPartitionDagRun)).all()
    if partition_key:
        assert len(queued) == 1
        assert queued[0].partition_key == partition_key
        assert queued[0].partition_date == DATE
    else:
        assert not queued


@pytest.mark.parametrize(
    "partition",
    [
        {"partition_key": ""},
        {"partition_key": "  "},
        {"partition_key": "x" * 251},
        {"partition_date": "2026-10-01T00:00:00Z"},
        {"partition_key": "eu", "partition_date": "2026-10-01T00:00:00"},
    ],
)
def test_invalid_partition_has_no_side_effect(api_client, source, session, partition):
    _, ti = source
    api_client.headers["X-Airflow-In-Process-Attempt-Id"] = str(ti.id)
    with pytest.raises(ServerResponseError) as error:
        api_client.request(
            "POST", "asset-events/emit", json={"name": ASSET.name, "uri": ASSET.uri, **partition}, retry=False
        )
    assert error.value.response.status_code == 422
    assert session.scalar(select(AssetEvent.id)) is None


@conf_vars({("core", "multi_team"): "True"})
@mock.patch("airflow.assets.manager.AssetManager._filter_dags_by_team", autospec=True, return_value=set())
@mock.patch("airflow.models.dag.DagModel.get_team_name", autospec=True, return_value="producer-team")
def test_callback_resolves_producer_team(get_team, filter_dags, api_client, source):
    run, _ = source
    api_client.asset_events.emit(
        EmitAssetEvent(name=ASSET.name, uri=ASSET.uri), source_dag_id=run.dag_id, source_run_id=run.run_id
    )
    get_team.assert_called_once()
    assert get_team.call_args.args == (run.dag_id,)
    assert filter_dags.call_args.kwargs["source_teams"] == {"producer-team"}
    assert filter_dags.call_args.kwargs["source_is_api"] is False


def test_manager_rejects_ambiguous_source(source, session):
    run, ti = source
    with pytest.raises(ValueError, match="Specify task_instance or dag_run, not both"):
        AssetManager.register_asset_change(asset=ASSET, task_instance=ti, dag_run=run, session=session)
    assert session.scalar(select(AssetEvent.id)) is None


def test_worker_cannot_claim_dag_callback_source(api_client, source, session):
    run, ti = source
    api_client.headers["X-Airflow-In-Process-Attempt-Id"] = str(ti.id)
    with pytest.raises(ServerResponseError) as error:
        api_client.asset_events.emit(
            EmitAssetEvent(name=ASSET.name, uri=ASSET.uri), source_dag_id=run.dag_id, source_run_id=run.run_id
        )
    assert error.value.response.status_code == 403
    assert session.scalar(select(AssetEvent.id)) is None


def test_unknown_asset_is_not_created(api_client, source, session):
    _, ti = source
    api_client.headers["X-Airflow-In-Process-Attempt-Id"] = str(ti.id)
    with pytest.raises(ServerResponseError) as error:
        api_client.asset_events.emit(EmitAssetEvent(name=ASSET.name, uri="example://wrong"))
    assert error.value.response.status_code == 404
    assert session.scalar(select(AssetEvent.id)) is None
    assert session.scalar(select(AssetModel).where(AssetModel.uri == "example://wrong")) is None


@pytest.mark.parametrize(
    ("source_kwargs", "status"), [({}, 403), ({"source_dag_id": "missing", "source_run_id": "missing"}, 404)]
)
def test_emission_requires_existing_runtime_source(api_client, source, session, source_kwargs, status):
    with pytest.raises(ServerResponseError) as error:
        api_client.asset_events.emit(EmitAssetEvent(name=ASSET.name, uri=ASSET.uri), **source_kwargs)
    assert error.value.response.status_code == status
    assert session.scalar(select(AssetEvent.id)) is None


def test_examples_parse_without_emitting(session):
    path = Path(__file__).parents[3] / "src/airflow/example_dags/example_asset_emission.py"
    dagbag = DagBag(dag_folder=str(path))
    assert not dagbag.import_errors
    assert set(dagbag.dags) == {
        "example_immediate_asset_task",
        "example_immediate_asset_dag_callback",
        "example_immediate_asset_task_consumer",
        "example_immediate_asset_callback_consumer",
    }
    assert session.scalar(select(AssetEvent.id)) is None


def test_previous_api_version_has_no_emit_endpoint(api_client, source):
    _, ti = source
    api_client.headers["X-Airflow-In-Process-Attempt-Id"] = str(ti.id)
    api_client.headers["Airflow-API-Version"] = "2026-10-30"
    with pytest.raises(ServerResponseError) as error:
        api_client.asset_events.emit(EmitAssetEvent(name=ASSET.name, uri=ASSET.uri))
    assert error.value.response.status_code == 404


def test_supervised_dag_callback_without_task_instance(api_client, source, session, tmp_path):
    run, _ = source
    path = tmp_path / "emitter.py"
    path.write_text(
        textwrap.dedent(
            """
            from airflow.sdk import DAG, Asset
            from airflow.providers.standard.operators.empty import EmptyOperator
            from pendulum import datetime

            def on_failure(context):
                assert 'ti' not in context and 'task_instance' not in context
                receipt = Asset(name='immediate', uri='example://immediate').emit(
                    extra={'callback': True}, partition_key='2026-10-01',
                    partition_date=datetime(2026, 10, 1, tz='UTC'),
                )
                assert receipt.id > 0

            with DAG('immediate_source', schedule=None, on_failure_callback=on_failure):
                EmptyOperator(task_id='fail')
            """
        )
    )
    callback = DagCallbackRequest(
        filepath=str(path),
        dag_id=run.dag_id,
        run_id=run.run_id,
        bundle_name="testing",
        bundle_version=None,
        context_from_server=None,
        is_failure_callback=True,
        msg="Deliberate failure",
    )
    with tempfile.TemporaryFile() as logs:
        process = DagFileProcessorProcess.start(
            id=uuid4(),
            path=path,
            bundle_path=tmp_path,
            bundle_name="testing",
            dag_file_rel_path=path.name,
            callbacks=[callback],
            client=api_client,
            logger=structlog.get_logger(),
            logger_filehandle=logs,
        )
        deadline = time.monotonic() + 30
        try:
            while not process.is_ready and time.monotonic() < deadline:
                process._service_subprocess(0.1)
            assert process.is_ready
        finally:
            if not process.is_ready:
                process.kill(force=True)
    session.expire_all()
    event = session.scalars(select(AssetEvent)).one()
    assert event.source_dag_id == run.dag_id
    assert event.source_run_id == run.run_id
    assert event.source_task_id is None
    assert event.source_task_instance_id is None
    assert event.partition_key == "2026-10-01"
    queued = session.scalars(select(AssetPartitionDagRun)).one()
    assert queued.partition_date == DATE
    assert run.state == DagRunState.FAILED
