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

import json
import tempfile
import textwrap
import time
from pathlib import Path
from unittest import mock
from uuid import uuid4

import pytest
import structlog
from sqlalchemy import delete, func, select
from sqlalchemy.exc import OperationalError

from airflow._shared.timezones import timezone
from airflow.api_fastapi.execution_api.in_process import InProcessExecutionAPI
from airflow.api_fastapi.execution_api.routes import asset_events as emission_route
from airflow.assets.manager import AssetManager
from airflow.callbacks.callback_requests import DagCallbackRequest
from airflow.dag_processing.dagbag import DagBag
from airflow.dag_processing.processor import DagFileProcessorProcess
from airflow.models.asset import AssetEvent, AssetModel, AssetPartitionDagRun, TaskOutletAssetReference
from airflow.models.dagrun import DagRun
from airflow.models.deadline import Deadline
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, AssetAccessControl, PartitionedAssetTimetable
from airflow.sdk.api.client import Client, ServerResponseError
from airflow.sdk.definitions.callback import SyncCallback
from airflow.sdk.execution_time.comms import EmitAssetEvent
from airflow.utils.session import create_session
from airflow.utils.state import CallbackState, DagRunState, TaskInstanceState

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_assets, clear_db_deadline

pytestmark = pytest.mark.db_test
DATE = timezone.datetime(2026, 10, 1)
ASSET = Asset(name="immediate", uri="example://immediate")


def deadline_callback(**kwargs):
    """Stands for a Deadline Alert callback; only its import path is stored."""


@pytest.fixture(autouse=True)
def clean_assets(session):
    clear_db_assets()
    clear_db_deadline()
    session.execute(delete(AssetPartitionDagRun))
    session.commit()
    yield
    session.rollback()
    clear_db_assets()
    clear_db_deadline()
    session.execute(delete(AssetPartitionDagRun))
    session.commit()


@pytest.fixture
def source(dag_maker, session):
    with dag_maker(dag_id="immediate_source", schedule=None):
        EmptyOperator(task_id="fail", retries=2)
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


def _message(key: str = "key-1", **kwargs) -> EmitAssetEvent:
    return EmitAssetEvent(name=ASSET.name, uri=ASSET.uri, idempotency_key=key, **kwargs)


def _emit_as(api_client: Client, attempt_id, message: EmitAssetEvent | None = None):
    api_client.headers["X-Airflow-In-Process-Attempt-Id"] = str(attempt_id)
    return api_client.asset_events.emit(message or _message())


def _emission_error(api_client: Client, attempt_id, message: EmitAssetEvent | None = None):
    with pytest.raises(ServerResponseError) as error:
        _emit_as(api_client, attempt_id, message)
    return error.value.response.status_code, error.value.response.json()["detail"]


def _event_count(session) -> int:
    session.expire_all()
    return session.scalar(select(func.count()).select_from(AssetEvent))


@pytest.mark.parametrize("scope", ["task", "dag"])
@pytest.mark.parametrize("partition_key", [None, "eu", "2026-10-01"])
def test_emit_partition_and_provenance(api_client, source, session, scope, partition_key):
    run, ti = source
    message = _message(extra={"rows": 42}, partition_key=partition_key)
    if scope == "task":
        receipt = _emit_as(api_client, ti.id, message)
    else:
        receipt = api_client.asset_events.emit(message, source_dag_id=run.dag_id, source_run_id=run.run_id)
    assert receipt.replayed is False
    session.expire_all()
    event = session.get(AssetEvent, receipt.id)
    assert event.source_dag_id == run.dag_id
    assert event.source_run_id == run.run_id
    assert event.source_task_id == (ti.task_id if scope == "task" else None)
    assert event.source_task_instance_id == (ti.id if scope == "task" else None)
    assert event.source_map_index == (ti.map_index if scope == "task" else None)
    assert event.partition_key == partition_key
    assert event.extra == {"rows": 42}
    assert event.origin == "emission"
    assert event.idempotency_key == "key-1"
    assert run.state == DagRunState.FAILED
    assert ti.state == TaskInstanceState.FAILED
    queued = session.scalars(select(AssetPartitionDagRun)).all()
    if partition_key:
        assert [(row.partition_key, row.partition_date) for row in queued] == [(partition_key, None)]
    else:
        assert not queued


@pytest.mark.parametrize(
    "body",
    [
        pytest.param({"partition_key": ""}, id="empty-partition"),
        pytest.param({"partition_key": "  "}, id="blank-partition"),
        pytest.param({"partition_key": "x" * 251}, id="long-partition"),
        pytest.param({"partition_date": "2026-10-01T00:00:00Z"}, id="partition-date"),
        pytest.param({"idempotency_key": None}, id="no-key"),
        pytest.param({"idempotency_key": ""}, id="empty-key"),
        pytest.param({"name": None, "uri": None}, id="no-asset"),
    ],
)
def test_invalid_request_has_no_side_effect(api_client, source, session, body):
    _, ti = source
    api_client.headers["X-Airflow-In-Process-Attempt-Id"] = str(ti.id)
    payload = {"name": ASSET.name, "uri": ASSET.uri, "idempotency_key": "key-1", **body}
    with pytest.raises(ServerResponseError) as error:
        api_client.request("POST", "asset-events/emit", json=payload, retry=False)
    assert error.value.response.status_code == 422
    assert _event_count(session) == 0


@pytest.mark.parametrize("reference", [{"name": ASSET.name}, {"uri": ASSET.uri}], ids=["by-name", "by-uri"])
def test_asset_reference(api_client, source, session, reference):
    _, ti = source
    receipt = _emit_as(api_client, ti.id, EmitAssetEvent(idempotency_key="key-1", **reference))
    session.expire_all()
    asset_id = session.scalar(select(AssetModel.id).where(AssetModel.uri == ASSET.uri))
    assert session.get(AssetEvent, receipt.id).asset_id == asset_id


def test_unknown_and_inactive_assets_are_errors(api_client, source, session):
    _, ti = source
    session.add(AssetModel(name="orphan", uri="example://orphan"))
    session.commit()

    unknown = EmitAssetEvent(name=ASSET.name, uri="example://wrong", idempotency_key="k")
    assert _emission_error(api_client, ti.id, unknown) == (
        404,
        {"reason": "asset_not_found", "message": "The asset is not in the Airflow database"},
    )
    inactive = EmitAssetEvent(name="orphan", uri="example://orphan", idempotency_key="k")
    assert _emission_error(api_client, ti.id, inactive)[1]["reason"] == "asset_inactive"
    assert _event_count(session) == 0
    assert session.scalar(select(AssetModel).where(AssetModel.uri == "example://wrong")) is None


def test_repeat_requests(api_client, source, session):
    run, ti = source
    first = _emit_as(api_client, ti.id, _message(extra={"rows": 1}))

    replayed = _emit_as(api_client, ti.id, _message(extra={"rows": 1}))
    assert (replayed.id, replayed.replayed) == (first.id, True)

    status, detail = _emission_error(api_client, ti.id, _message(extra={"rows": 2}))
    assert (status, detail["reason"], detail["event_id"]) == (409, "idempotency_conflict", first.id)

    ti.state = TaskInstanceState.UP_FOR_RETRY
    ti.end_date = timezone.utcnow()
    retry = ti.prepare_db_for_next_try(session)
    session.commit()
    from_retry = _emit_as(api_client, retry.id, _message(extra={"rows": 1}))
    assert (from_retry.id, from_retry.replayed) == (first.id, True)

    run.clear_number += 1
    session.merge(run)
    session.commit()
    after_clear = _emit_as(api_client, retry.id, _message(extra={"rows": 2}))
    assert after_clear.id != first.id
    assert after_clear.replayed is False
    assert _event_count(session) == 2


def test_concurrent_repeat_returns_the_first_receipt(api_client, source, session):
    _, ti = source
    asset_id = session.scalar(select(AssetModel.id).where(AssetModel.uri == ASSET.uri))
    first: list[int] = []
    budget_check = emission_route._require_emission_budget

    def commit_the_same_key_first(emission_source, *, session):
        # Stands for a second request that passed the existence check too and committed first.
        budget_check(emission_source, session=session)
        with create_session() as other_session:
            event = AssetEvent(
                asset_id=asset_id,
                origin="emission",
                idempotency_key="key-1",
                scope_hash=emission_route._compute_hash(
                    [emission_route.EMISSION_ORIGIN, asset_id, *emission_source.scope, "key-1"]
                ),
                payload_hash=emission_route._compute_hash(
                    {"asset_id": asset_id, "extra": {}, "partition_key": None}
                ),
            )
            other_session.add(event)
            other_session.flush()
            first.append(event.id)

    with mock.patch.object(
        emission_route, "_require_emission_budget", autospec=True, side_effect=commit_the_same_key_first
    ):
        receipt = _emit_as(api_client, ti.id)
    assert (receipt.id, receipt.replayed) == (first[0], True)
    assert _event_count(session) == 1


@pytest.mark.parametrize("reason", ["retry", "superseded"])
def test_archived_attempt(api_client, source, session, reason):
    _, ti = source
    ti.state = TaskInstanceState.UP_FOR_RETRY
    ti.end_date = timezone.utcnow()
    if reason == "retry":
        ti.prepare_db_for_next_try(session)
    else:
        ti.archive(reason=reason, session=session)
    session.commit()

    assert _emission_error(api_client, ti.id)[1]["reason"] == "attempt_archived"
    assert _event_count(session) == 0


def test_each_region_has_its_own_scope(api_client, source, session):
    _, ti = source
    other_region = TaskInstance(
        ti.task, run_id=ti.run_id, map_index=ti.map_index, region_id=uuid4(), dag_version_id=ti.dag_version_id
    )
    session.add(other_region)
    session.commit()

    first = _emit_as(api_client, ti.id)
    second = _emit_as(api_client, other_region.id)
    assert second.id != first.id
    assert second.replayed is False


def test_extra_must_be_strict_json(api_client, source, session):
    _, ti = source
    api_client.headers["X-Airflow-In-Process-Attempt-Id"] = str(ti.id)
    # The SDK refuses NaN before it sends; a raw request tests the server's own check.
    body = json.dumps(
        {"name": ASSET.name, "uri": ASSET.uri, "idempotency_key": "k", "extra": {"rows": 1}}
    ).replace('"rows": 1', '"rows": NaN')
    with pytest.raises(ServerResponseError) as error:
        api_client.request(
            "POST",
            "asset-events/emit",
            content=body,
            headers={"content-type": "application/json"},
            retry=False,
        )
    assert error.value.response.status_code == 422
    assert _event_count(session) == 0


@mock.patch.object(emission_route, "MAX_EMISSION_EXTRA_BYTES", 10)
def test_extra_size_limit(api_client, source, session):
    _, ti = source
    status, detail = _emission_error(api_client, ti.id, _message(extra={"rows": "x" * 20}))
    assert (status, detail["reason"]) == (413, "extra_too_large")
    assert _event_count(session) == 0


@mock.patch.object(emission_route, "MAX_EMISSIONS_PER_SOURCE", 1)
def test_emission_limit(api_client, source, session):
    _, ti = source
    _emit_as(api_client, ti.id, _message("first"))
    status, detail = _emission_error(api_client, ti.id, _message("second"))
    assert (status, detail["reason"]) == (429, "emission_limit")
    assert _event_count(session) == 1


@pytest.mark.backend("postgres", "mysql")
@pytest.mark.parametrize("scope", ["task", "dag"])
def test_budget_check_holds_the_source_lock(api_client, source, scope):
    run, ti = source
    if scope == "task":
        source_row = select(TaskInstance.id).where(TaskInstance.id == ti.id)
    else:
        source_row = select(DagRun.id).where(DagRun.id == run.id)
    locked: list[bool] = []
    budget_check = emission_route._require_emission_budget

    def try_to_lock_the_source(emission_source, *, session):
        with create_session() as other_session:
            try:
                other_session.execute(source_row.with_for_update(nowait=True))
            except OperationalError:
                other_session.rollback()
                locked.append(True)
            else:
                locked.append(False)
        budget_check(emission_source, session=session)

    with mock.patch.object(
        emission_route, "_require_emission_budget", autospec=True, side_effect=try_to_lock_the_source
    ):
        if scope == "task":
            _emit_as(api_client, ti.id)
        else:
            api_client.asset_events.emit(_message(), source_dag_id=run.dag_id, source_run_id=run.run_id)
    assert locked == [True]


def test_listeners_run_after_commit(api_client, source):
    _, ti = source
    visible: list[int] = []

    def count_committed_events(asset_event):
        with create_session() as other_session:
            visible.append(other_session.scalar(select(func.count()).select_from(AssetEvent)))

    with mock.patch.object(
        AssetManager, "nofity_asset_event_emitted", autospec=True, side_effect=count_committed_events
    ):
        _emit_as(api_client, ti.id)
    assert visible == [1]


@conf_vars({("core", "multi_team"): "True"})
def test_multi_team_requires_the_emission_list(api_client, source, session):
    _, ti = source
    status, detail = _emission_error(api_client, ti.id)
    assert (status, detail["reason"]) == (403, "asset_not_in_emission_list")

    asset_id = session.scalar(select(AssetModel.id).where(AssetModel.uri == ASSET.uri))
    session.add(TaskOutletAssetReference(asset_id=asset_id, dag_id=ti.dag_id, task_id=ti.task_id))
    session.commit()
    assert _emit_as(api_client, ti.id).replayed is False


@conf_vars({("core", "multi_team"): "True"})
def test_emission_list_declares_the_producer(api_client, dag_maker, session):
    with dag_maker(dag_id="declared_producer", schedule=None):
        EmptyOperator(task_id="produce", emits=[ASSET])
    run = dag_maker.create_dagrun()
    ti = run.get_task_instance("produce", session=session)
    session.commit()

    serialized_task = dag_maker.serialized_dag.get_task("produce")
    assert [asset.uri for asset in serialized_task.emits] == [ASSET.uri]
    assert [asset.uri for asset in serialized_task.outlets] == []
    references = session.scalars(
        select(TaskOutletAssetReference).where(TaskOutletAssetReference.dag_id == "declared_producer")
    ).all()
    assert [reference.task_id for reference in references] == ["produce"]
    assert _emit_as(api_client, ti.id).replayed is False


@pytest.mark.parametrize(("state", "status"), [(CallbackState.RUNNING, 201), (CallbackState.SUCCESS, 410)])
def test_executor_callback_emits_for_its_dag_run(api_client, source, session, state, status):
    run, _ = source
    deadline = Deadline(
        deadline_time=DATE,
        callback=SyncCallback(deadline_callback),
        dagrun_id=run.id,
        deadline_alert_id=None,
        dag_id=run.dag_id,
    )
    session.add(deadline)
    session.flush()
    deadline.callback.state = state
    session.commit()

    if status == 201:
        receipt = _emit_as(api_client, deadline.callback_id)
        session.expire_all()
        event = session.get(AssetEvent, receipt.id)
        assert (event.source_dag_id, event.source_run_id, event.source_task_id) == (
            run.dag_id,
            run.run_id,
            None,
        )
    else:
        assert _emission_error(api_client, deadline.callback_id)[1]["reason"] == "callback_finished"


@conf_vars({("core", "multi_team"): "True"})
def test_multi_team_rejects_dag_callback_emission(api_client, source, session):
    run, _ = source
    with pytest.raises(ServerResponseError) as error:
        api_client.asset_events.emit(_message(), source_dag_id=run.dag_id, source_run_id=run.run_id)
    assert error.value.response.status_code == 403
    assert error.value.response.json()["detail"]["reason"] == "asset_not_in_emission_list"
    assert _event_count(session) == 0


@conf_vars({("core", "multi_team"): "True"})
@mock.patch("airflow.assets.manager.AssetManager._filter_dags_by_team", autospec=True, return_value=set())
@mock.patch("airflow.models.dag.DagModel.get_team_name", autospec=True, return_value="producer-team")
def test_dag_run_source_reaches_only_its_own_team(get_team, filter_dags, source, session):
    run, _ = source
    AssetManager.register_asset_change(asset=ASSET, dag_run=run, session=session)
    get_team.assert_called_once()
    assert get_team.call_args.args == (run.dag_id,)
    assert filter_dags.call_args.kwargs["source_teams"] == {"producer-team"}
    assert filter_dags.call_args.kwargs["source_is_api"] is False
    assert filter_dags.call_args.kwargs["allow_consumer_teams"] == []
    assert filter_dags.call_args.kwargs["allow_global_consumers"] is False


@pytest.mark.parametrize(
    ("outlet_access", "emits_access", "expected"),
    [
        pytest.param(
            AssetAccessControl(consumer_teams=["a", "b"]),
            AssetAccessControl(),
            (["a", "b"], True),
            id="emits-cannot-widen",
        ),
        pytest.param(
            AssetAccessControl(),
            AssetAccessControl(consumer_teams=["a"], allow_global=False),
            (["a"], False),
            id="emits-narrows",
        ),
        pytest.param(
            AssetAccessControl(consumer_teams=["a", "b"]),
            AssetAccessControl(consumer_teams=["b", "c"]),
            (["b"], True),
            id="teams-intersect",
        ),
    ],
)
def test_outlet_and_emission_list_keep_the_strictest_policy(
    dag_maker, session, outlet_access, emits_access, expected
):
    def declare(access_control):
        return Asset(name=ASSET.name, uri=ASSET.uri, access_control=access_control)

    with dag_maker(dag_id="shared_reference", schedule=None):
        EmptyOperator(task_id="produce", outlets=[declare(outlet_access)], emits=[declare(emits_access)])
    session.expire_all()
    reference = session.scalars(
        select(TaskOutletAssetReference).where(TaskOutletAssetReference.dag_id == "shared_reference")
    ).one()
    assert (reference.allow_consumer_teams, reference.allow_global_consumers) == expected


def test_manager_rejects_ambiguous_source(source, session):
    run, ti = source
    with pytest.raises(ValueError, match="Specify task_instance or dag_run, not both"):
        AssetManager.register_asset_change(asset=ASSET, task_instance=ti, dag_run=run, session=session)
    assert _event_count(session) == 0


def test_worker_cannot_claim_dag_callback_source(api_client, source, session):
    run, ti = source
    api_client.headers["X-Airflow-In-Process-Attempt-Id"] = str(ti.id)
    with pytest.raises(ServerResponseError) as error:
        api_client.asset_events.emit(_message(), source_dag_id=run.dag_id, source_run_id=run.run_id)
    assert error.value.response.status_code == 403
    assert _event_count(session) == 0


@pytest.mark.parametrize(
    ("source_kwargs", "status"), [({}, 403), ({"source_dag_id": "missing", "source_run_id": "missing"}, 404)]
)
def test_emission_requires_existing_runtime_source(api_client, source, session, source_kwargs, status):
    with pytest.raises(ServerResponseError) as error:
        api_client.asset_events.emit(_message(), **source_kwargs)
    assert error.value.response.status_code == status
    assert _event_count(session) == 0


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
    assert _event_count(session) == 0


def test_previous_api_version_has_no_emit_endpoint(api_client, source):
    _, ti = source
    api_client.headers["X-Airflow-In-Process-Attempt-Id"] = str(ti.id)
    api_client.headers["Airflow-API-Version"] = "2026-10-30"
    with pytest.raises(ServerResponseError) as error:
        api_client.asset_events.emit(_message())
    assert error.value.response.status_code == 404


def test_supervised_dag_callback_without_task_instance(api_client, source, session, tmp_path):
    run, _ = source
    path = tmp_path / "emitter.py"
    path.write_text(
        textwrap.dedent(
            """
            from airflow.sdk import DAG, Asset, emit_asset_event
            from airflow.providers.standard.operators.empty import EmptyOperator

            def on_failure(context):
                assert 'ti' not in context and 'task_instance' not in context
                receipt = emit_asset_event(
                    Asset(name='immediate', uri='example://immediate'),
                    extra={'callback': True},
                    partition_key='2026-10-01',
                    idempotency_key='incident',
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
    assert event.idempotency_key == "incident"
    assert session.scalars(select(AssetPartitionDagRun.partition_key)).one() == "2026-10-01"
    assert run.state == DagRunState.FAILED
