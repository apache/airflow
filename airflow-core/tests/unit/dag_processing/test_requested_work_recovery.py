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

from pathlib import Path
from unittest import mock

import httpx2
import pytest
from uuid6 import uuid7

from airflow._shared.timezones import timezone
from airflow.api_fastapi.execution_api.datamodels.dag_parsing import (
    DagBundleInventoryResponse,
    DagBundleStateResponse,
    DagParseResultBody,
    ProcessorWorkItem,
)
from airflow.api_fastapi.execution_api.datamodels.taskinstance import TaskInstance
from airflow.callbacks.callback_requests import DagCallbackRequest, TaskCallbackRequest
from airflow.dag_processing.api_client import DagProcessorAPIClient
from airflow.dag_processing.bundles.base import BaseDagBundle, DagBundleRefreshError
from airflow.dag_processing.bundles.local import LocalDagBundle
from airflow.dag_processing.manager import (
    DagFileInfo,
    DagFileProcessorManager,
    DeferredCallback,
    PendingDagPublication,
    PriorityParseClaim,
)
from airflow.dag_processing.processor import DagFileProcessorProcess, DagParseSource

from tests_common.test_utils.config import conf_vars


def _make_work(bundle_name, relative_fileloc="a.py", *, callback=None):
    return ProcessorWorkItem(
        id=str(uuid7()),
        claim_id=uuid7(),
        bundle_name=bundle_name,
        relative_fileloc=relative_fileloc,
        callback=callback.model_dump_json() if callback is not None else None,
    )


@pytest.fixture
def api_manager(tmp_path):
    ready_path = tmp_path / "ready"
    ready_path.mkdir()
    bundle = LocalDagBundle(name="ready", path=str(ready_path), refresh_interval=0)
    bundle.is_initialized = True
    client = mock.create_autospec(DagProcessorAPIClient, instance=True)
    client.restart_required = False
    client.claim_work.return_value = []
    client.get_bundles.return_value = [
        DagBundleStateResponse(
            name=bundle.name, version=None, last_refreshed=None, revision=None, team_name=None
        )
    ]
    client.publish_inventory.side_effect = lambda name, body: DagBundleInventoryResponse(
        attempt_id=body.attempt_id, revision=uuid7(), accepted_at=timezone.utcnow()
    )
    manager = DagFileProcessorManager(
        max_runs=1, api_client=client, parallelism=2, bundle_refresh_check_interval=0
    )
    manager._dag_bundles = [bundle]
    manager._bundle_parse_sources[bundle.name] = DagParseSource()
    return manager, client, bundle


@pytest.mark.parametrize("bundle_version", [None, "v1"])
@mock.patch.object(DagFileProcessorProcess, "start", autospec=True)
@mock.patch.object(DagFileProcessorManager, "_get_logger_for_dag_file", autospec=True)
@mock.patch.object(DagFileProcessorManager, "prepare_callback_bundle", autospec=True)
def test_callback_queued_behind_running_callback_dispatches_and_acknowledges(
    prepare, get_logger, start, api_manager, bundle_version
):
    manager, client, bundle = api_manager
    prepare.return_value = bundle
    get_logger.return_value = (mock.Mock(spec=[]), mock.Mock(spec=[]))

    def start_process(**kwargs):
        process = mock.create_autospec(DagFileProcessorProcess, instance=True)
        process.id = kwargs["id"]
        process.had_callbacks = True
        process.api_request_error = None
        process.parse_source = kwargs["parse_source"]
        return process

    start.side_effect = start_process
    requests = [
        DagCallbackRequest(
            filepath="a.py", bundle_name=bundle.name, bundle_version=bundle_version, dag_id="dag", run_id=run
        )
        for run in ("first", "second")
    ]
    work = [_make_work(bundle.name, callback=request) for request in requests]
    for item in work:
        client.claim_work.return_value = [item]
        for request in manager.fetch_callbacks():
            manager._add_callback_to_queue(request)
        manager._start_new_processes()

    file, first = next(iter(manager._processors.items()))
    assert start.call_count == 1
    assert list(manager._file_queue) == [file]
    assert [request.run_id for request in manager._callback_to_execute[file]] == ["second"]
    assert list(manager._callback_claims.values()) == [work[1]]

    manager._processors.pop(file)
    manager._finish_callback_attempt(first)
    manager._start_new_processes()
    second = manager._processors.pop(file)
    manager._finish_callback_attempt(second)
    manager._acknowledge_requested_work()

    assert [call.kwargs["callbacks"][0].run_id for call in start.call_args_list] == ["first", "second"]
    assert client.acknowledge_work.call_args_list == [mock.call("callbacks", item) for item in work]
    assert not manager._callback_claims
    assert not manager._callback_attempts
    assert not manager._callback_to_execute
    assert not manager._file_queue


@mock.patch.object(DagFileProcessorManager, "_create_process", autospec=True)
def test_queued_callback_preserves_db_priority_request_behind_running_parse(create, api_manager):
    manager, _, bundle = api_manager
    manager.api_client = None
    file = DagFileInfo(bundle_name=bundle.name, bundle_path=bundle.path, rel_path=Path("a.py"))
    running = mock.create_autospec(DagFileProcessorProcess, instance=True)
    manager._processors[file] = running
    manager._add_files_to_queue([file], mode="frontprio")
    request = DagCallbackRequest(
        filepath="a.py", bundle_name=bundle.name, bundle_version=None, dag_id="dag", run_id="run"
    )
    manager._add_callback_to_queue(request)

    def create_process(_, file, *, source):
        process = mock.create_autospec(DagFileProcessorProcess, instance=True)
        process.id = uuid7()
        process.had_callbacks = bool(manager._callback_to_execute.pop(file, []))
        process.parse_source = source
        return process

    create.side_effect = create_process
    manager._start_new_processes()
    create.assert_not_called()
    assert file in manager._file_queue
    assert file in manager._priority_files

    del manager._processors[file]
    manager._start_new_processes()
    assert manager._processors[file].had_callbacks
    assert file in manager._priority_files
    assert file in manager._files_awaiting_source

    del manager._processors[file]
    manager._start_new_processes()
    assert not manager._processors[file].had_callbacks
    assert create.call_count == 2
    assert file not in manager._priority_files
    assert file not in manager._files_awaiting_source


@pytest.mark.parametrize("failure", ["initialize", "refresh_changed", "refresh_unchanged"])
@mock.patch.object(DagFileProcessorManager, "_create_process", autospec=True)
def test_failed_bundle_claims_allow_healthy_priority_work_then_resume(create, api_manager, tmp_path, failure):
    manager, client, ready = api_manager
    broken = mock.create_autospec(BaseDagBundle, instance=True)
    broken.name = "broken"
    broken.path = tmp_path / "broken"
    broken.path.mkdir()
    broken.is_initialized = True
    broken.supports_versioning = False
    broken.refresh_changes_source = True
    broken.refresh_interval = 0
    manager._dag_bundles.append(broken)
    client.get_bundles.return_value.append(
        DagBundleStateResponse(
            name=broken.name, version=None, last_refreshed=None, revision=None, team_name=None
        )
    )
    manager._bundle_parse_sources[broken.name] = DagParseSource()
    for filename in ("a.py", "b.py"):
        (broken.path / filename).write_text("from airflow.sdk import DAG\ndag = DAG('broken')\n")
    (ready.path / "ready.py").write_text("from airflow.sdk import DAG\ndag = DAG('ready')\n")
    work = [_make_work(broken.name, filename) for filename in ("a.py", "b.py")]
    client.claim_work.return_value = work
    manager._queue_requested_files_for_parsing()
    if failure == "initialize":
        broken.is_initialized = False
        broken.initialize.side_effect = OSError("unavailable")
    else:
        broken.refresh.side_effect = DagBundleRefreshError(
            "unavailable", source_unchanged=failure == "refresh_unchanged"
        )
    known_files = {}
    manager._refresh_dag_bundles(known_files)

    assert manager._bundles_needing_refresh == {broken.name}
    assert len(manager._priority_claims) == 2
    assert not manager._pending_work_acks
    healthy_work = _make_work(ready.name, "ready.py")
    client.claim_work.return_value = [healthy_work]
    manager._queue_requested_files_for_parsing()
    client.claim_work.assert_called_with("priority", [ready.name], 2)
    client.claim_work.return_value = []
    for _ in range(3):
        manager.claim_priority_files()
        client.claim_work.assert_called_with("priority", [ready.name], 1)
    assert len(manager._priority_claims) == 3

    def create_process(_, file, *, source):
        process = mock.create_autospec(DagFileProcessorProcess, instance=True)
        process.id = uuid7()
        process.had_callbacks = False
        process.parse_source = source
        return process

    create.side_effect = create_process
    manager._start_new_processes()
    create.assert_not_called()
    manager._refresh_dag_bundles(known_files)
    manager._start_new_processes()
    assert {file.bundle_name for file in manager._processors} == {ready.name}
    _, process = manager._processors.popitem()
    manager._finish_priority_claims(process.id)
    manager._acknowledge_requested_work()
    client.acknowledge_work.assert_called_once_with("priority", healthy_work)
    assert len(manager._priority_claims) == 2

    broken.initialize.side_effect = lambda: setattr(broken, "is_initialized", True)
    broken.refresh.side_effect = None
    manager._refresh_dag_bundles(known_files)
    manager._start_new_processes()

    assert not manager._bundles_needing_refresh
    assert len(manager._processors) == 2
    assert {file.bundle_name for file in manager._processors} == {broken.name}
    for file, process in list(manager._processors.items()):
        claim = next(claim for claim in manager._priority_claims.values() if claim.attempt_id == process.id)
        assert process.parse_source.inventory_sequence >= claim.inventory_sequence
        manager._finish_priority_claims(process.id)
        del manager._processors[file]
    manager._acknowledge_requested_work()
    assert not manager._priority_claims
    assert client.acknowledge_work.call_args_list == [
        mock.call("priority", item) for item in [healthy_work, *work]
    ]


def test_priority_claims_waiting_for_fresh_inventory_keep_the_budget_bounded(api_manager):
    manager, client, bundle = api_manager
    client.claim_work.return_value = [_make_work(bundle.name, filename) for filename in ("a.py", "b.py")]
    manager._queue_requested_files_for_parsing()

    assert manager.claim_priority_files() == []

    client.claim_work.assert_called_once_with("priority", [bundle.name], 2)
    assert len(manager._priority_claims) == 2


@pytest.mark.parametrize("stage", ["running", "publishing", "acknowledging", "stale"])
def test_failed_bundle_only_suspends_priority_claims_without_active_attempts(api_manager, stage):
    manager, client, bundle = api_manager
    manager._parallelism = 1
    manager._bundles_needing_refresh.add(bundle.name)
    file = DagFileInfo(bundle_name=bundle.name, bundle_path=bundle.path, rel_path=Path("a.py"))
    work = _make_work(bundle.name)
    attempt_id = uuid7()
    if stage == "acknowledging":
        manager._pending_work_acks.append(("priority", work))
    else:
        manager._priority_claims[work.id, work.claim_id] = PriorityParseClaim(
            work=work, inventory_sequence=1, attempt_id=attempt_id
        )
    if stage == "running":
        process = mock.create_autospec(DagFileProcessorProcess, instance=True)
        process.id = attempt_id
        manager._processors[file] = process
    elif stage == "publishing":
        pending = mock.create_autospec(PendingDagPublication, instance=True)
        pending.body = mock.create_autospec(DagParseResultBody, instance=True)
        pending.body.attempt_id = attempt_id
        manager._pending_publications[file] = pending

    assert manager.claim_priority_files() == []

    if stage == "stale":
        client.claim_work.assert_called_once_with("priority", [], 1)
        assert manager._priority_claims[work.id, work.claim_id].attempt_id == attempt_id
    else:
        client.claim_work.assert_not_called()


def test_uninitialized_bundle_is_not_claimed_until_initialization_recovers(api_manager):
    manager, client, bundle = api_manager
    bundle.is_initialized = False

    manager.claim_priority_files()

    client.claim_work.assert_called_once_with("priority", [], 2)
    manager._refresh_dag_bundles({})
    assert bundle.is_initialized
    manager.claim_priority_files()
    client.claim_work.assert_called_with("priority", [bundle.name], 2)


@pytest.mark.parametrize("kind", ["callbacks", "priority"])
@pytest.mark.parametrize("failure", ["connection", "503"])
def test_claim_failure_preserves_deferred_callbacks_and_claims(api_manager, kind, failure):
    manager, client, bundle = api_manager
    request = DagCallbackRequest(
        filepath="a.py", bundle_name=bundle.name, bundle_version="v1", dag_id="dag", run_id="run"
    )
    deferred = [request, request.model_copy(deep=True)]
    manager._deferred_api_callbacks = deferred.copy()
    claims = {id(callback): _make_work(bundle.name, callback=callback) for callback in deferred}
    manager._callback_claims = claims.copy()
    retries = {id(request): DeferredCallback(expires_at=100, next_attempt_time=10, attempts=2)}
    manager._deferred_callback_retries = retries.copy()
    if failure == "connection":
        client.claim_work.side_effect = httpx2.ConnectError("unavailable")
    else:
        response = httpx2.Response(503, request=httpx2.Request("POST", "http://api/claim"))
        client.claim_work.side_effect = httpx2.HTTPStatusError(
            "unavailable", request=response.request, response=response
        )

    result = manager.fetch_callbacks() if kind == "callbacks" else manager.claim_priority_files()

    assert result == (deferred if kind == "callbacks" else [])
    if kind == "callbacks":
        assert all(actual is expected for actual, expected in zip(result, deferred))
    assert manager._callback_claims == claims
    assert manager._deferred_callback_retries == retries
    assert all(actual is expected for actual, expected in zip(manager._deferred_api_callbacks, deferred))
    assert not manager._pending_work_acks


@pytest.mark.parametrize("callback_kind", ["dag", "task"])
@conf_vars({("dag_processor", "job_heartbeat_timeout"): "10"})
@mock.patch.object(DagFileProcessorManager, "_create_bundle_manager", autospec=True)
def test_callback_preparation_logs_retry_and_identifies_expired_work(
    create_bundle_manager, api_manager, caplog, callback_kind
):
    manager, client, bundle = api_manager
    fields = dict(filepath="a.py", bundle_name=bundle.name, bundle_version="v1")
    if callback_kind == "dag":
        request = DagCallbackRequest(**fields, dag_id="dag", run_id="run")
        target = "dag_id=dag run_id=run"
    else:
        request = TaskCallbackRequest(
            **fields,
            ti=TaskInstance(
                id=uuid7(), dag_id="dag", run_id="run", task_id="task", try_number=1, dag_version_id=uuid7()
            ),
        )
        target = "dag_id=dag run_id=run task_id=task"
    work = _make_work(bundle.name, callback=request)
    manager._callback_claims[id(request)] = work
    manager._deferred_api_callbacks.append(request)
    create_bundle_manager.return_value.get_bundle.side_effect = OSError("version unavailable")

    manager._add_callback_to_queue(request)

    assert {
        "event": "Unable to prepare callback bundle; retrying",
        "bundle_name": bundle.name,
        "bundle_version": "v1",
        "target": target,
        "error": "version unavailable",
    } in caplog
    assert not manager._pending_work_acks
    retry = manager._deferred_callback_retries[id(request)]
    retry.expires_at = retry.next_attempt_time = 0
    manager._add_callback_to_queue(request)

    assert {
        "event": "Dropping callback after bundle preparation timed out",
        "callback_type": type(request).__name__,
        "filepath": "a.py",
        "bundle_name": bundle.name,
        "bundle_version": "v1",
        "target": target,
        "timeout": 10,
        "timeout_setting": "[dag_processor] job_heartbeat_timeout",
    } in caplog
    assert manager._pending_work_acks == [("callbacks", work)]
    assert not manager._callback_claims
    assert not manager._deferred_api_callbacks
