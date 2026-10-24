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

from dataclasses import replace
from pathlib import Path
from unittest import mock

import pytest
from uuid6 import uuid7

from airflow._shared.timezones import timezone
from airflow.api_fastapi.execution_api.datamodels.dag_parsing import (
    DagBundleInventoryBody,
    DagBundleInventoryResponse,
    DagBundleStateResponse,
    ProcessorWorkItem,
)
from airflow.callbacks.callback_requests import DagCallbackRequest
from airflow.dag_processing.api_client import DagProcessorAPIClient
from airflow.dag_processing.bundles.base import BaseDagBundle, DagBundleRefreshError
from airflow.dag_processing.bundles.local import LocalDagBundle
from airflow.dag_processing.manager import DagFileInfo, DagFileProcessorManager, PendingBundleInventory
from airflow.dag_processing.processor import DagFileParsingResult, DagFileProcessorProcess, DagParseSource


@pytest.fixture
def api_manager(tmp_path):
    (tmp_path / "a.py").write_text("from airflow.sdk import DAG\ndag = DAG('test')\n")
    bundle = LocalDagBundle(name="testing", path=str(tmp_path), refresh_interval=0)
    bundle.is_initialized = True
    file = DagFileInfo(bundle_name=bundle.name, rel_path=Path("a.py"), bundle_path=tmp_path)
    client = mock.create_autospec(DagProcessorAPIClient, instance=True)
    revision = uuid7()
    client.get_bundles.return_value = [
        DagBundleStateResponse(
            name=bundle.name, version=None, last_refreshed=None, revision=revision, team_name=None
        )
    ]
    client.publish_inventory.side_effect = lambda name, body: DagBundleInventoryResponse(
        attempt_id=body.attempt_id, revision=revision, accepted_at=timezone.utcnow()
    )
    manager = DagFileProcessorManager(
        max_runs=1, api_client=client, parallelism=2, bundle_refresh_check_interval=0
    )
    manager._dag_bundles = [bundle]
    manager._bundle_versions[bundle.name] = None
    manager._bundle_refresh_generations[bundle.name] = 3
    manager._dispatch_sequence = 7
    manager._bundle_parse_sources[bundle.name] = DagParseSource(
        refresh_generation=3, bundle_revision=revision, inventory_sequence=7
    )
    work = ProcessorWorkItem(
        id=str(uuid7()), claim_id=uuid7(), bundle_name=bundle.name, relative_fileloc="a.py"
    )
    client.claim_work.return_value = [work]
    return manager, client, bundle, file, work


@mock.patch.object(DagFileProcessorManager, "_create_process", autospec=True)
def test_local_refresh_attaches_claim_after_new_inventory_without_invalidating_active_parse(
    create, api_manager
):
    manager, client, bundle, file, work = api_manager
    old = mock.create_autospec(DagFileProcessorProcess, instance=True)
    old.id = uuid7()
    old.had_callbacks = False
    old.parse_source = manager._bundle_parse_sources[bundle.name]
    manager._processors[file] = old

    manager._queue_requested_files_for_parsing()
    manager._refresh_dag_bundles({bundle.name: {file}})

    client.publish_inventory.assert_called_once()
    source = manager._bundle_parse_sources[bundle.name]
    assert source.refresh_generation == old.parse_source.refresh_generation == 3
    assert source.inventory_sequence > old.parse_source.inventory_sequence
    claim = manager._priority_claims[work.id, work.claim_id]
    assert claim.attempt_id is None
    manager._start_new_processes()
    create.assert_not_called()

    manager._processors.pop(file)
    new = mock.create_autospec(DagFileProcessorProcess, instance=True)
    new.id = uuid7()
    new.had_callbacks = False
    create.return_value = new
    manager._start_new_processes()

    create.assert_called_once_with(manager, file, source=source)
    assert claim.attempt_id == new.id


def test_inventory_created_before_claim_cannot_acknowledge_new_missing_file_request(api_manager):
    manager, _, bundle, _, work = api_manager
    source = manager._bundle_parse_sources[bundle.name]
    old_inventory = DagBundleInventoryBody(
        attempt_id=uuid7(), dispatch_sequence=7, expected_revision=source.bundle_revision, files=[]
    )
    manager._pending_inventories[bundle.name] = PendingBundleInventory(old_inventory, set(), source)
    manager._queue_requested_files_for_parsing()

    manager._publish_bundle_inventory(bundle.name, {})

    assert (work.id, work.claim_id) in manager._priority_claims
    assert not manager._pending_work_acks

    new_inventory = old_inventory.model_copy(update={"attempt_id": uuid7(), "dispatch_sequence": 8})
    manager._pending_inventories[bundle.name] = PendingBundleInventory(new_inventory, set(), source)
    manager._publish_bundle_inventory(bundle.name, {})

    assert (work.id, work.claim_id) not in manager._priority_claims
    assert manager._pending_work_acks == [("priority", work)]


@pytest.mark.parametrize("callback_version", [None, "v1"])
@mock.patch.object(DagFileProcessorManager, "_create_process", autospec=True)
def test_callbacks_run_while_priority_parse_waits_for_new_inventory(create, api_manager, callback_version):
    manager, _, _, file, work = api_manager
    manager._parallelism = 1
    manager._queue_requested_files_for_parsing()
    manager._force_refresh_bundles.clear()
    callback_file = replace(file, bundle_version=callback_version)
    request = DagCallbackRequest(
        filepath=str(file.rel_path),
        bundle_name=file.bundle_name,
        bundle_version=callback_version,
        dag_id="test",
        run_id="run",
    )
    manager._callback_to_execute[callback_file].append(request)
    manager._add_files_to_queue([callback_file], mode="front")

    def create_process(_, file, *, source):
        manager._callback_to_execute.pop(file)
        processor = mock.create_autospec(DagFileProcessorProcess, instance=True)
        processor.id = uuid7()
        processor.had_callbacks = True
        return processor

    create.side_effect = create_process
    manager._start_new_processes()

    create.assert_called_once_with(
        manager, callback_file, source=DagParseSource.for_callback(callback_version)
    )
    assert manager._priority_claims[work.id, work.claim_id].attempt_id is None
    manager._processors.pop(callback_file)
    manager._start_new_processes()

    assert create.call_count == 1
    assert file in manager._files_awaiting_source
    assert file not in manager._file_queue
    assert not manager._pending_work_acks


@pytest.mark.parametrize("callback_already_running", [False, True])
@mock.patch.object(DagFileProcessorManager, "_create_process", autospec=True)
def test_callback_for_removed_file_does_not_restore_acknowledged_priority_request(
    create, api_manager, callback_already_running
):
    manager, client, bundle, file, work = api_manager
    manager._queue_requested_files_for_parsing()
    request = DagCallbackRequest(
        filepath=str(file.rel_path), bundle_name=bundle.name, bundle_version=None, dag_id="test", run_id="run"
    )
    manager._callback_to_execute[file].append(request)
    file.absolute_path.unlink()

    manager._refresh_dag_bundles({bundle.name: {file}})

    assert client.publish_inventory.call_args.args[1].files == []
    assert not manager._priority_claims
    assert manager._pending_work_acks == [("priority", work)]
    assert file in manager._file_queue

    def create_process(_, file, *, source):
        manager._callback_to_execute.pop(file)
        processor = mock.create_autospec(DagFileProcessorProcess, instance=True)
        processor.id = uuid7()
        processor.had_callbacks = True
        processor.is_ready = True
        processor.start_time = 0
        processor.parse_source = source
        processor.parsing_result = None
        return processor

    create.side_effect = create_process
    if callback_already_running:
        manager._processors[file] = manager._create_process(file, source=DagParseSource.for_callback(None))
    manager._start_new_processes()
    assert not manager._priority_files
    assert not manager._files_awaiting_source
    assert not manager._file_queue
    manager._collect_results()
    manager._acknowledge_requested_work()
    manager._start_new_processes()

    create.assert_called_once_with(manager, file, source=DagParseSource.for_callback(None))
    client.acknowledge_work.assert_called_once_with("priority", work)
    assert not manager._priority_files
    assert not manager._files_awaiting_source
    assert not manager._file_queue
    assert not manager._processors


@pytest.mark.parametrize("file_process_interval", [0, 300])
@pytest.mark.parametrize("max_runs", [-1, 1])
@mock.patch.object(DagFileProcessorManager, "_create_process", autospec=True)
def test_failed_fetch_parks_priority_work_until_new_inventory_without_starving_regular_files(
    create, api_manager, max_runs, file_process_interval
):
    manager, client, local_bundle, file, work = api_manager
    bundle = mock.create_autospec(BaseDagBundle, instance=True)
    bundle.name = local_bundle.name
    bundle.path = local_bundle.path
    bundle.is_initialized = True
    bundle.supports_versioning = True
    bundle.refresh_changes_source = True
    bundle.refresh_interval = 300
    bundle.get_current_version.return_value = "v1"
    bundle.refresh.side_effect = DagBundleRefreshError("fetch failed", source_unchanged=True)
    manager._dag_bundles = [bundle]
    manager._bundle_versions[bundle.name] = "v1"
    source = manager._bundle_parse_sources[bundle.name]
    manager._bundle_parse_sources[bundle.name] = DagParseSource(
        bundle_version="v1",
        refresh_generation=source.refresh_generation,
        bundle_revision=source.bundle_revision,
        inventory_sequence=source.inventory_sequence,
    )
    client.get_bundles.return_value[0].version = "v1"
    manager.max_runs = max_runs
    manager._file_process_interval = file_process_interval
    manager._parallelism = 1
    manager._num_run = 1
    healthy_file = replace(file, rel_path=Path("healthy.py"))
    (bundle.path / healthy_file.rel_path).write_text("from airflow.sdk import DAG\ndag = DAG('healthy')\n")
    known_files = {bundle.name: {file, healthy_file}}
    manager._queue_requested_files_for_parsing()
    manager._refresh_dag_bundles(known_files)
    manager._add_files_to_queue([healthy_file], mode="back")

    def create_process(_, file, *, source):
        processor = mock.create_autospec(DagFileProcessorProcess, instance=True)
        processor.id = uuid7()
        processor.had_callbacks = False
        processor.is_ready = True
        processor.start_time = 0
        processor.parse_source = source
        processor.parsing_result = DagFileParsingResult(fileloc=str(file.rel_path), serialized_dags=[])
        return processor

    create.side_effect = create_process

    manager._start_new_processes()

    create.assert_called_once_with(manager, healthy_file, source=manager._bundle_parse_sources[bundle.name])
    assert file in manager._files_awaiting_source
    assert file not in manager._file_queue
    assert manager._bundle_parse_sources[bundle.name].refresh_generation == 4
    assert manager._bundle_parse_sources[bundle.name].inventory_sequence == 7
    claim = manager._priority_claims[work.id, work.claim_id]
    assert claim.attempt_id is None
    client.publish_inventory.assert_not_called()

    manager._collect_results()
    manager._publish_pending_results()
    assert manager._file_stats[healthy_file].run_count == 1
    assert not manager.max_runs_reached()
    for _ in range(3):
        manager.prepare_file_queue(known_files)
        manager._start_new_processes()
        assert file not in manager._processors
        assert file in manager._files_awaiting_source
        assert file not in manager._file_queue
        manager._collect_results()
        manager._publish_pending_results()
    assert all(call.args[1] == healthy_file for call in create.call_args_list)
    assert not manager._pending_work_acks

    bundle.refresh.side_effect = None
    manager._refresh_dag_bundles(known_files)
    manager._start_new_processes()

    assert claim.attempt_id == manager._processors[file].id
    assert file not in manager._files_awaiting_source
    assert manager._bundle_parse_sources[bundle.name].inventory_sequence >= claim.inventory_sequence
    manager._collect_results()
    manager._publish_pending_results()
    assert manager._file_stats[file].run_count == 1
    assert manager._pending_work_acks == [("priority", work)]
