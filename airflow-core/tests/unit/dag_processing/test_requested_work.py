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

import pytest
from uuid6 import uuid7

from airflow.api_fastapi.execution_api.datamodels.dag_parsing import DagParseResultBody
from airflow.callbacks.callback_requests import DagCallbackRequest
from airflow.dag_processing.api_client import DagProcessorAPIClient
from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.dag_processing.manager import DagFileInfo, DagFileProcessorManager, PendingDagPublication
from airflow.dag_processing.processor import DagFileProcessorProcess
from airflow.dag_processing.requested_work import DatabaseRequestedWorkSource, RequestedWorkSource


def test_default_source_does_not_claim_or_retain_work():
    source = RequestedWorkSource()
    file = DagFileInfo(rel_path=Path("example.py"), bundle_name="test")
    request = DagCallbackRequest(
        filepath="example.py",
        dag_id="example",
        run_id="test",
        is_failure_callback=False,
        bundle_name="test",
        bundle_version=None,
    )
    attempt_id = uuid7()

    assert (
        source.claim_priority_files(
            [], capacity=1, unavailable_bundles=set(), active_attempts=set(), inventory_sequence=1
        )
        == []
    )
    assert source.fetch_callbacks([], limit=1) == []
    source.callbacks_started(attempt_id, [request])
    source.parse_started(file, attempt_id)
    source.callback_deferred(request)
    source.callback_queued(request)
    source.callback_discarded(request)
    source.inventory_published("test", sequence=1, files=["example.py"], failed_sources=[])
    source.bundle_unusable("test", active_attempts={attempt_id})
    source.attempt_finished(attempt_id, had_callbacks=False)
    source.flush()

    assert not source.requires_acknowledgment
    assert not source.has_outstanding_work()
    assert not source.has_priority_request(file)
    assert not source.has_callback_claim(request)
    assert not source.has_callback_attempt(attempt_id)
    assert source.is_priority_ready(file, 0)
    assert source.is_callback_preparation_ready(request)


def test_database_source_delegates_claims_without_applying_api_limits():
    claim_priority = mock.create_autospec(lambda: [])
    fetch_callbacks = mock.create_autospec(lambda: [])
    source = DatabaseRequestedWorkSource(claim_priority, fetch_callbacks)

    assert (
        source.claim_priority_files(
            [], capacity=0, unavailable_bundles={"test"}, active_attempts={uuid7()}, inventory_sequence=2
        )
        is claim_priority.return_value
    )
    assert source.fetch_callbacks([], limit=0) is fetch_callbacks.return_value

    claim_priority.assert_called_once_with()
    fetch_callbacks.assert_called_once_with()
    assert not source.requires_acknowledgment


@pytest.mark.parametrize("use_api_client", [False, True])
def test_manager_uses_injected_source_with_dispatch_context(use_api_client):
    source = mock.create_autospec(RequestedWorkSource, instance=True)
    client = mock.create_autospec(DagProcessorAPIClient, instance=True) if use_api_client else None
    manager = DagFileProcessorManager(
        max_runs=1, parallelism=3, max_callbacks_per_loop=5, api_client=client, work_source=source
    )
    bundles = []
    for name, initialized in [("ready", True), ("refreshing", True), ("uninitialized", False)]:
        bundle = mock.create_autospec(BaseDagBundle, instance=True)
        bundle.name = name
        bundle.is_initialized = initialized
        bundles.append(bundle)
    manager._dag_bundles = bundles
    manager._bundles_needing_refresh = {"refreshing"}
    manager._bundles_waiting_for_refresh = {"ready"}
    manager._dispatch_sequence = 7
    file = DagFileInfo(rel_path=Path("running.py"), bundle_name="ready")
    processor = mock.create_autospec(DagFileProcessorProcess, instance=True)
    processor.id = uuid7()
    manager._processors[file] = processor
    pending = mock.create_autospec(PendingDagPublication, instance=True)
    pending.body = mock.create_autospec(DagParseResultBody, instance=True)
    pending.body.attempt_id = uuid7()
    manager._pending_publications[file] = pending

    assert manager.claim_priority_files() is source.claim_priority_files.return_value
    assert manager.fetch_callbacks() is source.fetch_callbacks.return_value

    source.claim_priority_files.assert_called_once_with(
        bundles,
        capacity=3,
        unavailable_bundles={"refreshing", "uninitialized"},
        active_attempts={processor.id, pending.body.attempt_id},
        inventory_sequence=8,
    )
    source.fetch_callbacks.assert_called_once_with(bundles, limit=5)


@mock.patch.object(DagFileProcessorManager, "_claim_priority_files", autospec=True)
@mock.patch.object(DagFileProcessorManager, "_fetch_callbacks_from_db", autospec=True)
def test_manager_defaults_to_database_source(fetch_callbacks, claim_priority):
    manager = DagFileProcessorManager(max_runs=1)

    assert isinstance(manager.work_source, DatabaseRequestedWorkSource)
    assert manager.claim_priority_files() is claim_priority.return_value
    assert manager.fetch_callbacks() is fetch_callbacks.return_value

    claim_priority.assert_called_once_with(manager)
    fetch_callbacks.assert_called_once_with(manager)
