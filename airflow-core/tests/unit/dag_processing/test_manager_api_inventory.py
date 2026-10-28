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

from contextlib import nullcontext
from pathlib import Path
from unittest import mock

import httpx2
import pytest
from uuid6 import uuid7

from airflow._shared.timezones import timezone
from airflow.api_fastapi.execution_api.datamodels.dag_parsing import (
    DagBundleInventoryBody,
    DagBundleInventoryResponse,
    DagBundleStateResponse,
)
from airflow.dag_processing.api_client import DagProcessorAPIClient
from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.dag_processing.manager import (
    DagBundleDiscoveryResult,
    DagFileInfo,
    DagFileProcessorManager,
    PendingBundleInventory,
)
from airflow.dag_processing.processor import DagParseSource

from tests_common.test_utils.config import conf_vars


@pytest.fixture(autouse=True)
def freeze_time(time_machine):
    time_machine.move_to("2026-10-28T12:00:00Z", tick=False)


def _create_bundle(tmp_path, name):
    bundle = mock.create_autospec(BaseDagBundle, instance=True)
    bundle.name = name
    bundle.path = tmp_path
    bundle.is_initialized = True
    bundle.supports_versioning = False
    bundle.refresh_changes_source = True
    bundle.refresh_interval = 3600
    return bundle


def _create_client(bundles):
    client = mock.create_autospec(DagProcessorAPIClient, instance=True)
    client.use_bundle.side_effect = lambda name: nullcontext()
    client.get_bundles.return_value = [
        DagBundleStateResponse(
            name=bundle.name, version=None, last_refreshed=None, revision=uuid7(), team_name=None
        )
        for bundle in bundles
    ]
    return client


@pytest.mark.parametrize("already_pending", [False, True], ids=["first-send", "retry"])
@conf_vars(
    {("workers", "execution_api_retry_wait_min"): "1", ("workers", "execution_api_retry_wait_max"): "1"}
)
@mock.patch("airflow.dag_processing.manager.time", autospec=True)
@mock.patch.object(
    DagFileProcessorManager,
    "_find_files_in_bundle",
    autospec=True,
    return_value=DagBundleDiscoveryResult(files=set()),
)
def test_slow_inventory_sends_yield_and_rotate_to_later_bundles(find_files, clock, already_pending, tmp_path):
    clock.monotonic.return_value = 100.0
    bundles = [_create_bundle(tmp_path, name) for name in ("a", "b", "c")]
    client = _create_client(bundles)
    manager = DagFileProcessorManager(max_runs=-1, api_client=client, bundle_refresh_check_interval=5)
    manager._dag_bundles = bundles
    original_bodies = {}
    if already_pending:
        for sequence, state in enumerate(client.get_bundles.return_value, start=1):
            body = DagBundleInventoryBody(
                attempt_id=uuid7(), dispatch_sequence=sequence, expected_revision=state.revision, files=[]
            )
            manager._pending_inventories[state.name] = PendingBundleInventory(body, set(), DagParseSource())
            original_bodies[state.name] = body

    def publish_with_timeout(bundle_name, body):
        clock.monotonic.return_value += 5.0
        raise httpx2.ReadTimeout("Inventory response timed out")

    client.publish_inventory.side_effect = publish_with_timeout
    for index, now in enumerate((100.0, 106.0, 112.0), start=1):
        clock.monotonic.return_value = now
        manager._refresh_dag_bundles({})
        assert client.publish_inventory.call_count == index
        assert clock.monotonic.return_value == now + 5.0
        for name, body in original_bodies.items():
            assert manager._pending_inventories[name].body is body
        original_bodies.update({name: pending.body for name, pending in manager._pending_inventories.items()})

    assert [call.args[0] for call in client.publish_inventory.call_args_list] == ["a", "b", "c"]
    assert set(manager._pending_inventories) == {"a", "b", "c"}
    assert all(pending.attempts == 1 for pending in manager._pending_inventories.values())
    if already_pending:
        find_files.assert_not_called()
    for bundle in bundles:
        assert bundle.refresh.call_count == (0 if already_pending else 1)


@pytest.mark.parametrize("rejection", ["http_413", "invalid_path", "local_size_limit"])
@mock.patch("airflow.dag_processing.manager.MAX_PUBLICATION_BYTES", 1024)
@mock.patch("airflow.dag_processing.manager.time", autospec=True)
@mock.patch.object(DagFileProcessorManager, "_find_files_in_bundle", autospec=True)
def test_rejected_inventory_waits_before_refresh_and_recovers(find_files, clock, rejection, caplog, tmp_path):
    clock.monotonic.return_value = 100.0
    bundle = _create_bundle(tmp_path, "testing")
    client = _create_client([bundle])
    state = client.get_bundles.return_value[0]
    state.last_refreshed = timezone.utcnow()
    manager = DagFileProcessorManager(max_runs=-1, api_client=client, bundle_refresh_check_interval=5)
    manager._dag_bundles = [bundle]
    manager._bundle_versions[bundle.name] = "previous"
    manager._bundle_parse_sources[bundle.name] = DagParseSource(bundle_revision=state.revision)
    manager.request_bundle_refresh(bundle.name)
    previous_file, new_file = [
        DagFileInfo(bundle_name=bundle.name, rel_path=Path(name), bundle_path=tmp_path)
        for name in ("previous.py", "new.py")
    ]
    previous_file.absolute_path.touch()
    new_file.absolute_path.touch()
    known_files = {bundle.name: {previous_file}}
    observed = {new_file}
    if rejection == "invalid_path":
        observed.add(DagFileInfo(bundle_name=bundle.name, rel_path=Path(r"bad\dag.py"), bundle_path=tmp_path))
    elif rejection == "local_size_limit":
        observed = {
            DagFileInfo(
                bundle_name=bundle.name, rel_path=Path(f"{index}/{'é' * 80}.py"), bundle_path=tmp_path
            )
            for index in range(6)
        }
    else:
        response = httpx2.Response(413, request=httpx2.Request("POST", "http://api/inventory"))
        client.publish_inventory.side_effect = httpx2.HTTPStatusError(
            "Proxy body limit", request=response.request, response=response
        )
    find_files.return_value = DagBundleDiscoveryResult(files=observed)

    manager._refresh_dag_bundles(known_files)

    assert known_files == {bundle.name: {previous_file}}
    assert manager._bundle_versions[bundle.name] == "previous"
    assert bundle.name not in manager._bundle_parse_sources
    assert not manager._pending_inventories
    bundle.refresh.assert_called_once_with()
    find_files.assert_called_once_with(manager, bundle)
    sends = int(rejection == "http_413")
    assert client.publish_inventory.call_count == sends
    if rejection == "http_413":
        assert {
            "event": "Inventory publication was rejected for testing",
            "status_code": 413,
            "log_level": "warning",
        } in caplog

    manager.request_bundle_refresh(bundle.name)
    for now in (105.0, 3699.0):
        clock.monotonic.return_value = now
        manager._refresh_dag_bundles(known_files)
        bundle.refresh.assert_called_once_with()
        find_files.assert_called_once_with(manager, bundle)
        assert client.publish_inventory.call_count == sends
        assert known_files == {bundle.name: {previous_file}}

    revision = uuid7()

    def accept_inventory(bundle_name, body):
        return DagBundleInventoryResponse(
            attempt_id=body.attempt_id, accepted_at=timezone.utcnow(), revision=revision
        )

    find_files.return_value = DagBundleDiscoveryResult(files={new_file})
    client.publish_inventory.side_effect = accept_inventory
    clock.monotonic.return_value = 3700.0
    manager._refresh_dag_bundles(known_files)

    assert bundle.refresh.call_count == 2
    assert find_files.call_count == 2
    assert client.publish_inventory.call_count == sends + 1
    assert known_files == {bundle.name: {new_file}}
    assert manager._bundle_versions[bundle.name] is None
    assert manager._bundle_parse_sources[bundle.name].bundle_revision == revision
    assert not manager._pending_inventories
    assert not manager._bundle_inventory_retry_times

    clock.monotonic.return_value = 3705.0
    manager._refresh_dag_bundles(known_files)
    assert bundle.refresh.call_count == 2


@pytest.mark.parametrize("refresh_cooldown", [False, True])
@mock.patch("airflow.dag_processing.manager.time", autospec=True)
@mock.patch.object(
    DagFileProcessorManager,
    "_find_files_in_bundle",
    autospec=True,
    return_value=DagBundleDiscoveryResult(files=set()),
)
def test_missing_catalog_bundle_warns_once_until_it_reappears(
    find_files, clock, caplog, tmp_path, refresh_cooldown
):
    bundle = _create_bundle(tmp_path, "testing")
    client = _create_client([bundle])
    catalog = client.get_bundles.return_value
    manager = DagFileProcessorManager(max_runs=-1, api_client=client, bundle_refresh_check_interval=5)
    manager._dag_bundles = [bundle]
    manager._bundle_versions[bundle.name] = None
    manager._bundle_parse_sources[bundle.name] = DagParseSource(bundle_revision=catalog[0].revision)
    if refresh_cooldown:
        manager._bundle_refresh_retry_at[bundle.name] = 1000.0

    def accept_inventory(bundle_name, body):
        return DagBundleInventoryResponse(
            attempt_id=body.attempt_id, accepted_at=timezone.utcnow(), revision=uuid7()
        )

    client.publish_inventory.side_effect = accept_inventory
    diagnostic = {
        "event": "Bundle missing from API catalog; parsing paused",
        "bundle_name": bundle.name,
        "log_level": "warning",
    }
    for now, present, warns in (
        (100.0, False, True),
        (105.0, False, False),
        (110.0, True, False),
        (115.0, False, True),
        (120.0, False, False),
    ):
        caplog.clear()
        clock.monotonic.return_value = now
        client.get_bundles.return_value = catalog if present else []
        manager._refresh_dag_bundles({})
        assert (diagnostic in caplog) is warns
        if warns:
            assert sum(entry["event"] == diagnostic["event"] for entry in caplog.entries) == 1
        assert (bundle.name in manager._bundle_parse_sources) is present
        assert bundle.name not in manager._bundle_refresh_retry_at

    find_files.assert_called_once_with(manager, bundle)
