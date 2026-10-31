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
"""API-mode manager paths that must not reach the metadata database."""

from __future__ import annotations

import json
import textwrap
from contextlib import nullcontext
from unittest import mock

import httpx2
import pytest
from sqlalchemy.engine import Engine
from uuid6 import uuid7

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.api_fastapi.execution_api.datamodels.dag_parsing import (
    DagBundleInventoryResponse,
    DagBundleStateResponse,
    DagParseResultResponse,
)
from airflow.dag_processing.api_client import DagProcessorAPIClient
from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.dag_processing.bundles.manager import _load_bundle_config_snapshot
from airflow.dag_processing.manager import DagFileProcessorManager

from tests_common.test_utils.config import conf_vars


def _create_client(*bundle_names: str):
    client = mock.create_autospec(DagProcessorAPIClient, instance=True)
    client.use_bundle.side_effect = lambda name: nullcontext()
    client.get_bundles.return_value = [
        DagBundleStateResponse(name=name, version=None, last_refreshed=None, revision=None, team_name=None)
        for name in bundle_names
    ]
    return client


def _create_bundle(name: str, *, supports_versioning: bool = False):
    bundle = mock.create_autospec(BaseDagBundle, instance=True)
    bundle.name = name
    bundle.is_initialized = True
    bundle.supports_versioning = supports_versioning
    return bundle


@pytest.fixture
def database_calls(monkeypatch):
    """Make every metadata-database entry point record its use and fail."""
    calls = []

    def forbid(name):
        def record(*args, **kwargs):
            calls.append(name)
            raise RuntimeError(f"Unexpected database access through {name}")

        return record

    monkeypatch.setattr(settings, "Session", forbid("settings.Session"))
    monkeypatch.setattr(settings, "NonScopedSession", forbid("settings.NonScopedSession"))
    monkeypatch.setattr(Engine, "connect", forbid("Engine.connect"))
    monkeypatch.setattr(Engine, "raw_connection", forbid("Engine.raw_connection"))
    return calls


@mock.patch.object(DagFileProcessorManager, "deactivate_stale_dags", autospec=True)
def test_stale_dag_scan_does_not_deactivate_dags(deactivate):
    manager = DagFileProcessorManager(max_runs=1, api_client=_create_client(), parsing_cleanup_interval=0)

    manager._scan_stale_dags()

    deactivate.assert_not_called()


@mock.patch.object(DagFileProcessorManager, "get_bundle_state", autospec=True, return_value=None)
def test_unreadable_catalog_does_not_read_bundle_state_or_refresh(get_bundle_state):
    client = _create_client()
    client.get_bundles.side_effect = httpx2.ConnectError("API unavailable")
    manager = DagFileProcessorManager(max_runs=1, api_client=client, bundle_refresh_check_interval=0)
    bundle = _create_bundle("testing")
    manager._dag_bundles = [bundle]

    manager._refresh_dag_bundles({})

    get_bundle_state.assert_not_called()
    bundle.refresh.assert_not_called()


@pytest.mark.parametrize(
    ("provisioned", "expectation"),
    [
        pytest.param(["a", "b"], nullcontext(), id="all-provisioned"),
        pytest.param(["a"], pytest.raises(ValueError, match=r"\['b'\]"), id="missing-bundle"),
    ],
)
@mock.patch("airflow.dag_processing.manager.DagBundlesManager", autospec=True)
def test_sync_bundles_checks_provisioning_without_writing_bundles(bundles_manager, provisioned, expectation):
    client = _create_client(*provisioned)
    manager = DagFileProcessorManager(max_runs=1, api_client=client, bundle_names_to_parse=["a", "b"])

    with expectation:
        manager.sync_bundles()

    client.get_bundles.assert_called_once()
    bundles_manager.return_value.sync_bundles_to_db.assert_not_called()


@mock.patch("airflow.dag_processing.manager.BundleUsageTrackingManager", autospec=True)
def test_stale_bundle_version_cleanup_is_scoped_to_loaded_versioned_bundles(tracking_manager):
    manager = DagFileProcessorManager(max_runs=1, api_client=_create_client())
    manager._dag_bundles = [_create_bundle("versioned", supports_versioning=True), _create_bundle("plain")]

    manager.cleanup_stale_bundle_versions()

    tracking_manager.return_value.remove_stale_bundle_versions_for_bundle.assert_called_once_with("versioned")
    tracking_manager.return_value.remove_stale_bundle_versions.assert_not_called()


@mock.patch.object(DagFileProcessorManager, "register_exit_signals", autospec=True)
@mock.patch("airflow.dag_processing.manager.gc", autospec=True)
def test_parsing_loop_publishes_through_the_api_without_database_calls(
    gc, register_exit_signals, database_calls, tmp_path, monkeypatch
):
    """
    One full ``run()`` of an API-mode manager, observed in the manager process only.

    Removing the API-mode guard in ``_scan_stale_dags`` or ``sync_bundles`` fails this test. It
    does not cover the guard for an unreadable catalog in ``_refresh_dag_bundles``, because the
    catalog read succeeds here, nor the one in ``cleanup_stale_bundle_versions``, whose
    database-mode fallback lists bundles without querying the database; the tests above cover both.
    """
    dags = tmp_path / "dags"
    dags.mkdir()
    (dags / "api_mode_dag.py").write_text(
        textwrap.dedent(
            """
            from airflow.sdk import DAG

            with DAG("api_mode_dag", schedule=None):
                pass
            """
        )
    )
    bundle_config = {
        "name": "api_mode",
        "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
        "kwargs": {"path": str(dags), "refresh_interval": 0},
    }
    client = _create_client("api_mode")
    client.publish_inventory.side_effect = lambda bundle_name, body: DagBundleInventoryResponse(
        attempt_id=body.attempt_id, accepted_at=timezone.utcnow(), revision=uuid7()
    )
    published = []

    def publish_parse_result(body):
        published.append(body)
        return DagParseResultResponse(attempt_id=body.attempt_id, accepted_at=timezone.utcnow())

    client.publish_parse_result.side_effect = publish_parse_result
    # run() sets this for the whole process; monkeypatch restores the previous value on teardown.
    monkeypatch.setenv("_AIRFLOW_PROCESS_CONTEXT", "client")

    with conf_vars(
        {
            ("dag_processor", "dag_bundle_config_list"): json.dumps([bundle_config]),
            ("core", "load_examples"): "False",
            ("logging", "dag_processor_child_process_log_directory"): str(tmp_path / "logs"),
        }
    ):
        _load_bundle_config_snapshot.cache_clear()
        manager = DagFileProcessorManager(
            max_runs=1, api_client=client, parsing_cleanup_interval=0, stale_bundle_cleanup_interval=1
        )
        try:
            manager.run()
        finally:
            manager.terminate()
            manager.end()
    _load_bundle_config_snapshot.cache_clear()

    assert database_calls == []
    assert [(body.relative_fileloc, len(body.serialized_dags), body.import_errors) for body in published] == [
        ("api_mode_dag.py", 1, {})
    ]
    assert client.publish_inventory.call_args_list
    assert all(
        call.args[0] == "api_mode" and call.args[1].files == ["api_mode_dag.py"]
        for call in client.publish_inventory.call_args_list
    )
