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
Tests for the API-backed persistence of DagFileProcessorManager (AIP-92).

The DAG processor never reads or writes the metadata database directly; every persistence and
metadata operation is routed through the DAG Processing API client, and parse-time metadata
reads go through the remote Execution API.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from airflow.dag_processing.manager import (
    BundleState,
    DagFileInfo,
    DagFileProcessorManager,
    _api_token,
    _dag_processing_api_server_url,
)

from tests_common.test_utils.config import conf_vars

MGR = "airflow.dag_processing.manager"


# --- DAG Processing API URL resolution + client construction ---


@conf_vars({("api", "base_url"): "http://my-host:9999/"})
def test_dag_processing_api_server_url_derives_sibling_mount():
    assert _dag_processing_api_server_url() == "http://my-host:9999/dag-processing"


@conf_vars({("core", "dag_processing_api_server_url"): "http://explicit/dag-processing"})
def test_dag_processing_api_server_url_prefers_explicit():
    assert _dag_processing_api_server_url() == "http://explicit/dag-processing"


def test_api_token_reads_externally_provisioned_file(tmp_path):
    # The processor only carries a token a trusted component wrote; it never mints one.
    token_file = tmp_path / "token"
    token_file.write_text("provisioned-token\n")
    with conf_vars({("dag_processor", "api_token_path"): str(token_file)}):
        assert _api_token() == "provisioned-token"


def test_api_token_none_when_unset():
    with conf_vars({("dag_processor", "api_token_path"): ""}):
        assert _api_token() is None


@conf_vars({("core", "load_examples"): "False"})
@mock.patch(f"{MGR}.DagProcessingApiClient")
def test_client_built_with_derived_default(mock_client_cls):
    # No explicit URL -> always built, pointing at the derived /dag-processing mount.
    manager = DagFileProcessorManager(max_runs=1)
    assert manager._dag_processing_client is mock_client_cls.return_value
    (url,) = mock_client_cls.call_args.args
    assert url.endswith("/dag-processing")


@conf_vars(
    {
        ("core", "load_examples"): "False",
        ("core", "dag_processing_api_server_url"): "http://api:8080/dag-processing",
    }
)
@mock.patch(f"{MGR}.DagProcessingApiClient")
def test_client_built_with_explicit_url(mock_client_cls):
    manager = DagFileProcessorManager(max_runs=1)
    assert manager._dag_processing_client is mock_client_cls.return_value
    # The client is built with the explicit URL (a self-signed token kwarg is also passed).
    assert mock_client_cls.call_args.args[0] == "http://api:8080/dag-processing"


# --- every persistence/metadata method delegates to the client ---


@conf_vars({("core", "load_examples"): "False"})
@mock.patch(f"{MGR}.DagProcessingApiClient")
def test_persist_delegates_to_client(mock_client_cls):
    manager = DagFileProcessorManager(max_runs=1)
    client = manager._dag_processing_client

    manager.persist_parsing_result(
        bundle_name="b1",
        bundle_version="v1",
        version_data=None,
        parsing_result=mock.MagicMock(),
        run_duration=1.0,
        relative_fileloc="dags/a.py",
    )

    client.persist_parsing_result.assert_called_once()
    kwargs = client.persist_parsing_result.call_args.kwargs
    assert kwargs["bundle_name"] == "b1"
    assert kwargs["relative_fileloc"] == "dags/a.py"


@conf_vars({("core", "load_examples"): "False"})
@mock.patch(f"{MGR}.DagProcessingApiClient")
def test_get_bundle_state_delegates_to_client(mock_client_cls):
    manager = DagFileProcessorManager(max_runs=1)
    client = manager._dag_processing_client
    client.get_bundle_state.return_value = {"last_refreshed": None, "version": "v9"}

    state = manager.get_bundle_state("b1")

    client.get_bundle_state.assert_called_once_with("b1")
    assert state == BundleState(last_refreshed=None, version="v9")


@conf_vars({("core", "load_examples"): "False"})
@mock.patch(f"{MGR}.DagProcessingApiClient")
def test_get_bundle_state_returns_none_from_client(mock_client_cls):
    manager = DagFileProcessorManager(max_runs=1)
    client = manager._dag_processing_client
    client.get_bundle_state.return_value = None

    assert manager.get_bundle_state("b1") is None


@conf_vars({("core", "load_examples"): "False"})
@mock.patch(f"{MGR}.DagProcessingApiClient")
def test_update_bundle_state_delegates_to_client(mock_client_cls):
    manager = DagFileProcessorManager(max_runs=1)
    client = manager._dag_processing_client
    ts = datetime.now(timezone.utc)

    manager.update_bundle_state("b1", last_refreshed=ts, version=None)

    client.update_bundle_state.assert_called_once_with("b1", last_refreshed=ts, version=None)


@conf_vars({("core", "load_examples"): "False"})
@mock.patch(f"{MGR}.DagProcessingApiClient")
def test_sync_bundles_delegates_to_client(mock_client_cls):
    manager = DagFileProcessorManager(max_runs=1)
    client = manager._dag_processing_client

    manager.sync_bundles()

    client.sync_bundles.assert_called_once_with()


@conf_vars({("core", "load_examples"): "False"})
@mock.patch(f"{MGR}.DagProcessingApiClient")
def test_deactivate_stale_dags_delegates_to_client(mock_client_cls):
    manager = DagFileProcessorManager(max_runs=1)
    client = manager._dag_processing_client
    file_info = DagFileInfo(rel_path=Path("a.py"), bundle_name="b1")

    manager.deactivate_stale_dags(last_parsed={file_info: datetime.now(timezone.utc)})

    client.deactivate_stale_dags.assert_called_once()
    entries = client.deactivate_stale_dags.call_args.kwargs["last_parsed"]
    assert entries[0]["bundle_name"] == "b1"
    assert entries[0]["relative_fileloc"] == "a.py"


@conf_vars({("core", "load_examples"): "False"})
@mock.patch(f"{MGR}.DagProcessingApiClient")
def test_purge_inactive_dag_warnings_delegates_to_client(mock_client_cls):
    manager = DagFileProcessorManager(max_runs=1)
    client = manager._dag_processing_client

    manager.purge_inactive_dag_warnings()

    client.purge_inactive_dag_warnings.assert_called_once_with()


@conf_vars({("core", "load_examples"): "False"})
@mock.patch(f"{MGR}.DagProcessingApiClient")
def test_claim_priority_files_delegates_to_client(mock_client_cls):
    manager = DagFileProcessorManager(max_runs=1)
    client = manager._dag_processing_client
    client.claim_priority_files.return_value = [{"bundle_name": "b1", "relative_fileloc": "a.py"}]
    bundle = mock.MagicMock()
    bundle.name = "b1"
    bundle.path = Path("/bundles/b1")
    manager._dag_bundles = [bundle]

    files = manager.claim_priority_files()

    client.claim_priority_files.assert_called_once_with(["b1"])
    assert len(files) == 1
    assert files[0].bundle_name == "b1"
    assert str(files[0].rel_path) == "a.py"


@conf_vars({("core", "load_examples"): "False"})
@mock.patch(f"{MGR}.DagProcessingApiClient")
def test_fetch_callbacks_delegates_to_client(mock_client_cls):
    manager = DagFileProcessorManager(max_runs=1)
    client = manager._dag_processing_client
    client.fetch_callbacks.return_value = ["callback-request"]

    result = manager.fetch_callbacks()

    client.fetch_callbacks.assert_called_once()
    assert result == ["callback-request"]


# --- Job lifecycle runs through the API ---


def test_run_dag_processor_job_uses_api():
    from airflow.cli.commands.dag_processor_command import _run_dag_processor_job

    client = mock.MagicMock()
    client.register_job.return_value = 99
    processor = mock.MagicMock()
    processor._dag_processing_client = client
    job_runner = mock.MagicMock(job_type="DagProcessorJob", processor=processor)
    job_runner.job.heartrate = 0.0  # no throttle so the first heartbeat fires immediately

    _run_dag_processor_job(job_runner)

    client.register_job.assert_called_once_with("DagProcessorJob")
    job_runner._execute.assert_called_once()
    client.complete_job.assert_called_once_with(99, state="success")
    # the heartbeat hook routes through the API client
    processor.heartbeat()
    client.job_heartbeat.assert_called_with(99)


# --- parse-time metadata reads go to the remote Execution API ---


@conf_vars(
    {
        ("core", "load_examples"): "False",
        ("core", "execution_api_server_url"): "http://exec:8080/execution",
    }
)
@mock.patch(f"{MGR}.DagProcessingApiClient")
def test_parse_time_client_is_remote(mock_client_cls):
    manager = DagFileProcessorManager(max_runs=1)
    with (
        mock.patch("airflow.dag_processing.manager._api_token", return_value="tok"),
        mock.patch("airflow.dag_processing.manager.Client") as client_cls,
    ):
        _ = manager.client

    client_cls.assert_called_once()
    kwargs = client_cls.call_args.kwargs
    assert kwargs["base_url"] == "http://exec:8080/execution"
    assert kwargs["token"] == "tok"
    assert kwargs["dry_run"] is False


# --- bundle-init credentials resolve through the Execution API (no metadata DB) ---


def test_secrets_comms_resolves_connection_via_execution_api():
    from airflow.dag_processing.manager import _DagProcessorSecretsComms
    from airflow.sdk.execution_time.comms import GetConnection

    client = mock.MagicMock()
    comms = _DagProcessorSecretsComms(client)
    with mock.patch(
        "airflow.dag_processing.manager.handle_get_connection",
        return_value=("conn-result", {}),
    ) as handler:
        result = comms.send(GetConnection(conn_id="git_default"))

    handler.assert_called_once()
    assert handler.call_args.args[0] is client  # routed through the Execution API client
    assert result == "conn-result"


def test_secrets_comms_ignores_unsupported_messages():
    from airflow.dag_processing.manager import _DagProcessorSecretsComms

    comms = _DagProcessorSecretsComms(mock.MagicMock())
    # e.g. a MaskSecret emitted while masking a fetched secret: no-op, never touches the client.
    assert comms.send(object()) is None


@conf_vars({("core", "load_examples"): "False"})
@mock.patch(f"{MGR}.DagProcessingApiClient")
def test_before_run_installs_secrets_comms(mock_client_cls):
    from airflow.dag_processing.manager import _DagProcessorSecretsComms
    from airflow.sdk.execution_time import task_runner

    manager = DagFileProcessorManager(max_runs=1)
    manager.__dict__["client"] = mock.MagicMock()  # pre-populate the cached_property

    sentinel = object()
    old = getattr(task_runner, "SUPERVISOR_COMMS", sentinel)
    try:
        manager._setup_secrets_comms()
        assert isinstance(task_runner.SUPERVISOR_COMMS, _DagProcessorSecretsComms)
    finally:
        if old is sentinel:
            if hasattr(task_runner, "SUPERVISOR_COMMS"):
                del task_runner.SUPERVISOR_COMMS
        else:
            task_runner.SUPERVISOR_COMMS = old
