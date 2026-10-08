#
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
import pathlib
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from task_sdk.coordinators.node._bundle_test_utils import (
    BUNDLE_NAME,
    mutate_byte,
    mutate_section,
    read_layout,
    write_bundle,
)
from uuid6 import uuid7

from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance
from airflow.sdk.coordinators._subprocess import _PopenActivitySubprocess
from airflow.sdk.coordinators.node import _bundle_reader as _reader
from airflow.sdk.coordinators.node._bundle_reader import _digest_cache
from airflow.sdk.coordinators.node.coordinator import NodeCoordinator, _Bundle
from airflow.sdk.execution_time.coordinator import TaskLaunchError
from airflow.sdk.importers import reset_importer_registry

from tests_common.test_utils.config import conf_vars

SCHEMA_VERSION = "2026-06-16"


@pytest.fixture(autouse=True)
def clear_digest_cache():
    _digest_cache.clear()


def _make_ti(dag_id: str = "test_dag", queue: str = "ts") -> TaskInstance:
    return TaskInstance(
        id=uuid7(),
        dag_version_id=uuid7(),
        task_id="test_task",
        dag_id=dag_id,
        run_id="run_1",
        try_number=1,
        map_index=-1,
        queue=queue,
    )


class TestNodeCoordinatorAttributes:
    def test_default_kwargs(self):
        coordinator = NodeCoordinator()

        assert coordinator.node_executable == "node"
        assert coordinator.task_startup_timeout == 10.0

    def test_custom_kwargs(self):
        coordinator = NodeCoordinator(
            node_executable="/opt/node/bin/node",
            task_handler_bundle_name="ts-task-handlers",
            task_startup_timeout=30.0,
        )

        assert coordinator.node_executable == "/opt/node/bin/node"
        assert coordinator.task_handler_bundle_name == "ts-task-handlers"
        assert coordinator.task_startup_timeout == 30.0

    def test_build_command_scans_passed_roots_in_colocated_mode(self, tmp_path):
        bundle = write_bundle(tmp_path, "test_dag")
        coordinator = NodeCoordinator()
        with coordinator._set_scan_roots([tmp_path]):
            command, schema_version = coordinator._build_execute_task_command(what=_make_ti())
        assert command == ["node", str(bundle)]
        assert schema_version == SCHEMA_VERSION


class TestNodeCoordinatorExecuteTaskCommand:
    def test_selects_bundle_by_dag_id(self, tmp_path):
        selected = write_bundle(tmp_path, "sales")
        coordinator = NodeCoordinator(node_executable="/opt/node/bin/node")

        with coordinator._set_scan_roots([tmp_path]):
            command, schema_version = coordinator._build_execute_task_command(what=_make_ti(dag_id="sales"))

        assert command == ["/opt/node/bin/node", str(selected)]
        assert schema_version == SCHEMA_VERSION

    def test_build_execute_task_command_returns_node_bundle_and_schema_version(self, tmp_path):
        bundle = write_bundle(tmp_path, "test_dag")
        coordinator = NodeCoordinator(node_executable="/opt/node/bin/node")

        with coordinator._set_scan_roots([tmp_path]):
            command, schema_version = coordinator._build_execute_task_command(what=_make_ti())

        assert command == ["/opt/node/bin/node", str(bundle)]
        assert schema_version == SCHEMA_VERSION


@pytest.fixture
def mock_client(make_ti_context):
    client = MagicMock()
    client.task_instances.start.return_value = make_ti_context()
    return client


class TestNodeCoordinatorExecuteNativeDag:
    """With a NodeCoordinator configured, a task of a native TypeScript Dag runs the bundle of its Dag."""

    @pytest.fixture(autouse=True)
    def _node_coordinator_config(self):
        coordinators = {"ts": {"classpath": "airflow.sdk.coordinators.node.NodeCoordinator"}}
        reset_importer_registry()
        with conf_vars({("sdk", "coordinators"): json.dumps(coordinators)}):
            yield
        reset_importer_registry()

    @pytest.fixture
    def mock_start(self, tmp_path):
        write_bundle(tmp_path, "sales", name="a.min.mjs", schema_version="2026-06-16")
        write_bundle(tmp_path, "sales", name="b.min.mjs", schema_version="2026-10-30")
        bundle = MagicMock(path=tmp_path, version="v1")
        bundle.name = "dags-folder"
        with (
            patch("airflow.sdk.coordinators._subprocess._initialize_pinned_bundle", return_value=bundle),
            patch("airflow.sdk.coordinators._subprocess.BundleVersionLock"),
            patch.object(_PopenActivitySubprocess, "start") as mock_start,
        ):
            mock_start.return_value.wait.return_value = 0
            yield mock_start

    def _execute(self, rel_path: str, client):
        return NodeCoordinator().execute_task(
            what=_make_ti(dag_id="sales"),
            dag_rel_path=rel_path,
            bundle_info=BundleInfo(name="dags-folder", version="v1"),
            client=client,
            subprocess_logs_to_stdout=False,
        )

    def test_runs_the_bundle_of_the_dag_and_not_the_first_bundle_by_path(
        self, mock_start, mock_client, tmp_path
    ):
        self._execute("b.min.mjs", mock_client)

        assert mock_start.call_args.kwargs["command"] == ["node", str(tmp_path / "b.min.mjs")]
        assert mock_start.call_args.kwargs["subprocess_schema_version"] == "2026-10-30"

    def test_fails_without_starting_node_for_a_bundle_that_fails_its_integrity_check(
        self, mock_start, mock_client, tmp_path
    ):
        mutate_section(tmp_path / "b.min.mjs", "code")

        with pytest.raises(TaskLaunchError, match="code SHA-256 mismatch"):
            self._execute("b.min.mjs", mock_client)

        mock_start.assert_not_called()


class TestNodeCoordinatorDagFileCommand:
    def test_runs_the_bundle_the_dag_was_parsed_from(self, tmp_path):
        bundle = write_bundle(tmp_path, "sales", schema_version="2026-10-30")
        coordinator = NodeCoordinator(node_executable="/opt/node/bin/node")

        command, schema_version = coordinator._build_dag_file_command(
            what=_make_ti(dag_id="sales"), path=bundle
        )

        assert command == ["/opt/node/bin/node", str(bundle)]
        assert schema_version == "2026-10-30"

    def test_leaves_it_to_the_runtime_to_report_a_dag_the_bundle_does_not_declare(self, tmp_path):
        bundle = write_bundle(tmp_path, "inventory")

        command, _ = NodeCoordinator()._build_dag_file_command(what=_make_ti(dag_id="sales"), path=bundle)

        assert command == ["node", str(bundle)]

    def test_tampered_bundle_raises(self, tmp_path):
        bundle = write_bundle(tmp_path, "sales")
        mutate_section(bundle, "code")

        with pytest.raises(ValueError, match="code SHA-256 mismatch"):
            NodeCoordinator()._build_dag_file_command(what=_make_ti(dag_id="sales"), path=bundle)


class TestNodeCoordinatorParseDagCommand:
    def test_returns_node_and_bundle_schema_version(self, tmp_path):
        bundle = write_bundle(tmp_path / "typescript", "native_dag")
        coordinator = NodeCoordinator(node_executable="/opt/node/bin/node")

        command, schema_version = coordinator._build_parse_dag_command(path=bundle)

        assert command == ["/opt/node/bin/node", str(bundle)]
        assert schema_version == SCHEMA_VERSION

    def test_tampered_bundle_raises(self, tmp_path):
        bundle = write_bundle(tmp_path, "native_dag")
        mutate_section(bundle, "code")

        with pytest.raises(ValueError, match="code SHA-256 mismatch"):
            NodeCoordinator()._build_parse_dag_command(path=bundle)


class TestBundleFind:
    @pytest.mark.parametrize(
        "name",
        ["tasks.mjs", "tasks.js", "tasks.min.js", "bundle.min.mjs.bak", "min.mjs.txt"],
        ids=["mjs", "js", "min-js", "suffixed", "embedded"],
    )
    def test_ignores_files_without_the_bundle_suffix(self, tmp_path, name):
        # Written as a real bundle, so only the name can exclude it.
        write_bundle(tmp_path, "sales", name=name)

        with pytest.raises(FileNotFoundError, match="dag_id='sales'") as exc_info:
            _Bundle.find([tmp_path], "sales")

        # Never opened, so it cannot appear among the rejected candidates.
        assert "rejected candidates" not in str(exc_info.value)

    def test_reports_unreadable_bundle(self, tmp_path, monkeypatch):
        write_bundle(tmp_path, "sales")
        original_open = pathlib.Path.open

        def raise_os_error(self, *args, **kwargs):
            if self.name == BUNDLE_NAME:
                raise PermissionError("denied")
            return original_open(self, *args, **kwargs)

        monkeypatch.setattr(pathlib.Path, "open", raise_os_error)

        with pytest.raises(FileNotFoundError, match="cannot read bundle.min.mjs"):
            _Bundle.find([tmp_path], "sales")

    def test_skips_root_when_bundle_probe_fails(self, tmp_path, monkeypatch):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        unstattable = write_bundle(first, "sales")
        expected = write_bundle(second, "sales")
        original_stat = pathlib.Path.stat

        def fail_first_probe(self, *args, **kwargs):
            if self == unstattable:
                raise PermissionError("denied")
            return original_stat(self, *args, **kwargs)

        monkeypatch.setattr(pathlib.Path, "stat", fail_first_probe)

        found = _Bundle.find([first, second], "sales")

        assert found.path == expected

    def test_finds_bundle_nested_below_a_root(self, tmp_path):
        expected = write_bundle(tmp_path / "team" / "sales", "sales")

        found = _Bundle.find([tmp_path], "sales")

        assert found.path == expected

    def test_selects_bundle_by_dag_id_within_one_root(self, tmp_path):
        write_bundle(tmp_path, "inventory", name="inventory.min.mjs")
        expected = write_bundle(tmp_path, "sales", name="sales.min.mjs")

        found = _Bundle.find([tmp_path], "sales")

        assert found.path == expected

    def test_orders_candidates_in_one_root_by_path(self, tmp_path):
        # Directory iteration order is filesystem-dependent, so sorted name decides the winner.
        expected = write_bundle(tmp_path, "sales", name="a.min.mjs")
        write_bundle(tmp_path, "sales", name="b.min.mjs")
        write_bundle(tmp_path / "nested", "sales")

        found = _Bundle.find([tmp_path], "sales")

        assert found.path == expected

    def test_survives_a_directory_symlink_loop(self, tmp_path):
        expected = write_bundle(tmp_path, "sales")
        loop = tmp_path / "loop"
        try:
            loop.symlink_to(tmp_path, target_is_directory=True)
        except (OSError, NotImplementedError):
            pytest.skip("filesystem does not support directory symlinks")

        found = _Bundle.find([tmp_path], "sales")

        assert found.path == expected

    def test_names_unrelated_min_mjs_file_among_rejected_candidates(self, tmp_path):
        stray = tmp_path / "vendor.min.mjs"
        stray.write_bytes(b"export {};\n")

        with pytest.raises(FileNotFoundError) as exc_info:
            _Bundle.find([tmp_path], "sales")

        message = str(exc_info.value)
        assert str(stray) in message
        assert "no airflow bundle layout" in message

    def test_selects_later_bundle_containing_requested_dag(self, tmp_path):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        write_bundle(first, "inventory")
        expected = write_bundle(second, "sales")

        found = _Bundle.find([first, second], "sales")

        assert found.path == expected

    def test_first_configured_match_wins_for_duplicate_dag(self, tmp_path):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        expected = write_bundle(first, "sales", code=b'console.log("first");\n')
        write_bundle(second, "sales", code=b'console.log("second");\n')

        found = _Bundle.find([first, second], "sales")

        assert found.path == expected

    @mock.patch("airflow.sdk.coordinators.node.coordinator.log.debug", autospec=True)
    def test_skips_corrupt_candidate_and_selects_later_match(self, log_debug, tmp_path):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        corrupt = write_bundle(first, "sales")
        layout = read_layout(corrupt)
        mutate_byte(corrupt, int(layout["code"]["start"], 16))  # type: ignore[index, call-overload]
        expected = write_bundle(second, "sales")

        found = _Bundle.find([first, second], "sales")

        assert found.path == expected
        rejected_log = next(
            call
            for call in log_debug.call_args_list
            if call.args == ("TypeScript bundle rejected; skipping",)
        )
        assert rejected_log.kwargs["exc_info"] is True

    def test_skips_deeply_nested_metadata_and_selects_later_match(self, tmp_path):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        deeply_nested_json = b'{"nested":' + (b"[" * 10_000) + b"0" + (b"]" * 10_000) + b"}"
        write_bundle(first, "sales", metadata_payload=deeply_nested_json)
        expected = write_bundle(second, "sales")

        found = _Bundle.find([first, second], "sales")

        assert found.path == expected

    def test_skips_layout_decoder_recursion_and_selects_later_match(self, tmp_path, monkeypatch):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        write_bundle(first, "sales")
        expected = write_bundle(second, "sales")
        original_loads = json.loads
        call_count = 0

        def recurse_once(payload):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RecursionError("test recursion")
            return original_loads(payload)

        monkeypatch.setattr(_reader.json, "loads", recurse_once)

        found = _Bundle.find([first, second], "sales")

        assert found.path == expected

    def test_skips_matching_bundle_with_invalid_schema_version(self, tmp_path):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        write_bundle(first, "sales", schema_version="banana")
        expected = write_bundle(second, "sales")

        found = _Bundle.find([first, second], "sales")

        assert found.path == expected

    def test_error_names_dag_roots_and_rejected_candidates(self, tmp_path):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        (first / BUNDLE_NAME).write_bytes(b"export {};\n")
        write_bundle(second, "inventory")

        with pytest.raises(FileNotFoundError) as exc_info:
            _Bundle.find([first, second], "sales")

        message = str(exc_info.value)
        assert "dag_id='sales'" in message
        assert str(first) in message
        assert str(second) in message
        assert "rejected candidates" in message
        assert "verified bundle declares dag_ids=['inventory']" in message
        assert "matching bundles were rejected" not in message
