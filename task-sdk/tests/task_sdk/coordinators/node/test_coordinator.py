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

import pytest
from task_sdk.coordinators._execute_test_utils import execute_task, register_dag_bundle
from task_sdk.coordinators.node._bundle_test_utils import (
    BUNDLE_NAME,
    mutate_byte,
    read_layout,
    replace_layout_payload,
    write_bundle,
)
from uuid6 import uuid7

from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance
from airflow.sdk.coordinators._subprocess import _PopenActivitySubprocess
from airflow.sdk.coordinators.node import _bundle_reader as _reader
from airflow.sdk.coordinators.node._bundle_reader import _digest_cache, read_cache_digest
from airflow.sdk.coordinators.node.coordinator import NodeCoordinator, _Bundle
from airflow.sdk.execution_time.comms import TaskHandlerArtifactRef
from airflow.sdk.execution_time.coordinator import TaskHandlerArtifactError, TaskHandlerCandidate

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


def _write_plain_file(root: pathlib.Path) -> pathlib.Path:
    path = root / BUNDLE_NAME
    path.write_bytes(b"export {};\n")
    return path


def _write_corrupted_bundle(root: pathlib.Path) -> pathlib.Path:
    bundle = write_bundle(root, "test_dag")
    # The last byte before the trailing newline is in the code region.
    mutate_byte(bundle, len(bundle.read_bytes()) - 2)
    return bundle


class TestNodeCoordinatorBuildTaskHandlerCommand:
    def test_returns_node_and_the_bundle_schema_version(self, tmp_path):
        # A bundle registering no Dag: the command does not depend on the Dags it declares.
        bundle = write_bundle(tmp_path)
        coordinator = NodeCoordinator(node_executable="/opt/node/bin/node")

        command, schema_version = coordinator._build_task_handler_command(path=bundle)

        assert command == ["/opt/node/bin/node", str(bundle)]
        assert schema_version == SCHEMA_VERSION

    @pytest.mark.parametrize(
        ("write", "error"),
        [
            pytest.param(_write_plain_file, "has no airflow bundle layout", id="not-a-bundle"),
            pytest.param(_write_corrupted_bundle, "code SHA-256 mismatch", id="corrupted-code"),
        ],
    )
    def test_rejects_a_bundle_that_fails_verification(self, tmp_path, write, error):
        path = write(tmp_path)

        with pytest.raises(ValueError, match=error):
            NodeCoordinator()._build_task_handler_command(path=path)


@pytest.fixture
def ts_task_handlers(tmp_path):
    """Register *tmp_path* as the ``ts-task-handlers`` Dag bundle and return that name."""
    with register_dag_bundle("ts-task-handlers", tmp_path) as name:
        yield name


@pytest.fixture
def mock_client(make_ti_context):
    client = mock.MagicMock()
    client.task_instances.start.return_value = make_ti_context()
    return client


def _execute_task(
    mock_client,
    bundle_name: str,
    *,
    dag_rel_path: str = BUNDLE_NAME,
    task_handler_artifact: TaskHandlerArtifactRef | None = None,
    coordinator: NodeCoordinator | None = None,
):
    """Run a task of the Dag bundle *bundle_name* and return the commands the runtime was started with."""
    return execute_task(
        coordinator or NodeCoordinator(node_executable="/opt/node/bin/node"),
        mock_client,
        what=_make_ti(dag_id="sales"),
        dag_rel_path=dag_rel_path,
        bundle_info=BundleInfo(name=bundle_name),
        task_handler_artifact=task_handler_artifact,
    )


class TestNodeCoordinatorExecuteTask:
    def test_a_task_without_a_reference_runs_its_dag_file(self, tmp_path, ts_task_handlers, mock_client):
        bundle = write_bundle(tmp_path)

        result, popen_calls = _execute_task(mock_client, ts_task_handlers)

        assert popen_calls[0][:2] == ["/opt/node/bin/node", str(bundle)]
        assert result.exit_code == 0

    def test_a_referenced_bundle_runs_even_when_another_declares_the_same_dag(
        self, tmp_path, ts_task_handlers, mock_client
    ):
        write_bundle(tmp_path, "sales", name="a.min.mjs")
        expected = write_bundle(tmp_path, "sales", name="team/z.min.mjs")
        reference = TaskHandlerArtifactRef(
            bundle_info=BundleInfo(name=ts_task_handlers), rel_path="team/z.min.mjs"
        )

        _, popen_calls = _execute_task(
            mock_client, "other-dags", dag_rel_path="dag.py", task_handler_artifact=reference
        )

        assert popen_calls[0][:2] == ["/opt/node/bin/node", str(expected)]

    def test_a_referenced_bundle_runs_whatever_dags_it_declares(
        self, tmp_path, ts_task_handlers, mock_client
    ):
        expected = write_bundle(tmp_path, name="etl.min.mjs")
        reference = TaskHandlerArtifactRef(rel_path="etl.min.mjs")

        _, popen_calls = _execute_task(
            mock_client, ts_task_handlers, dag_rel_path="dag.py", task_handler_artifact=reference
        )

        assert popen_calls[0][:2] == ["/opt/node/bin/node", str(expected)]

    def test_task_handler_bundle_name_is_not_read(self, tmp_path, ts_task_handlers, mock_client):
        bundle = write_bundle(tmp_path)
        coordinator = NodeCoordinator(task_handler_bundle_name="not-a-configured-bundle")

        _, popen_calls = _execute_task(mock_client, ts_task_handlers, coordinator=coordinator)

        assert popen_calls[0][1] == str(bundle)

    @mock.patch.object(_PopenActivitySubprocess, "start", autospec=True)
    def test_the_schema_version_of_the_bundle_is_forwarded(
        self, mock_start, tmp_path, ts_task_handlers, mock_client
    ):
        write_bundle(tmp_path, "sales", schema_version="2026-06-16")
        mock_start.return_value.wait.return_value = 0

        NodeCoordinator().execute_task(
            what=_make_ti(dag_id="sales"),
            dag_rel_path=BUNDLE_NAME,
            bundle_info=BundleInfo(name=ts_task_handlers),
            client=mock_client,
            subprocess_logs_to_stdout=False,
        )

        assert mock_start.call_args.kwargs["subprocess_schema_version"] == SCHEMA_VERSION

    def test_a_python_dag_file_raises_the_unbound_message(self, tmp_path, ts_task_handlers, mock_client):
        (tmp_path / "dags").mkdir()
        (tmp_path / "dags" / "etl.py").write_text("print('hello')\n")

        with pytest.raises(TaskHandlerArtifactError) as raised:
            _execute_task(mock_client, ts_task_handlers, dag_rel_path="dags/etl.py")

        assert str(raised.value) == (
            "Task 'test_task' of Dag 'sales' has no task handler artifact, and its Dag file 'dags/etl.py' "
            "is not an artifact that NodeCoordinator runs. Queue 'ts' routes it to a Lang-SDK "
            "coordinator, so it must be a @task.stub task the Dag processor bound to an artifact, or a "
            "task of a Dag defined in a Lang SDK. Check the import errors of 'dags/etl.py', and that the "
            "scheduler has the same [sdk] configuration as the Dag processor."
        )

    def test_a_min_mjs_file_that_is_not_a_bundle_is_not_an_artifact(
        self, tmp_path, ts_task_handlers, mock_client
    ):
        _write_plain_file(tmp_path)
        reference = TaskHandlerArtifactRef(rel_path=BUNDLE_NAME)

        with pytest.raises(TaskHandlerArtifactError, match="is not an artifact that NodeCoordinator runs"):
            _execute_task(
                mock_client, ts_task_handlers, dag_rel_path="dag.py", task_handler_artifact=reference
            )

    def test_a_bundle_that_fails_verification_raises_with_the_reason(
        self, tmp_path, ts_task_handlers, mock_client
    ):
        _write_corrupted_bundle(tmp_path)
        reference = TaskHandlerArtifactRef(rel_path=BUNDLE_NAME)

        with pytest.raises(TaskHandlerArtifactError) as raised:
            _execute_task(
                mock_client, ts_task_handlers, dag_rel_path="dag.py", task_handler_artifact=reference
            )

        assert str(raised.value).startswith(
            f"Task handler artifact '{BUNDLE_NAME}' in Dag bundle '{ts_task_handlers}' cannot run: "
        )
        assert "code SHA-256 mismatch" in str(raised.value)

    def test_a_bundle_with_an_unknown_schema_version_raises_before_the_runtime_starts(
        self, tmp_path, ts_task_handlers, mock_client
    ):
        write_bundle(tmp_path, "sales", schema_version="1999-01-01")
        reference = TaskHandlerArtifactRef(rel_path=BUNDLE_NAME)

        with mock.patch.object(_PopenActivitySubprocess, "start", autospec=True) as mock_start:
            with pytest.raises(TaskHandlerArtifactError, match="uses supervisor schema version '1999-01-01'"):
                _execute_task(
                    mock_client, ts_task_handlers, dag_rel_path="dag.py", task_handler_artifact=reference
                )

        mock_start.assert_not_called()


class TestListTaskHandlerCandidates:
    def test_lists_min_mjs_files_with_a_layout_line(self, tmp_path):
        bundle = write_bundle(tmp_path, "test_dag", name="team-a/handlers.min.mjs")
        _write_plain_file(tmp_path)
        write_bundle(tmp_path, "test_dag", name="handlers.mjs")

        assert NodeCoordinator().list_task_handler_candidates(tmp_path) == [
            TaskHandlerCandidate(
                rel_path="team-a/handlers.min.mjs",
                size_bytes=bundle.stat().st_size,
                cache_digest=read_cache_digest(bundle),
            )
        ]

    def test_lists_a_bundle_with_an_invalid_layout_line_with_an_error(self, tmp_path):
        bundle = write_bundle(tmp_path, "test_dag", name="handlers.min.mjs")
        replace_layout_payload(bundle, b"[]")

        assert NodeCoordinator().list_task_handler_candidates(tmp_path) == [
            TaskHandlerCandidate(
                rel_path="handlers.min.mjs",
                size_bytes=bundle.stat().st_size,
                cache_digest=None,
                error="handlers.min.mjs: embedded airflow bundle layout must contain a mapping",
            )
        ]


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
