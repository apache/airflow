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

import pathlib
from unittest import mock

import pytest
from task_sdk.coordinators._execute_test_utils import execute_task, register_dag_bundle
from task_sdk.coordinators.node._bundle_test_utils import (
    BUNDLE_NAME,
    mutate_byte,
    replace_layout_payload,
    write_bundle,
)
from uuid6 import uuid7

from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance
from airflow.sdk.coordinators._subprocess import _PopenActivitySubprocess
from airflow.sdk.coordinators.node._bundle_reader import _digest_cache, read_cache_digest
from airflow.sdk.coordinators.node.coordinator import NodeCoordinator
from airflow.sdk.execution_time.comms import TaskHandlerArtifactRef
from airflow.sdk.execution_time.coordinator import TaskHandlerArtifactError, TaskHandlerCandidate

from tests_common.test_utils.paths import AIRFLOW_ROOT_PATH

SCHEMA_VERSION = "2026-06-16"
TYPESCRIPT_FIXTURES = AIRFLOW_ROOT_PATH / "ts-sdk" / "tests" / "cli" / "fixtures"


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


class TestBundlesPackedByAirflowTsPack:
    """A bundle reads whether or not its metadata still carries the task_handlers older packers embedded."""

    @pytest.fixture(
        params=["bundle-v1.min.mjs", "bundle-v1-with-task-handlers.min.mjs"],
        ids=["without-task-handlers", "with-task-handlers"],
    )
    def packed_bundle(self, request, tmp_path):
        bundle = tmp_path / "handlers.min.mjs"
        bundle.write_bytes((TYPESCRIPT_FIXTURES / request.param).read_bytes())
        return bundle

    def test_is_listed_as_a_candidate(self, packed_bundle):
        candidates = NodeCoordinator().list_task_handler_candidates(packed_bundle.parent)

        assert [(c.rel_path, c.error) for c in candidates] == [("handlers.min.mjs", None)]

    def test_is_started_with_its_schema_version(self, packed_bundle):
        command, schema_version = NodeCoordinator(node_executable="node")._build_task_handler_command(
            path=packed_bundle
        )

        assert command == ["node", str(packed_bundle)]
        assert schema_version == SCHEMA_VERSION


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
