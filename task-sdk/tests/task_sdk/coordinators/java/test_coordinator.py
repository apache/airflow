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

import os
import pathlib
import re
import socket
import subprocess
import zipfile
from unittest.mock import MagicMock, patch

import pytest
from uuid6 import uuid7

from airflow.sdk.coordinators.java.coordinator import (
    JavaCoordinator,
    _calculate_classpath,
    _JarInfo,
    _walk_jars,
)
from airflow.sdk.execution_time.coordinator import BaseCoordinator
from airflow.sdk.execution_time.supervisor import ActivitySubprocess
from airflow.sdk.execution_time.workloads.task import TaskInstanceDTO

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    pytest.skip("Coordinator is only compatible with Airflow >= 3.3.0", allow_module_level=True)


def _make_ti(dag_id: str = "test_dag", queue: str = "java") -> TaskInstanceDTO:
    return TaskInstanceDTO(
        id=uuid7(),
        dag_version_id=uuid7(),
        task_id="task_1",
        dag_id=dag_id,
        run_id="run_1",
        try_number=1,
        map_index=-1,
        pool_slots=1,
        queue=queue,
        priority_weight=1,
    )


def _make_jar(
    path: pathlib.Path,
    *,
    main_class: str | None = "com.example.Main",
    schema_version: str | None = None,
) -> pathlib.Path:
    """Write a minimal JAR with (optionally) a Main-Class manifest entry."""
    lines = ["Manifest-Version: 1.0"]
    if main_class:
        lines.append(f"Main-Class: {main_class}")
    if schema_version:
        lines.append(f"Airflow-Supervisor-Schema-Version: {schema_version}")
    manifest = "\n".join(lines) + "\n\n"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("META-INF/MANIFEST.MF", manifest)
    return path


class TestCalculateClasspath:
    def test_single_jar(self, tmp_path):
        jar = tmp_path.joinpath("app.jar")
        jar.write_bytes(b"")
        result = _calculate_classpath([tmp_path])
        assert result == jar.as_posix()

    def test_multiple_jars_all_included(self, tmp_path):
        tmp_path.joinpath("a.jar").write_bytes(b"")
        tmp_path.joinpath("b.jar").write_bytes(b"")
        tmp_path.joinpath("c.jar").write_bytes(b"")
        result = _calculate_classpath([tmp_path])
        entries = set(result.split(os.pathsep))
        assert entries == {
            tmp_path.joinpath("a.jar").as_posix(),
            tmp_path.joinpath("b.jar").as_posix(),
            tmp_path.joinpath("c.jar").as_posix(),
        }

    def test_non_jar_files_excluded(self, tmp_path):
        jar = tmp_path.joinpath("app.jar")
        jar.write_bytes(b"")
        tmp_path.joinpath("readme.txt").write_bytes(b"")
        tmp_path.joinpath("config.yaml").write_bytes(b"")
        result = _calculate_classpath([tmp_path])
        assert result == jar.as_posix()

    def test_empty_directory_returns_empty_string(self, tmp_path):
        result = _calculate_classpath([tmp_path])
        assert result == ""


class TestMainJar:
    def test_returns_main_class_from_jar(self, tmp_path):
        _make_jar(tmp_path.joinpath("app.jar"), main_class="com.example.Main", schema_version="2026-06-16")
        assert _JarInfo.find([tmp_path], "") == _JarInfo("com.example.Main", "2026-06-16")

    def test_no_jars_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match=re.escape(str(tmp_path.resolve()))):
            _JarInfo.find([tmp_path], "")

    def test_jar_without_main_class_not_returned(self, tmp_path):
        _make_jar(tmp_path.joinpath("app.jar"), main_class=None)
        with pytest.raises(FileNotFoundError):
            _JarInfo.find([tmp_path], "")

    def test_jar_with_main_class_but_no_schema_version_raises(self, tmp_path):
        """A JAR with Main-Class but no Airflow-Supervisor-Schema-Version must raise ValueError."""
        _make_jar(tmp_path.joinpath("app.jar"), main_class="com.example.Main")
        with pytest.raises(FileNotFoundError, match="Airflow-Supervisor-Schema-Version"):
            _JarInfo.find([tmp_path], "")

    def test_non_jar_files_skipped(self, tmp_path):
        tmp_path.joinpath("readme.txt").write_bytes(b"not a jar")
        _make_jar(tmp_path.joinpath("app.jar"), main_class="com.example.Main", schema_version="2026-06-16")
        assert _JarInfo.find([tmp_path], "") == _JarInfo("com.example.Main", "2026-06-16")

    def test_first_jar_missing_main_class_falls_through_to_second(self, tmp_path):
        # Alphabetically: a.jar (no Main-Class), b.jar (has Main-Class).
        _make_jar(tmp_path.joinpath("a.jar"), main_class=None)
        _make_jar(tmp_path.joinpath("b.jar"), main_class="com.example.Fallback", schema_version="2026-06-16")
        assert _JarInfo.find([tmp_path], "") == _JarInfo("com.example.Fallback", "2026-06-16")

    def test_fully_qualified_class_name_preserved(self, tmp_path):
        _make_jar(
            tmp_path.joinpath("app.jar"),
            main_class="org.apache.airflow.sdk.java.TaskRunner",
            schema_version="2026-06-16",
        )
        assert _JarInfo.find([tmp_path], "") == _JarInfo(
            main_class="org.apache.airflow.sdk.java.TaskRunner",
            schema_version="2026-06-16",
        )

    def test_find_by_explicit_main_class(self, tmp_path):
        """When a main_class filter is given, only the matching JAR is returned."""
        _make_jar(tmp_path.joinpath("a.jar"), main_class="com.example.Alpha", schema_version="2026-06-16")
        _make_jar(tmp_path.joinpath("b.jar"), main_class="com.example.Beta", schema_version="2026-06-16")
        result = _JarInfo.find([tmp_path], "com.example.Beta")
        assert result.main_class == "com.example.Beta"

    def test_find_by_explicit_main_class_not_present_raises(self, tmp_path):
        """When no JAR matches the main_class filter, FileNotFoundError is raised."""
        _make_jar(tmp_path.joinpath("app.jar"), main_class="com.example.Main", schema_version="2026-06-16")
        with pytest.raises(FileNotFoundError, match="com.example.Missing"):
            _JarInfo.find([tmp_path], "com.example.Missing")

    def test_symlink_cycle_does_not_infinite_recurse(self, tmp_path):
        nested = tmp_path / "inner"
        nested.mkdir()
        _make_jar(nested / "app.jar", main_class="com.example.Loop", schema_version="2026-06-16")
        loop = nested / "loop"
        try:
            loop.symlink_to(tmp_path)
        except (OSError, NotImplementedError):
            pytest.skip("symlinks not supported on this platform")

        result = _JarInfo.find([tmp_path], "com.example.Loop")
        assert result == _JarInfo("com.example.Loop", "2026-06-16")


class TestWalkJars:
    def test_skips_directory_whose_key_is_already_in_seen_dirs(self, tmp_path):
        """A directory whose (st_dev, st_ino) is already in seen_dirs is skipped."""
        _make_jar(tmp_path / "app.jar", main_class="com.example.Main", schema_version="2026-06-16")
        st = tmp_path.stat()
        seen_dirs: set[tuple[int, int]] = {(st.st_dev, st.st_ino)}
        assert list(_walk_jars([tmp_path], seen_dirs)) == []

    def test_records_visited_directories_in_seen_dirs(self, tmp_path):
        """Every directory descended into is added to seen_dirs."""
        sub = tmp_path / "sub"
        sub.mkdir()
        _make_jar(sub / "app.jar", main_class="com.example.Main", schema_version="2026-06-16")
        seen_dirs: set[tuple[int, int]] = set()
        list(_walk_jars([tmp_path], seen_dirs))
        assert (tmp_path.stat().st_dev, tmp_path.stat().st_ino) in seen_dirs
        assert (sub.stat().st_dev, sub.stat().st_ino) in seen_dirs

    def test_symlink_cycle_yields_each_jar_once(self, tmp_path):
        """A symlink that loops back to an ancestor must not yield the same JAR twice."""
        nested = tmp_path / "inner"
        nested.mkdir()
        jar = _make_jar(nested / "app.jar", main_class="com.example.Loop", schema_version="2026-06-16")
        loop = nested / "loop"
        try:
            loop.symlink_to(tmp_path)
        except (OSError, NotImplementedError):
            pytest.skip("symlinks not supported on this platform")

        seen_dirs: set[tuple[int, int]] = set()
        yielded = list(_walk_jars([tmp_path], seen_dirs))
        assert [p.resolve() for p in yielded] == [jar.resolve()]

    def test_skip_logged_when_directory_revisited(self, tmp_path):
        """A revisited directory triggers the 'Skipping already-visited directory' debug log."""
        sub = tmp_path / "sub"
        sub.mkdir()
        seen_dirs: set[tuple[int, int]] = {(sub.stat().st_dev, sub.stat().st_ino)}
        with patch("airflow.sdk.coordinators.java.coordinator.log") as mock_log:
            list(_walk_jars([sub], seen_dirs))
        mock_log.debug.assert_any_call("Skipping already-visited directory", path=sub)


class TestJavaCoordinatorAttributes:
    def test_default_kwargs(self):
        coordinator = JavaCoordinator(jars_root="/airflow/java-bundles")
        assert coordinator.java_executable == "java"
        assert coordinator.jvm_args == []
        assert coordinator.jars_root == [pathlib.Path("/airflow/java-bundles")]

    def test_custom_kwargs(self):
        coordinator = JavaCoordinator(
            java_executable="/opt/java/bin/java",
            jvm_args=["-Xmx512m", "-Xms256m"],
            jars_root=["/airflow/java-bundles"],
        )
        assert coordinator.java_executable == "/opt/java/bin/java"
        assert coordinator.jvm_args == ["-Xmx512m", "-Xms256m"]
        assert coordinator.jars_root == [pathlib.Path("/airflow/java-bundles")]


@pytest.fixture
def jars_root(tmp_path):
    _make_jar(tmp_path.joinpath("app.jar"), main_class="com.example.TaskRunner", schema_version="2026-06-16")
    return tmp_path


@pytest.fixture
def mock_client(make_ti_context):
    client = MagicMock()
    client.task_instances.start.return_value = make_ti_context()
    return client


class TestJavaCoordinatorExecuteTask:
    def _captured_popen_cmd(
        self,
        jars_root: pathlib.Path,
        mock_client,
        *,
        java_executable: str = "java",
        jvm_args: list[str] | None = None,
    ) -> list[str]:
        """Run execute_task with mocked subprocess and return the command list."""
        ti = _make_ti()
        coordinator = JavaCoordinator(
            java_executable=java_executable,
            jvm_args=jvm_args or [],
            jars_root=jars_root,
        )

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = 12345
        comm_sock = MagicMock(spec=socket.socket)
        logs_sock = MagicMock(spec=socket.socket)
        popen_calls: list = []

        def capture_popen(cmd, **kwargs):
            popen_calls.append(cmd)
            return mock_proc

        with (
            patch(
                "airflow.sdk.coordinators._subprocess.subprocess.Popen",
                side_effect=capture_popen,
            ),
            patch(
                "airflow.sdk.coordinators._subprocess._accept_connections",
                side_effect=lambda servers, drains, proc, **kw: (
                    {servers["comm"]: comm_sock, servers["logs"]: logs_sock},
                    {soc: b"" for soc in drains.values()},
                ),
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers"),
            patch.object(ActivitySubprocess, "_on_child_started"),
            patch.object(ActivitySubprocess, "wait", return_value=0),
            patch("psutil.Process"),
        ):
            coordinator.execute_task(
                what=ti,
                dag_rel_path="dags/test.jar",
                bundle_info=MagicMock(),
                client=mock_client,
                subprocess_logs_to_stdout=False,
            )

        assert popen_calls, "subprocess.Popen was not called"
        return popen_calls[0]

    def test_java_executable_is_first_arg(self, jars_root, mock_client):
        cmd = self._captured_popen_cmd(
            jars_root, mock_client, java_executable="/usr/lib/jvm/java-17/bin/java"
        )
        assert cmd[0] == "/usr/lib/jvm/java-17/bin/java"

    def test_classpath_flag_and_value_present(self, jars_root, mock_client):
        cmd = self._captured_popen_cmd(jars_root, mock_client)
        assert "-classpath" in cmd
        cp_idx = cmd.index("-classpath")
        classpath = cmd[cp_idx + 1]
        assert jars_root.joinpath("app.jar").as_posix() in classpath

    def test_main_class_present(self, jars_root, mock_client):
        cmd = self._captured_popen_cmd(jars_root, mock_client)
        assert "com.example.TaskRunner" in cmd

    def test_comm_and_logs_args_present(self, jars_root, mock_client):
        cmd = self._captured_popen_cmd(jars_root, mock_client)
        comm_args = [a for a in cmd if a.startswith("--comm=")]
        logs_args = [a for a in cmd if a.startswith("--logs=")]
        assert len(comm_args) == 1
        assert len(logs_args) == 1

    def test_comm_and_logs_contain_port(self, jars_root, mock_client):
        cmd = self._captured_popen_cmd(jars_root, mock_client)
        comm_arg = next(a for a in cmd if a.startswith("--comm="))
        logs_arg = next(a for a in cmd if a.startswith("--logs="))
        # format is host:port
        assert ":" in comm_arg.split("=", 1)[1]
        assert ":" in logs_arg.split("=", 1)[1]

    def test_jvm_args_inserted_before_main_class(self, jars_root, mock_client):
        cmd = self._captured_popen_cmd(jars_root, mock_client, jvm_args=["-Xmx512m", "-Dsome.prop=value"])
        main_idx = cmd.index("com.example.TaskRunner")
        for jvm_arg in ["-Xmx512m", "-Dsome.prop=value"]:
            assert jvm_arg in cmd
            assert cmd.index(jvm_arg) < main_idx

    def test_comm_and_logs_after_main_class(self, jars_root, mock_client):
        cmd = self._captured_popen_cmd(jars_root, mock_client)
        main_idx = cmd.index("com.example.TaskRunner")
        comm_idx = next(i for i, a in enumerate(cmd) if a.startswith("--comm="))
        logs_idx = next(i for i, a in enumerate(cmd) if a.startswith("--logs="))
        assert comm_idx > main_idx
        assert logs_idx > main_idx

    def test_returns_execution_result(self, jars_root, mock_client):
        ti = _make_ti()
        coordinator = JavaCoordinator(jars_root=jars_root)

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.pid = 99999
        comm_sock = MagicMock(spec=socket.socket)
        logs_sock = MagicMock(spec=socket.socket)

        with (
            patch("subprocess.Popen", return_value=mock_proc),
            patch(
                "airflow.sdk.coordinators._subprocess._accept_connections",
                side_effect=lambda servers, drains, proc, **kw: (
                    {servers["comm"]: comm_sock, servers["logs"]: logs_sock},
                    {soc: b"" for soc in drains.values()},
                ),
            ),
            patch.object(ActivitySubprocess, "_register_pipe_readers"),
            patch.object(ActivitySubprocess, "_on_child_started"),
            patch.object(ActivitySubprocess, "wait", return_value=0),
            patch("psutil.Process"),
        ):
            result = coordinator.execute_task(
                what=ti,
                dag_rel_path="dags/test.jar",
                bundle_info=MagicMock(),
                client=mock_client,
                subprocess_logs_to_stdout=False,
            )

        assert isinstance(result, BaseCoordinator.ExecutionResult)
        assert result.exit_code == 0
