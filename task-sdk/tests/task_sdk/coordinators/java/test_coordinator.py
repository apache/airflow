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
import os
import pathlib
import re
import socket
import subprocess
from unittest.mock import ANY, MagicMock, patch

import pytest
from task_sdk.coordinators.java._jar_test_utils import make_jar
from uuid6 import uuid7

from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance
from airflow.sdk.coordinators._subprocess import TASK_HANDLER_PARSING_SCHEMA_VERSION, _PopenActivitySubprocess
from airflow.sdk.coordinators.java.coordinator import (
    JavaCoordinator,
    _calculate_classpath,
    _JarInfo,
    _JarMetadata,
    _walk_jars,
)
from airflow.sdk.execution_time.coordinator import BaseCoordinator, TaskLaunchError
from airflow.sdk.execution_time.supervisor import ActivitySubprocess
from airflow.sdk.importers import reset_importer_registry

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    pytest.skip("Coordinator is only compatible with Airflow >= 3.3.0", allow_module_level=True)


def _make_ti(dag_id: str = "test_dag", queue: str = "java") -> TaskInstance:
    return TaskInstance(
        id=uuid7(),
        dag_version_id=uuid7(),
        task_id="task_1",
        dag_id=dag_id,
        run_id="run_1",
        try_number=1,
        map_index=-1,
        queue=queue,
    )


def _make_jar(
    path: pathlib.Path,
    *,
    main_class: str | None = "com.example.Main",
    schema_version: str | None = None,
) -> pathlib.Path:
    """Write a minimal JAR with (optionally) a Main-Class manifest entry."""
    attributes = {"Manifest-Version": "1.0"}
    if main_class:
        attributes["Main-Class"] = main_class
    if schema_version:
        attributes["Airflow-Supervisor-Schema-Version"] = schema_version
    return make_jar(path, attributes=attributes)


class TestJarMetadata:
    def test_reads_a_folded_main_class(self, tmp_path):
        main_class = "org.apache.airflow.example.nativedag.generated.VeryLongNativeDagBundleBuilder"
        jar = make_jar(tmp_path / "app.jar", attributes={"Main-Class": main_class})

        assert _JarMetadata.from_jar(jar) == _JarMetadata(main_class, None)

    def test_jar_without_manifest_gives_none(self, tmp_path):
        jar = make_jar(tmp_path / "lib.jar", entries={"com/example/Lib.class": b"\xca\xfe"})

        assert _JarMetadata.from_jar(jar) is None

    def test_non_zip_gives_none(self, tmp_path):
        jar = tmp_path / "broken.jar"
        jar.write_bytes(b"not a zip")

        assert _JarMetadata.from_jar(jar) is None

    @pytest.mark.parametrize("kind", ["missing", "directory"])
    def test_unreadable_jar_gives_none(self, tmp_path, kind):
        jar = tmp_path / "gone.jar"
        if kind == "directory":
            jar.mkdir()

        assert _JarMetadata.from_jar(jar) is None

    def test_permission_error_propagates(self, tmp_path):
        jar = tmp_path / "locked.jar"
        with (
            patch(
                "airflow.sdk.coordinators.java.coordinator.zipfile.ZipFile",
                side_effect=PermissionError(13, "Permission denied", str(jar)),
            ),
            pytest.raises(PermissionError, match="locked.jar"),
        ):
            _JarMetadata.from_jar(jar)


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
        jar = _make_jar(
            tmp_path.joinpath("app.jar"), main_class="com.example.Main", schema_version="2026-06-16"
        )
        assert _JarInfo.find([tmp_path], "") == _JarInfo(jar.resolve(), "com.example.Main", "2026-06-16")

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
        jar = _make_jar(
            tmp_path.joinpath("app.jar"), main_class="com.example.Main", schema_version="2026-06-16"
        )
        assert _JarInfo.find([tmp_path], "") == _JarInfo(jar.resolve(), "com.example.Main", "2026-06-16")

    def test_first_jar_missing_main_class_falls_through_to_second(self, tmp_path):
        # Alphabetically: a.jar (no Main-Class), b.jar (has Main-Class).
        _make_jar(tmp_path.joinpath("a.jar"), main_class=None)
        jar = _make_jar(
            tmp_path.joinpath("b.jar"), main_class="com.example.Fallback", schema_version="2026-06-16"
        )
        assert _JarInfo.find([tmp_path], "") == _JarInfo(jar.resolve(), "com.example.Fallback", "2026-06-16")

    def test_fully_qualified_class_name_preserved(self, tmp_path):
        jar = _make_jar(
            tmp_path.joinpath("app.jar"),
            main_class="org.apache.airflow.sdk.java.TaskRunner",
            schema_version="2026-06-16",
        )
        assert _JarInfo.find([tmp_path], "") == _JarInfo(
            path=jar.resolve(),
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
        jar = _make_jar(nested / "app.jar", main_class="com.example.Loop", schema_version="2026-06-16")
        loop = nested / "loop"
        try:
            loop.symlink_to(tmp_path)
        except (OSError, NotImplementedError):
            pytest.skip("symlinks not supported on this platform")

        result = _JarInfo.find([tmp_path], "com.example.Loop")
        assert result == _JarInfo(jar.resolve(), "com.example.Loop", "2026-06-16")

    @pytest.mark.parametrize(
        ("app", "sdk"),
        [
            pytest.param("app.jar", "libs/airflow-sdk.jar", id="main-class-first"),
            pytest.param("x-app.jar", "airflow-sdk.jar", id="schema-version-first"),
        ],
    )
    def test_records_the_jar_that_sets_the_main_class(self, tmp_path, app, sdk):
        # A thin JAR leaves the schema version to the airflow-sdk JAR, and the scan stops at whichever
        # comes second.
        tmp_path.joinpath("libs").mkdir()
        jar = _make_jar(tmp_path / app, main_class="com.example.App")
        _make_jar(tmp_path / sdk, main_class=None, schema_version="2026-10-30")

        assert _JarInfo.find([tmp_path], "") == _JarInfo(jar.resolve(), "com.example.App", "2026-10-30")

    def test_for_jar_records_the_jar_it_runs(self, tmp_path):
        jar = _make_jar(tmp_path / "app.jar", main_class="com.example.App")
        _make_jar(tmp_path / "airflow-sdk.jar", main_class=None, schema_version="2026-10-30")

        assert _JarInfo.for_jar([tmp_path], jar, "com.example.App", None) == _JarInfo(
            jar.resolve(), "com.example.App", "2026-10-30"
        )


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
        coordinator = JavaCoordinator()
        assert coordinator.java_executable == "java"
        assert coordinator.jvm_args == []
        assert coordinator.task_handler_bundle_name is None
        # main_class stays optional: the entrypoint is auto-detected from the bundle scan.
        assert coordinator.main_class == ""

    def test_custom_kwargs(self):
        coordinator = JavaCoordinator(
            java_executable="/opt/java/bin/java",
            jvm_args=["-Xmx512m", "-Xms256m"],
            task_handler_bundle_name="java-task-handlers",
        )
        assert coordinator.java_executable == "/opt/java/bin/java"
        assert coordinator.jvm_args == ["-Xmx512m", "-Xms256m"]
        assert coordinator.task_handler_bundle_name == "java-task-handlers"

    def test_build_command_scans_passed_roots_in_colocated_mode(self, tmp_path):
        _make_jar(tmp_path / "app.jar", main_class="com.example.TaskRunner", schema_version="2026-06-16")
        coordinator = JavaCoordinator(main_class="com.example.TaskRunner")
        with coordinator._set_scan_roots([tmp_path]):
            command, schema_version = coordinator._build_execute_task_command(what=_make_ti())
        assert command[0] == "java"
        assert command[-1] == "com.example.TaskRunner"
        assert schema_version == "2026-06-16"


@pytest.fixture
def jars_dir(tmp_path):
    _make_jar(tmp_path.joinpath("app.jar"), main_class="com.example.TaskRunner", schema_version="2026-06-16")
    return tmp_path


@pytest.fixture
def java_task_handlers(jars_dir):
    """Register *jars_dir* as the ``java-task-handlers`` Dag bundle and return that name."""
    bundle = {
        "name": "java-task-handlers",
        "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
        "kwargs": {"path": str(jars_dir)},
    }
    with conf_vars({("dag_processor", "dag_bundle_config_list"): json.dumps([bundle])}):
        yield bundle["name"]


@pytest.fixture
def mock_client(make_ti_context):
    client = MagicMock()
    client.task_instances.start.return_value = make_ti_context()
    return client


class TestJavaCoordinatorExecuteTask:
    def _captured_popen_cmd(
        self,
        bundle_name: str,
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
            task_handler_bundle_name=bundle_name,
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

    def test_java_executable_is_first_arg(self, java_task_handlers, mock_client):
        cmd = self._captured_popen_cmd(
            java_task_handlers, mock_client, java_executable="/usr/lib/jvm/java-17/bin/java"
        )
        assert cmd[0] == "/usr/lib/jvm/java-17/bin/java"

    def test_classpath_flag_and_value_present(self, jars_dir, java_task_handlers, mock_client):
        cmd = self._captured_popen_cmd(java_task_handlers, mock_client)
        assert "-classpath" in cmd
        cp_idx = cmd.index("-classpath")
        classpath = cmd[cp_idx + 1]
        assert jars_dir.joinpath("app.jar").as_posix() in classpath

    def test_main_class_present(self, java_task_handlers, mock_client):
        cmd = self._captured_popen_cmd(java_task_handlers, mock_client)
        assert "com.example.TaskRunner" in cmd

    def test_comm_and_logs_args_present(self, java_task_handlers, mock_client):
        cmd = self._captured_popen_cmd(java_task_handlers, mock_client)
        comm_args = [a for a in cmd if a.startswith("--comm=")]
        logs_args = [a for a in cmd if a.startswith("--logs=")]
        assert len(comm_args) == 1
        assert len(logs_args) == 1

    def test_comm_and_logs_contain_port(self, java_task_handlers, mock_client):
        cmd = self._captured_popen_cmd(java_task_handlers, mock_client)
        comm_arg = next(a for a in cmd if a.startswith("--comm="))
        logs_arg = next(a for a in cmd if a.startswith("--logs="))
        # format is host:port
        assert ":" in comm_arg.split("=", 1)[1]
        assert ":" in logs_arg.split("=", 1)[1]

    def test_jvm_args_inserted_before_main_class(self, java_task_handlers, mock_client):
        cmd = self._captured_popen_cmd(
            java_task_handlers, mock_client, jvm_args=["-Xmx512m", "-Dsome.prop=value"]
        )
        main_idx = cmd.index("com.example.TaskRunner")
        for jvm_arg in ["-Xmx512m", "-Dsome.prop=value"]:
            assert jvm_arg in cmd
            assert cmd.index(jvm_arg) < main_idx

    def test_comm_and_logs_after_main_class(self, java_task_handlers, mock_client):
        cmd = self._captured_popen_cmd(java_task_handlers, mock_client)
        main_idx = cmd.index("com.example.TaskRunner")
        comm_idx = next(i for i, a in enumerate(cmd) if a.startswith("--comm="))
        logs_idx = next(i for i, a in enumerate(cmd) if a.startswith("--logs="))
        assert comm_idx > main_idx
        assert logs_idx > main_idx

    def test_returns_execution_result(self, java_task_handlers, mock_client):
        ti = _make_ti()
        coordinator = JavaCoordinator(task_handler_bundle_name=java_task_handlers)

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


class TestJavaCoordinatorExecuteNativeDag:
    """With a JavaCoordinator configured, a task of a native Java Dag runs the JAR of its Dag."""

    @pytest.fixture(autouse=True)
    def _java_coordinator_config(self):
        coordinators = {"java": {"classpath": "airflow.sdk.coordinators.java.JavaCoordinator"}}
        reset_importer_registry()
        with conf_vars({("sdk", "coordinators"): json.dumps(coordinators)}):
            yield
        reset_importer_registry()

    @pytest.fixture
    def mock_start(self, tmp_path):
        _make_jar(tmp_path / "a.jar", main_class="com.example.A", schema_version="2026-06-16")
        _make_jar(tmp_path / "b.jar", main_class="com.example.B", schema_version="2026-10-30")
        bundle = MagicMock(path=tmp_path, version="v1")
        bundle.name = "dags-folder"
        with (
            patch("airflow.sdk.coordinators._subprocess._initialize_pinned_bundle", return_value=bundle),
            patch("airflow.sdk.coordinators._subprocess.BundleVersionLock"),
            patch.object(_PopenActivitySubprocess, "start") as mock_start,
        ):
            mock_start.return_value.wait.return_value = 0
            yield mock_start

    def _execute(self, rel_path: str, client, coordinator: JavaCoordinator | None = None):
        return (coordinator or JavaCoordinator()).execute_task(
            what=_make_ti(),
            dag_rel_path=rel_path,
            bundle_info=BundleInfo(name="dags-folder", version="v1"),
            client=client,
            subprocess_logs_to_stdout=False,
        )

    def test_runs_the_jar_of_the_dag_and_not_the_first_jar_by_path(self, mock_start, mock_client):
        self._execute("b.jar", mock_client)

        assert mock_start.call_args.kwargs["command"][-1] == "com.example.B"
        assert mock_start.call_args.kwargs["subprocess_schema_version"] == "2026-10-30"

    def test_a_task_of_a_python_dag_still_scans_the_bundle(self, mock_start, mock_client):
        self._execute("dag.py", mock_client)

        assert mock_start.call_args.kwargs["command"][-1] == "com.example.A"

    def test_fails_without_starting_the_jvm_for_a_jar_the_coordinator_cannot_run(
        self, mock_start, mock_client
    ):
        with pytest.raises(
            TaskLaunchError, match="runs 'com.example.B', but .* main_class is 'com.example.A'"
        ):
            self._execute("b.jar", mock_client, JavaCoordinator(main_class="com.example.A"))

        mock_start.assert_not_called()


class TestBuildDagFileCommand:
    def _build(self, coordinator: JavaCoordinator, root: pathlib.Path, jar: pathlib.Path):
        with coordinator._set_scan_roots([root]):
            return coordinator._build_dag_file_command(what=_make_ti(), path=jar)

    @pytest.mark.parametrize("parsed", ["a.jar", "b.jar"])
    def test_runs_the_main_class_of_the_jar_the_dag_was_parsed_from(self, tmp_path, parsed):
        _make_jar(tmp_path / "a.jar", main_class="com.example.A", schema_version="2026-10-30")
        _make_jar(tmp_path / "b.jar", main_class="com.example.B", schema_version="2026-10-30")
        coordinator = JavaCoordinator()

        command, schema_version = self._build(coordinator, tmp_path, tmp_path / parsed)

        expected = {"a.jar": "com.example.A", "b.jar": "com.example.B"}[parsed]
        assert command[-1] == expected
        assert schema_version == "2026-10-30"
        with coordinator._set_scan_roots([tmp_path]):
            assert coordinator._build_parse_dag_command(path=tmp_path / parsed) == (command, schema_version)

    def test_runs_a_jar_whose_schema_version_is_too_old_to_parse(self, tmp_path):
        jar = _make_jar(tmp_path / "app.jar", main_class="com.example.Dags", schema_version="2026-06-16")

        _, schema_version = self._build(JavaCoordinator(), tmp_path, jar)

        assert schema_version == "2026-06-16"

    def test_rejects_a_jar_whose_main_class_differs_from_the_pinned_one(self, tmp_path):
        jar = _make_jar(tmp_path / "app.jar", main_class="com.example.Other", schema_version="2026-10-30")

        with pytest.raises(ValueError, match="main_class is 'com.example.Dags'"):
            self._build(JavaCoordinator(main_class="com.example.Dags"), tmp_path, jar)

    def test_rejects_a_main_class_another_jar_sets(self, tmp_path):
        new = _make_jar(tmp_path / "etl-new.jar", main_class="com.example.A", schema_version="2026-10-30")
        _make_jar(tmp_path / "etl-old.jar", main_class="com.example.A", schema_version="2026-10-30")

        with pytest.raises(ValueError, match="all set Main-Class 'com.example.A'"):
            self._build(JavaCoordinator(), tmp_path, new)


class TestBuildParseDagCommand:
    def _build(self, coordinator: JavaCoordinator, root: pathlib.Path, jar: pathlib.Path):
        with coordinator._set_scan_roots([root]):
            return coordinator._build_parse_dag_command(path=jar)

    def test_fat_jar(self, tmp_path):
        jar = _make_jar(tmp_path / "app.jar", main_class="com.example.Dags", schema_version="2026-10-30")
        coordinator = JavaCoordinator(java_executable="/opt/java/bin/java", jvm_args=["-Xmx256m"])

        command, schema_version = self._build(coordinator, tmp_path, jar)

        assert command == ["/opt/java/bin/java", "-classpath", jar.as_posix(), "-Xmx256m", "com.example.Dags"]
        assert schema_version == "2026-10-30"

    def test_thin_jar_takes_the_schema_version_from_a_sibling(self, tmp_path):
        jar = _make_jar(tmp_path / "app.jar", main_class="com.example.Dags")
        (tmp_path / "libs").mkdir()
        sdk = _make_jar(tmp_path / "libs" / "airflow-sdk.jar", main_class=None, schema_version="2026-10-30")

        command, schema_version = self._build(JavaCoordinator(), tmp_path, jar)

        assert command[2].split(os.pathsep) == [jar.as_posix(), sdk.as_posix()]
        assert command[-1] == "com.example.Dags"
        assert schema_version == "2026-10-30"

    def test_matches_the_execute_command(self, tmp_path):
        jar = _make_jar(tmp_path / "app.jar", main_class="com.example.Dags", schema_version="2026-10-30")
        _make_jar(tmp_path / "dep.jar", main_class=None)
        coordinator = JavaCoordinator(jvm_args=["-Xmx256m"])

        parse = self._build(coordinator, tmp_path, jar)
        with coordinator._set_scan_roots([tmp_path]):
            execute = coordinator._build_execute_task_command(what=_make_ti())

        assert parse == execute

    def test_takes_the_schema_version_of_the_parsed_jar(self, tmp_path):
        _make_jar(tmp_path / "a.jar", main_class="com.example.A", schema_version="2026-06-16")
        jar = _make_jar(tmp_path / "b.jar", main_class="com.example.B", schema_version="2026-10-30")

        _, schema_version = self._build(JavaCoordinator(), tmp_path, jar)

        assert schema_version == "2026-10-30"

    @pytest.mark.parametrize("own", [True, False], ids=["own", "sibling"])
    def test_rejects_a_schema_version_that_cannot_parse(self, tmp_path, own):
        jar = _make_jar(
            tmp_path / "app.jar", main_class="com.example.Dags", schema_version="2026-06-16" if own else None
        )
        if not own:
            _make_jar(tmp_path / "sdk.jar", main_class=None, schema_version="2026-06-16")

        with pytest.raises(
            ValueError,
            match=re.escape(
                f"{jar} uses supervisor schema 2026-06-16, which cannot parse Dags; "
                "rebuild it with a newer Java SDK or list it in .airflowignore"
            ),
        ):
            self._build(JavaCoordinator(), tmp_path, jar)

    def test_rejects_a_main_class_another_jar_sets(self, tmp_path):
        new = _make_jar(tmp_path / "etl-new.jar", main_class="com.example.A", schema_version="2026-10-30")
        old = _make_jar(tmp_path / "etl-old.jar", main_class="com.example.A", schema_version="2026-10-30")

        with pytest.raises(ValueError, match="all set Main-Class 'com.example.A'") as excinfo:
            self._build(JavaCoordinator(), tmp_path, new)

        assert os.fspath(new) in str(excinfo.value)
        assert os.fspath(old) in str(excinfo.value)
        assert "Keep one in the bundle" in str(excinfo.value)

    @pytest.mark.parametrize(
        ("attributes", "match"),
        [
            pytest.param(None, "has no META-INF/MANIFEST.MF", id="no-manifest"),
            pytest.param({"Manifest-Version": "1.0"}, "sets no Main-Class", id="no-main-class"),
            pytest.param({"Main-Class": "com.example.Other"}, "main_class is 'com.example.Dags'", id="pin"),
        ],
    )
    def test_rejects_a_jar_it_cannot_run(self, tmp_path, attributes, match):
        jar = make_jar(tmp_path / "app.jar", attributes=attributes, entries={"a.class": b""})

        with pytest.raises(ValueError, match=match):
            self._build(JavaCoordinator(main_class="com.example.Dags"), tmp_path, jar)

    def test_rejects_a_non_zip(self, tmp_path):
        jar = tmp_path / "broken.jar"
        jar.write_bytes(b"not a zip")

        with pytest.raises(ValueError, match="broken.jar is not a valid JAR: File is not a zip file"):
            self._build(JavaCoordinator(), tmp_path, jar)

    def test_rejects_a_jar_deleted_after_discovery(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="gone.jar"):
            self._build(JavaCoordinator(), tmp_path, tmp_path / "gone.jar")

    @patch("airflow.sdk.coordinators._subprocess._set_close_on_exec_above_stderr", autospec=True)
    @patch("airflow.sdk.coordinators._subprocess.signal.signal", autospec=True)
    @patch("airflow.sdk.coordinators._subprocess.os.execvpe", autospec=True, side_effect=OSError("exec"))
    def test_parse_dag_execs_the_jvm(self, mock_execvpe, mock_signal, mock_close_on_exec, tmp_path):
        jar = _make_jar(tmp_path / "app.jar", main_class="com.example.Dags", schema_version="2026-10-30")
        reported: list[str | None] = []

        with pytest.raises(OSError, match="exec"):
            JavaCoordinator().parse_dag(
                path=jar,
                bundle_path=tmp_path,
                comm_address=("127.0.0.1", 1001),
                logs_address=("127.0.0.1", 1002),
                report_schema_version=reported.append,
            )

        assert reported == ["2026-10-30"]
        argv = mock_execvpe.call_args.args[1]
        assert argv == [
            "java",
            "-classpath",
            jar.as_posix(),
            "com.example.Dags",
            "--comm=127.0.0.1:1001",
            "--logs=127.0.0.1:1002",
        ]


class TestFindTaskHandlerArtifact:
    @pytest.mark.parametrize("dag_id", ["etl", "another_dag"])
    def test_finds_the_jar_whose_main_class_a_task_runs(self, tmp_path, dag_id):
        # A task runs the Main-Class the scan picks, whatever its Dag, so the Dag id plays no part.
        jar = _make_jar(tmp_path / "a.jar", main_class="com.example.A", schema_version="2026-10-30")
        _make_jar(tmp_path / "b.jar", main_class="com.example.B", schema_version="2026-06-16")
        coordinator = JavaCoordinator()
        with coordinator._set_scan_roots([tmp_path]):
            command, schema_version = coordinator._build_execute_task_command(what=_make_ti(dag_id=dag_id))

        artifact = coordinator._find_task_handler_artifact(bundle_path=tmp_path, dag_id=dag_id)

        assert command[-1] == "com.example.A"
        assert artifact.path == jar.resolve()
        assert artifact.schema_version == schema_version

    def test_finds_the_jar_of_the_configured_main_class(self, tmp_path):
        _make_jar(tmp_path / "a.jar", main_class="com.example.A", schema_version="2026-10-30")
        jar = _make_jar(tmp_path / "b.jar", main_class="com.example.B", schema_version="2026-10-30")

        artifact = JavaCoordinator(main_class="com.example.B")._find_task_handler_artifact(
            bundle_path=tmp_path, dag_id="etl"
        )

        assert artifact.path == jar.resolve()

    def test_finds_a_thin_jar_whose_schema_version_another_jar_sets(self, tmp_path):
        jar = _make_jar(tmp_path / "app.jar", main_class="com.example.App")
        (tmp_path / "libs").mkdir()
        _make_jar(tmp_path / "libs" / "airflow-sdk.jar", main_class=None, schema_version="2026-10-30")

        artifact = JavaCoordinator()._find_task_handler_artifact(bundle_path=tmp_path, dag_id="etl")

        assert artifact.path == jar.resolve()
        assert artifact.schema_version == "2026-10-30"

    def test_resolves_a_symlinked_bundle_root(self, tmp_path):
        real = tmp_path / "real"
        (real / "team-a").mkdir(parents=True)
        target = _make_jar(
            real / "team-a" / "app.jar", main_class="com.example.App", schema_version="2026-10-30"
        )
        root = tmp_path / "bundle"
        try:
            root.symlink_to(real, target_is_directory=True)
        except (OSError, NotImplementedError):
            pytest.skip("symlinks not supported on this platform")

        artifact = JavaCoordinator()._find_task_handler_artifact(bundle_path=root, dag_id="etl")

        assert artifact.path == target.resolve()
        assert artifact.path.relative_to(root.resolve()) == pathlib.Path("team-a", "app.jar")

    @pytest.mark.parametrize(
        ("main_class", "schema_version", "error", "match"),
        [
            pytest.param(
                None, "2026-10-30", FileNotFoundError, "with Main-Class metadata", id="no-main-class"
            ),
            pytest.param(
                "com.example.App",
                None,
                FileNotFoundError,
                "Airflow-Supervisor-Schema-Version",
                id="no-version",
            ),
            pytest.param(
                "com.example.App",
                "1999-01-01",
                ValueError,
                "not found in supervisor schema",
                id="unknown-version",
            ),
        ],
    )
    def test_raises_when_a_task_finds_no_jar_it_can_run(
        self, tmp_path, main_class, schema_version, error, match
    ):
        _make_jar(tmp_path / "app.jar", main_class=main_class, schema_version=schema_version)

        with pytest.raises(error, match=match):
            JavaCoordinator()._find_task_handler_artifact(bundle_path=tmp_path, dag_id="etl")


class TestBuildParseTaskHandlerCommand:
    def _build(self, coordinator: JavaCoordinator, root: pathlib.Path, path: pathlib.Path):
        with coordinator._set_scan_roots([root]):
            return coordinator._build_parse_task_handler_command(path=path)

    def test_runs_the_command_a_task_runs(self, tmp_path):
        _make_jar(tmp_path / "a.jar", main_class="com.example.A", schema_version="2026-10-30")
        _make_jar(tmp_path / "b.jar", main_class="com.example.B", schema_version="2026-10-30")
        (tmp_path / "lib").mkdir()
        _make_jar(tmp_path / "lib" / "dep.jar", main_class=None)
        coordinator = JavaCoordinator(java_executable="/opt/java/bin/java", jvm_args=["-Xmx1g"])
        artifact = coordinator._find_task_handler_artifact(bundle_path=tmp_path, dag_id="etl")
        with coordinator._set_scan_roots([tmp_path]):
            task = coordinator._build_execute_task_command(what=_make_ti())

        command, schema_version = self._build(coordinator, tmp_path, artifact.path)

        classpath = os.pathsep.join(
            (tmp_path / name).as_posix() for name in ("a.jar", "b.jar", "lib/dep.jar")
        )
        assert command == ["/opt/java/bin/java", "-classpath", classpath, "-Xmx1g", "com.example.A"]
        assert (command, schema_version) == task

    def test_scans_the_scan_roots_and_not_the_directory_of_the_jar(self, tmp_path):
        (tmp_path / "app").mkdir()
        (tmp_path / "libs").mkdir()
        jar = _make_jar(tmp_path / "app" / "app.jar", main_class="com.example.App")
        _make_jar(tmp_path / "libs" / "airflow-sdk.jar", main_class=None, schema_version="2026-10-30")
        coordinator = JavaCoordinator()
        with coordinator._set_scan_roots([tmp_path]):
            task = coordinator._build_execute_task_command(what=_make_ti())

        assert self._build(coordinator, tmp_path, jar) == task

    def test_runs_the_configured_main_class(self, tmp_path):
        _make_jar(tmp_path / "a.jar", main_class="com.example.A", schema_version="2026-10-30")
        jar = _make_jar(tmp_path / "b.jar", main_class="com.example.B", schema_version="2026-10-30")

        command, _ = self._build(JavaCoordinator(main_class="com.example.B"), tmp_path, jar)

        assert command[-1] == "com.example.B"

    def test_a_thin_jar_takes_the_schema_version_from_the_sdk_jar(self, tmp_path):
        jar = _make_jar(tmp_path / "app.jar", main_class="com.example.App")
        _make_jar(tmp_path / "airflow-sdk.jar", main_class=None, schema_version="2026-10-30")

        command, schema_version = self._build(JavaCoordinator(), tmp_path, jar)

        assert command[-1] == "com.example.App"
        assert schema_version == "2026-10-30"

    def test_runs_a_main_class_that_two_jars_set_as_a_task_does(self, tmp_path):
        # A native Dag's own command rejects this, but a task runs it, so the probe does too.
        new = _make_jar(tmp_path / "etl-new.jar", main_class="com.example.A", schema_version="2026-10-30")
        _make_jar(tmp_path / "etl-old.jar", main_class="com.example.A", schema_version="2026-10-30")
        coordinator = JavaCoordinator()
        with coordinator._set_scan_roots([tmp_path]):
            task = coordinator._build_execute_task_command(what=_make_ti())

        assert self._build(coordinator, tmp_path, new) == task

    @patch("airflow.sdk.coordinators._subprocess._set_close_on_exec_above_stderr", autospec=True)
    @patch("airflow.sdk.coordinators._subprocess._set_parent_death_signal", autospec=True)
    @patch("airflow.sdk.coordinators._subprocess.signal.signal", autospec=True)
    @patch(
        "airflow.sdk.coordinators._subprocess.os.execvpe", autospec=True, side_effect=OSError("exec failed")
    )
    def test_the_probe_execs_what_a_task_runs(
        self, mock_execvpe, mock_signal, mock_death_signal, mock_close_on_exec, tmp_path
    ):
        _make_jar(tmp_path / "app.jar", main_class="com.example.App")
        _make_jar(
            tmp_path / "airflow-sdk.jar", main_class=None, schema_version=TASK_HANDLER_PARSING_SCHEMA_VERSION
        )
        coordinator = JavaCoordinator()
        with coordinator._set_scan_roots([tmp_path]):
            command, _ = coordinator._build_execute_task_command(what=_make_ti())
        artifact = coordinator._find_task_handler_artifact(bundle_path=tmp_path, dag_id="etl")
        reported: list[str | None] = []

        with pytest.raises(OSError, match="exec failed"):
            coordinator.parse_task_handler(
                path=artifact.path,
                bundle_path=tmp_path,
                comm_address=("127.0.0.1", 1001),
                logs_address=("127.0.0.1", 1002),
                report_schema_version=reported.append,
            )

        assert reported == [TASK_HANDLER_PARSING_SCHEMA_VERSION]
        mock_execvpe.assert_called_once_with(
            command[0], [*command, "--comm=127.0.0.1:1001", "--logs=127.0.0.1:1002"], ANY
        )
