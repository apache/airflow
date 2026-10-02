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

import io
import json
import os
import pathlib
import re
import socket
import struct
import subprocess
import zipfile
from unittest.mock import MagicMock, patch

import pytest
from uuid6 import uuid7

from airflow.sdk.api.datamodels._generated import TaskInstance
from airflow.sdk.coordinators.java.coordinator import (
    JavaCoordinator,
    _calculate_classpath,
    _JarInfo,
    _parse_manifest,
    _walk_jars,
)
from airflow.sdk.execution_time.coordinator import BaseCoordinator, TaskHandlerCandidate
from airflow.sdk.execution_time.supervisor import ActivitySubprocess

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
    cache_digest: str | None = None,
) -> pathlib.Path:
    """Write a minimal JAR with (optionally) a Main-Class manifest entry."""
    lines = ["Manifest-Version: 1.0"]
    if main_class:
        lines.append(f"Main-Class: {main_class}")
    if schema_version:
        lines.append(f"Airflow-Supervisor-Schema-Version: {schema_version}")
    if cache_digest:
        lines.append(f"Airflow-Cache-Digest: {cache_digest}")
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


def test_parse_manifest_joins_continued_values_and_stops_at_the_main_section():
    manifest = (
        b"Manifest-Version: 1.0\r\n"
        b"Airflow-Cache-Digest: 6a7cf9d4ddd454e97952b07038a8ebc6ff37c3a52ae176e921\r\n"
        b" bc03065d6404cd\r\n"
        b"Main-Class: com.example.Main\r\n"
        b"\r\n"
        b"Name: com/example/\r\n"
        b"Sealed: true\r\n"
    )

    assert _parse_manifest(manifest) == {
        "manifest-version": "1.0",
        "airflow-cache-digest": "6a7cf9d4ddd454e97952b07038a8ebc6ff37c3a52ae176e921bc03065d6404cd",
        "main-class": "com.example.Main",
    }


def test_parse_manifest_decodes_a_character_split_across_a_fold():
    manifest = b"Manifest-Version: 1.0\r\nImplementation-Vendor: Z\xc3\r\n \xbcrich\r\n\r\n"

    assert _parse_manifest(manifest)["implementation-vendor"] == "Z\u00fcrich"


class TestJavaCoordinatorParseTaskHandlerCommand:
    @pytest.fixture
    def bundle(self, tmp_path):
        _make_jar(tmp_path / "app.jar", main_class="com.example.App", schema_version="2026-06-16")
        _make_jar(tmp_path / "other.jar", main_class="com.example.Other", schema_version="2026-06-16")
        (tmp_path / "lib").mkdir()
        _make_jar(tmp_path / "lib" / "dep.jar", main_class=None)
        return tmp_path

    @pytest.mark.parametrize(
        ("jar", "main_class"), [("app.jar", "com.example.App"), ("other.jar", "com.example.Other")]
    )
    def test_runs_the_probed_jars_own_main_class_with_the_bundle_on_the_classpath(
        self, bundle, jar, main_class
    ):
        coordinator = JavaCoordinator(jvm_args=["-Xmx1g"], main_class="com.example.Configured")
        with coordinator._set_scan_roots([bundle]):
            command, schema_version = coordinator._build_parse_task_handler_command(path=bundle / jar)

        classpath = os.pathsep.join(
            (bundle / name).as_posix() for name in ("app.jar", "lib/dep.jar", "other.jar")
        )
        assert command == ["java", "-classpath", classpath, "-Xmx1g", main_class]
        assert schema_version == "2026-06-16"

    def test_a_thin_jar_takes_the_schema_version_from_the_sdk_jar(self, tmp_path):
        _make_jar(tmp_path / "app.jar", main_class="com.example.App")
        _make_jar(tmp_path / "airflow-sdk.jar", main_class=None, schema_version="2026-10-30")
        coordinator = JavaCoordinator()
        with coordinator._set_scan_roots([tmp_path]):
            command, schema_version = coordinator._build_parse_task_handler_command(path=tmp_path / "app.jar")

        assert command[-1] == "com.example.App"
        assert schema_version == "2026-10-30"

    def test_reads_a_main_class_continued_on_the_next_manifest_line(self, tmp_path):
        main_class = "org.example.a.very.long.package.name.that.needs.folding.BundleMain"
        manifest = f"Manifest-Version: 1.0\r\nMain-Class: {main_class[:50]}\r\n {main_class[50:]}\r\n"
        manifest += "Airflow-Supervisor-Schema-Version: 2026-06-16\r\n\r\n"
        with zipfile.ZipFile(tmp_path / "app.jar", "w") as zf:
            zf.writestr("META-INF/MANIFEST.MF", manifest)
        coordinator = JavaCoordinator()
        with coordinator._set_scan_roots([tmp_path]):
            command, _ = coordinator._build_parse_task_handler_command(path=tmp_path / "app.jar")

        assert command[-1] == main_class

    @pytest.mark.parametrize(
        ("content", "reason"),
        [
            ("no-main-class", "its manifest sets no Main-Class"),
            ("no-manifest", "it has no readable manifest"),
            ("not-a-zip", "it has no readable manifest"),
        ],
    )
    def test_a_jar_that_is_not_executable_is_rejected(self, tmp_path, content, reason):
        path = tmp_path / "app.jar"
        if content == "no-main-class":
            _make_jar(path, main_class=None, schema_version="2026-06-16")
        elif content == "no-manifest":
            with zipfile.ZipFile(path, "w") as zf:
                zf.writestr("com/example/App.class", b"")
        else:
            path.write_text("not a zip")
        coordinator = JavaCoordinator()
        with (
            coordinator._set_scan_roots([tmp_path]),
            pytest.raises(ValueError, match=f"is not an executable JAR: {reason}$"),
        ):
            coordinator._build_parse_task_handler_command(path=path)


_CACHE_DIGEST = "c" * 64


def _flag_encrypted(jar: bytearray) -> None:
    jar[jar.index(b"PK\x01\x02") + 8] |= 0x01


def _set_unknown_compression(jar: bytearray) -> None:
    struct.pack_into("<H", jar, jar.index(b"PK\x01\x02") + 10, 99)


def _corrupt_lzma_stream(jar: bytearray) -> None:
    header = jar.index(b"PK\x03\x04")
    name_length, extra_length = struct.unpack_from("<HH", jar, header + 26)
    # Past the 4-byte LZMA header and 5 bytes of properties.
    start = header + 30 + name_length + extra_length + 9
    jar[start : start + 6] = bytes(byte ^ 0xFF for byte in jar[start : start + 6])


class TestListTaskHandlerCandidates:
    def test_lists_only_jars_that_carry_a_cache_digest(self, tmp_path):
        handlers = _make_jar(tmp_path / "handlers.jar", cache_digest=_CACHE_DIGEST)
        (tmp_path / "lib").mkdir()
        _make_jar(tmp_path / "lib" / "commons-cli.jar", main_class="org.apache.commons.cli.Main")
        _make_jar(tmp_path / "lib" / "airflow-sdk.jar", main_class=None, schema_version="2026-06-16")
        with zipfile.ZipFile(tmp_path / "lib" / "no-manifest.jar", "w") as zf:
            zf.writestr("com/example/App.class", b"")
        (tmp_path / "lib" / "broken.jar").write_text("not a zip")
        _make_jar(tmp_path / "handlers.zip", cache_digest=_CACHE_DIGEST)

        assert JavaCoordinator().list_task_handler_candidates(tmp_path) == [
            TaskHandlerCandidate(
                rel_path="handlers.jar", size_bytes=handlers.stat().st_size, cache_digest=_CACHE_DIGEST
            )
        ]

    def test_reports_a_jar_with_a_cache_digest_but_no_main_class(self, tmp_path):
        jar = _make_jar(tmp_path / "handlers.jar", main_class=None, cache_digest=_CACHE_DIGEST)

        assert JavaCoordinator().list_task_handler_candidates(tmp_path) == [
            TaskHandlerCandidate(
                rel_path="handlers.jar",
                size_bytes=jar.stat().st_size,
                cache_digest=_CACHE_DIGEST,
                error="handlers.jar has an Airflow-Cache-Digest manifest attribute but no Main-Class",
            )
        ]

    @pytest.mark.parametrize(
        ("compression", "damage"),
        [
            pytest.param(zipfile.ZIP_STORED, _flag_encrypted, id="encrypted"),
            pytest.param(zipfile.ZIP_STORED, _set_unknown_compression, id="unsupported-compression"),
            pytest.param(zipfile.ZIP_LZMA, _corrupt_lzma_stream, id="corrupt-lzma"),
        ],
    )
    def test_skips_a_jar_whose_manifest_cannot_be_read(self, tmp_path, compression, damage):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", compression=compression) as zf:
            zf.writestr(
                "META-INF/MANIFEST.MF",
                f"Manifest-Version: 1.0\nMain-Class: com.example.Main\nAirflow-Cache-Digest: {_CACHE_DIGEST}\n\n",
            )
        unreadable = bytearray(buffer.getvalue())
        damage(unreadable)
        (tmp_path / "unreadable.jar").write_bytes(unreadable)
        handlers = _make_jar(tmp_path / "handlers.jar", cache_digest=_CACHE_DIGEST)

        assert JavaCoordinator().list_task_handler_candidates(tmp_path) == [
            TaskHandlerCandidate(
                rel_path="handlers.jar", size_bytes=handlers.stat().st_size, cache_digest=_CACHE_DIGEST
            )
        ]


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
