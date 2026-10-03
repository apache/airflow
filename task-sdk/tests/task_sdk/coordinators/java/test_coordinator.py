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
import os
import pathlib
import re
import struct
import zipfile
from unittest.mock import MagicMock, patch

import pytest
from task_sdk.coordinators._execute_test_utils import execute_task, register_dag_bundle
from uuid6 import uuid7

from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance
from airflow.sdk.coordinators._subprocess import _PopenActivitySubprocess
from airflow.sdk.coordinators.java.coordinator import (
    JavaCoordinator,
    _calculate_classpath,
    _find_schema_version,
    _parse_manifest,
)
from airflow.sdk.execution_time.comms import TaskHandlerArtifactRef
from airflow.sdk.execution_time.coordinator import (
    BaseCoordinator,
    TaskHandlerArtifactError,
    TaskHandlerCandidate,
)

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

    def test_nested_jars_are_sorted_by_path(self, tmp_path):
        (tmp_path / "lib").mkdir()
        for name in ("z.jar", "lib/b.jar", "lib/a.jar", "a.jar"):
            tmp_path.joinpath(name).write_bytes(b"")

        assert _calculate_classpath([tmp_path]).split(os.pathsep) == [
            tmp_path.joinpath(name).as_posix() for name in ("a.jar", "lib/a.jar", "lib/b.jar", "z.jar")
        ]

    def test_symlink_cycle_lists_each_jar_once(self, tmp_path):
        nested = tmp_path / "inner"
        nested.mkdir()
        jar = nested / "app.jar"
        jar.write_bytes(b"")
        try:
            (nested / "loop").symlink_to(tmp_path)
        except (OSError, NotImplementedError):
            pytest.skip("symlinks not supported on this platform")

        assert _calculate_classpath([tmp_path]) == jar.as_posix()


class TestFindSchemaVersion:
    def test_returns_the_first_version_in_sorted_walk_order(self, tmp_path):
        _make_jar(tmp_path / "b.jar", main_class=None, schema_version="2026-10-30")
        (tmp_path / "lib").mkdir()
        _make_jar(tmp_path / "lib" / "a.jar", main_class=None, schema_version="2026-06-16")
        _make_jar(tmp_path / "a.jar", main_class="com.example.Main")

        assert _find_schema_version([tmp_path]) == "2026-10-30"

    def test_skips_jars_that_cannot_be_read_or_carry_no_version(self, tmp_path):
        (tmp_path / "a.jar").write_text("not a zip")
        _make_jar(tmp_path / "b.jar", main_class="com.example.Main")
        _make_jar(tmp_path / "c.jar", main_class=None, schema_version="2026-06-16")

        assert _find_schema_version([tmp_path]) == "2026-06-16"

    def test_no_version_raises_naming_the_roots(self, tmp_path):
        _make_jar(tmp_path / "app.jar", main_class="com.example.Main")

        with pytest.raises(
            FileNotFoundError,
            match=rf"Airflow-Supervisor-Schema-Version metadata in {re.escape(str(tmp_path))}",
        ):
            _find_schema_version([tmp_path])


class TestJavaCoordinatorAttributes:
    def test_default_kwargs(self):
        coordinator = JavaCoordinator()
        assert coordinator.java_executable == "java"
        assert coordinator.jvm_args == []
        assert coordinator.task_handler_bundle_name is None
        # main_class stays optional: the Main-Class of the JAR's manifest decides without it.
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


class TestJavaCoordinatorBuildTaskHandlerCommand:
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
        coordinator = JavaCoordinator(jvm_args=["-Xmx1g"])
        with coordinator._set_scan_roots([bundle]):
            command, schema_version = coordinator._build_task_handler_command(path=bundle / jar)

        classpath = os.pathsep.join(
            (bundle / name).as_posix() for name in ("app.jar", "lib/dep.jar", "other.jar")
        )
        assert command == ["java", "-classpath", classpath, "-Xmx1g", main_class]
        assert schema_version == "2026-06-16"

    @pytest.mark.parametrize("jar", ["app.jar", "other.jar"])
    def test_main_class_overrides_the_jars_own_main_class(self, bundle, jar):
        coordinator = JavaCoordinator(main_class="com.example.Configured")
        with coordinator._set_scan_roots([bundle]):
            command, schema_version = coordinator._build_task_handler_command(path=bundle / jar)

        assert command[-1] == "com.example.Configured"
        assert schema_version == "2026-06-16"

    def test_main_class_lets_a_jar_without_a_main_class_run(self, tmp_path):
        _make_jar(tmp_path / "handlers.jar", main_class=None, schema_version="2026-06-16")
        coordinator = JavaCoordinator(main_class="com.example.Configured")
        with coordinator._set_scan_roots([tmp_path]):
            command, schema_version = coordinator._build_task_handler_command(path=tmp_path / "handlers.jar")

        assert command[-1] == "com.example.Configured"
        assert schema_version == "2026-06-16"

    def test_a_thin_jar_without_a_main_class_takes_its_version_from_the_sdk_jar(self, tmp_path):
        _make_jar(tmp_path / "handlers.jar", main_class=None)
        _make_jar(tmp_path / "airflow-sdk.jar", main_class=None, schema_version="2026-10-30")
        coordinator = JavaCoordinator(main_class="com.example.Configured")
        with coordinator._set_scan_roots([tmp_path]):
            command, schema_version = coordinator._build_task_handler_command(path=tmp_path / "handlers.jar")

        assert command[-1] == "com.example.Configured"
        assert schema_version == "2026-10-30"

    def test_a_thin_jar_takes_the_schema_version_from_the_sdk_jar(self, tmp_path):
        _make_jar(tmp_path / "app.jar", main_class="com.example.App")
        _make_jar(tmp_path / "airflow-sdk.jar", main_class=None, schema_version="2026-10-30")
        coordinator = JavaCoordinator()
        with coordinator._set_scan_roots([tmp_path]):
            command, schema_version = coordinator._build_task_handler_command(path=tmp_path / "app.jar")

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
            command, _ = coordinator._build_task_handler_command(path=tmp_path / "app.jar")

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
            coordinator._build_task_handler_command(path=path)


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

    def test_lists_a_jar_with_a_cache_digest_but_no_main_class_when_main_class_is_set(self, tmp_path):
        jar = _make_jar(tmp_path / "handlers.jar", main_class=None, cache_digest=_CACHE_DIGEST)

        [candidate] = JavaCoordinator(main_class="com.example.Main").list_task_handler_candidates(tmp_path)

        assert (candidate.rel_path, candidate.size_bytes, candidate.error) == (
            "handlers.jar",
            jar.stat().st_size,
            None,
        )

    def test_main_class_is_part_of_the_cache_digest(self, tmp_path):
        _make_jar(tmp_path / "handlers.jar", cache_digest=_CACHE_DIGEST)

        def digest(main_class: str) -> str | None:
            [candidate] = JavaCoordinator(main_class=main_class).list_task_handler_candidates(tmp_path)
            return candidate.cache_digest

        unset, first, second = digest(""), digest("com.example.First"), digest("com.example.Second")

        assert unset == _CACHE_DIGEST
        assert len({unset, first, second}) == 3
        assert digest("com.example.First") == first
        assert len(first or "") in range(1, 129)

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
    _make_jar(
        tmp_path.joinpath("app.jar"),
        main_class="com.example.TaskRunner",
        schema_version="2026-06-16",
        cache_digest=_CACHE_DIGEST,
    )
    return tmp_path


@pytest.fixture
def java_task_handlers(jars_dir):
    """Register *jars_dir* as the ``java-task-handlers`` Dag bundle and return that name."""
    with register_dag_bundle("java-task-handlers", jars_dir) as name:
        yield name


@pytest.fixture
def mock_client(make_ti_context):
    client = MagicMock()
    client.task_instances.start.return_value = make_ti_context()
    return client


def _execute_task(
    mock_client,
    bundle_name: str,
    *,
    dag_rel_path: str = "app.jar",
    task_handler_artifact: TaskHandlerArtifactRef | None = None,
    coordinator: JavaCoordinator | None = None,
):
    """Run a task of the Dag bundle *bundle_name* and return the commands the runtime was started with."""
    return execute_task(
        coordinator or JavaCoordinator(),
        mock_client,
        what=_make_ti(),
        dag_rel_path=dag_rel_path,
        bundle_info=BundleInfo(name=bundle_name),
        task_handler_artifact=task_handler_artifact,
    )


class TestJavaCoordinatorExecuteTask:
    def _captured_popen_cmd(
        self,
        bundle_name: str,
        mock_client,
        *,
        java_executable: str = "java",
        jvm_args: list[str] | None = None,
    ) -> list[str]:
        coordinator = JavaCoordinator(java_executable=java_executable, jvm_args=jvm_args or [])
        _, popen_calls = _execute_task(mock_client, bundle_name, coordinator=coordinator)
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
        result, _ = _execute_task(mock_client, java_task_handlers)

        assert isinstance(result, BaseCoordinator.ExecutionResult)
        assert result.exit_code == 0

    def test_a_task_without_a_reference_runs_its_dag_file(self, jars_dir, java_task_handlers, mock_client):
        _make_jar(jars_dir / "other.jar", main_class="com.example.Other", cache_digest=_CACHE_DIGEST)

        _, popen_calls = _execute_task(mock_client, java_task_handlers, dag_rel_path="other.jar")

        assert "com.example.Other" in popen_calls[0]
        assert "com.example.TaskRunner" not in popen_calls[0]

    def test_a_referenced_jar_runs_with_its_own_main_class_when_two_jars_declare_one(
        self, jars_dir, java_task_handlers, mock_client
    ):
        _make_jar(jars_dir / "z.jar", main_class="com.example.Referenced", cache_digest=_CACHE_DIGEST)
        reference = TaskHandlerArtifactRef(bundle_info=BundleInfo(name=java_task_handlers), rel_path="z.jar")

        _, popen_calls = _execute_task(
            mock_client, "other-dags", dag_rel_path="dag.py", task_handler_artifact=reference
        )

        assert "com.example.Referenced" in popen_calls[0]
        assert "com.example.TaskRunner" not in popen_calls[0]

    def test_main_class_overrides_the_main_class_of_the_manifest(self, java_task_handlers, mock_client):
        coordinator = JavaCoordinator(main_class="com.example.Configured")

        _, popen_calls = _execute_task(mock_client, java_task_handlers, coordinator=coordinator)

        assert "com.example.Configured" in popen_calls[0]
        assert "com.example.TaskRunner" not in popen_calls[0]

    def test_main_class_runs_a_jar_whose_manifest_has_none(self, jars_dir, java_task_handlers, mock_client):
        _make_jar(
            jars_dir / "handlers.jar",
            main_class=None,
            schema_version="2026-06-16",
            cache_digest=_CACHE_DIGEST,
        )
        coordinator = JavaCoordinator(main_class="com.example.Configured")

        _, popen_calls = _execute_task(
            mock_client, java_task_handlers, dag_rel_path="handlers.jar", coordinator=coordinator
        )

        assert "com.example.Configured" in popen_calls[0]

    @patch.object(_PopenActivitySubprocess, "start", autospec=True)
    def test_a_thin_jar_takes_the_schema_version_from_the_first_sdk_jar_in_sorted_order(
        self, mock_start, jars_dir, java_task_handlers, mock_client
    ):
        _make_jar(jars_dir / "thin.jar", main_class="com.example.Thin", cache_digest=_CACHE_DIGEST)
        (jars_dir / "lib").mkdir()
        _make_jar(jars_dir / "lib" / "sdk-b.jar", main_class=None, schema_version="2026-10-30")
        _make_jar(jars_dir / "lib" / "sdk-a.jar", main_class=None, schema_version="2026-06-16")
        (jars_dir / "app.jar").unlink()
        mock_start.return_value.wait.return_value = 0

        JavaCoordinator().execute_task(
            what=_make_ti(),
            dag_rel_path="thin.jar",
            bundle_info=BundleInfo(name=java_task_handlers),
            client=mock_client,
            subprocess_logs_to_stdout=False,
        )

        assert mock_start.call_args.kwargs["subprocess_schema_version"] == "2026-06-16"
        command = mock_start.call_args.kwargs["command"]
        classpath = command[command.index("-classpath") + 1].split(os.pathsep)
        assert classpath == [
            (jars_dir / "lib" / "sdk-a.jar").as_posix(),
            (jars_dir / "lib" / "sdk-b.jar").as_posix(),
            (jars_dir / "thin.jar").as_posix(),
        ]

    def test_a_python_dag_file_raises_the_unbound_message(self, jars_dir, java_task_handlers, mock_client):
        (jars_dir / "dags").mkdir()
        (jars_dir / "dags" / "etl.py").write_text("print('hello')\n")

        with pytest.raises(TaskHandlerArtifactError) as raised:
            _execute_task(mock_client, java_task_handlers, dag_rel_path="dags/etl.py")

        assert str(raised.value) == (
            "Task 'task_1' of Dag 'test_dag' has no task handler artifact, and its Dag file 'dags/etl.py' "
            "is not an artifact that JavaCoordinator runs. Queue 'java' routes it to a Lang-SDK "
            "coordinator, so it must be a @task.stub task the Dag processor bound to an artifact, or a "
            "task of a Dag defined in a Lang SDK. Check the import errors of 'dags/etl.py', and that the "
            "scheduler has the same [sdk] configuration as the Dag processor."
        )

    def test_a_dependency_jar_without_a_cache_digest_is_not_an_artifact(
        self, jars_dir, java_task_handlers, mock_client
    ):
        _make_jar(jars_dir / "dep.jar", main_class="org.dependency.Main", schema_version="2026-06-16")
        reference = TaskHandlerArtifactRef(rel_path="dep.jar")

        with pytest.raises(TaskHandlerArtifactError, match="is not an artifact that JavaCoordinator runs"):
            _execute_task(
                mock_client, java_task_handlers, dag_rel_path="dag.py", task_handler_artifact=reference
            )

    @pytest.mark.skipif(os.geteuid() == 0, reason="root opens every file")
    def test_a_jar_the_worker_cannot_open_raises_with_the_reason(
        self, jars_dir, java_task_handlers, mock_client
    ):
        (jars_dir / "app.jar").chmod(0)
        reference = TaskHandlerArtifactRef(rel_path="app.jar")

        with pytest.raises(TaskHandlerArtifactError) as raised:
            _execute_task(
                mock_client, java_task_handlers, dag_rel_path="dag.py", task_handler_artifact=reference
            )

        assert str(raised.value) == (
            f"Task handler artifact 'app.jar' in Dag bundle '{java_task_handlers}' cannot run: "
            f"[Errno 13] Permission denied: {os.fspath(jars_dir / 'app.jar')!r}"
        )

    def test_a_jar_with_no_main_class_anywhere_raises_with_the_reason(
        self, jars_dir, java_task_handlers, mock_client
    ):
        _make_jar(
            jars_dir / "handlers.jar",
            main_class=None,
            schema_version="2026-06-16",
            cache_digest=_CACHE_DIGEST,
        )
        reference = TaskHandlerArtifactRef(rel_path="handlers.jar")

        with pytest.raises(TaskHandlerArtifactError) as raised:
            _execute_task(
                mock_client, java_task_handlers, dag_rel_path="dag.py", task_handler_artifact=reference
            )

        assert str(raised.value) == (
            f"Task handler artifact 'handlers.jar' in Dag bundle '{java_task_handlers}' cannot run: "
            "handlers.jar has an Airflow-Cache-Digest manifest attribute but no Main-Class"
        )

    def test_a_bundle_without_a_schema_version_raises_before_the_runtime_starts(
        self, jars_dir, java_task_handlers, mock_client
    ):
        (jars_dir / "app.jar").unlink()
        _make_jar(jars_dir / "thin.jar", main_class="com.example.Thin", cache_digest=_CACHE_DIGEST)
        reference = TaskHandlerArtifactRef(rel_path="thin.jar")

        with patch.object(_PopenActivitySubprocess, "start", autospec=True) as mock_start:
            with pytest.raises(TaskHandlerArtifactError, match="Airflow-Supervisor-Schema-Version"):
                _execute_task(
                    mock_client, java_task_handlers, dag_rel_path="dag.py", task_handler_artifact=reference
                )

        mock_start.assert_not_called()
