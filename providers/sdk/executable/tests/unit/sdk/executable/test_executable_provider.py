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

import stat
from unittest.mock import MagicMock, patch

import pytest
import yaml

from airflow.providers.sdk.executable.bundle_scanner import (
    BundleScanner,
    ResolvedExecutableBundle,
    read_source_code,
)
from airflow.providers.sdk.executable.coordinator import ExecutableRuntimeCoordinator
from airflow.providers.sdk.executable.get_provider_info import get_provider_info


def test_get_provider_info_exposes_executable_runtime_components():
    info = get_provider_info()
    assert info == {
        "package-name": "apache-airflow-providers-sdk-executable",
        "name": "SDK: Executable",
        "description": (
            "Native executable language support for Apache Airflow runtime coordinators.\n"
            "Supports any compiled binary (Go, Rust, etc.) that implements the Airflow\n"
            "SDK coordinator protocol (--comm/--logs socket-based IPC).\n"
        ),
        "integrations": [
            {
                "integration-name": "Native Executable",
                "external-doc-url": "https://airflow.apache.org/",
                "tags": ["software"],
            }
        ],
        "runtime-coordinators": [
            "airflow.providers.sdk.executable.coordinator.ExecutableRuntimeCoordinator",
        ],
    }


def test_executable_provider_entrypoints_are_importable():
    assert ExecutableRuntimeCoordinator.runtime_name == "executable"
    assert ExecutableRuntimeCoordinator.file_extension == ""


def _make_executable(path):
    """Create a file and make it executable."""
    path.touch()
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)


def _write_metadata(directory, dag_ids):
    """Write an airflow-metadata.yaml file with the given dag IDs."""
    metadata = {"dags": {dag_id: {"tasks": ["task1"]} for dag_id in dag_ids}}
    metadata_path = directory / "airflow-metadata.yaml"
    with open(metadata_path, "w") as f:
        yaml.safe_dump(metadata, f)


class TestCanHandleDagFile:
    def test_valid_executable_with_metadata(self, tmp_path):
        binary = tmp_path / "my_bundle"
        _make_executable(binary)
        _write_metadata(tmp_path, ["tutorial_dag"])

        assert ExecutableRuntimeCoordinator.can_handle_dag_file("test_bundle", str(binary)) is True

    def test_non_executable_file(self, tmp_path):
        regular_file = tmp_path / "my_bundle"
        regular_file.touch()
        _write_metadata(tmp_path, ["tutorial_dag"])

        assert ExecutableRuntimeCoordinator.can_handle_dag_file("test_bundle", str(regular_file)) is False

    def test_executable_without_metadata(self, tmp_path):
        binary = tmp_path / "my_bundle"
        _make_executable(binary)
        # No metadata file

        assert ExecutableRuntimeCoordinator.can_handle_dag_file("test_bundle", str(binary)) is False

    def test_nonexistent_path(self):
        assert ExecutableRuntimeCoordinator.can_handle_dag_file("test_bundle", "/nonexistent/path") is False

    def test_python_file_not_handled(self, tmp_path):
        py_file = tmp_path / "my_dag.py"
        py_file.write_text("# a python dag")

        assert ExecutableRuntimeCoordinator.can_handle_dag_file("test_bundle", str(py_file)) is False


class TestDagParsingRuntimeCmd:
    def test_builds_correct_command(self):
        cmd = ExecutableRuntimeCoordinator.dag_parsing_runtime_cmd(
            dag_file_path="/path/to/my_bundle",
            bundle_name="test_bundle",
            bundle_path="/path/to",
            comm_addr="127.0.0.1:12345",
            logs_addr="127.0.0.1:12346",
        )
        assert cmd == [
            "/path/to/my_bundle",
            "--comm=127.0.0.1:12345",
            "--logs=127.0.0.1:12346",
        ]


class TestTaskExecutionRuntimeCmd:
    def test_pure_executable_dag(self, tmp_path):
        """When dag_file_path points to an executable binary."""
        binary = tmp_path / "my_bundle"
        _make_executable(binary)

        what = MagicMock(spec=["dag_id"])
        what.dag_id = "tutorial_dag"

        bundle_info = MagicMock(spec=["name", "version"])

        cmd = ExecutableRuntimeCoordinator.task_execution_runtime_cmd(
            what=what,
            dag_file_path=str(binary),
            bundle_path=str(tmp_path),
            bundle_info=bundle_info,
            comm_addr="127.0.0.1:12345",
            logs_addr="127.0.0.1:12346",
        )
        assert cmd == [
            str(binary),
            "--comm=127.0.0.1:12345",
            "--logs=127.0.0.1:12346",
        ]

    def test_python_stub_dag_with_bundles_folder(self, tmp_path):
        """When dag_file_path is a .py file, resolve from bundles_folder."""
        # Set up bundles folder with an executable bundle
        bundles_dir = tmp_path / "bundles"
        bundle_home = bundles_dir / "my_bundle"
        bundle_home.mkdir(parents=True)

        binary = bundle_home / "my_bundle"
        _make_executable(binary)
        _write_metadata(bundle_home, ["tutorial_dag"])

        what = MagicMock(spec=["dag_id"])
        what.dag_id = "tutorial_dag"

        bundle_info = MagicMock(spec=["name", "version"])

        py_file = tmp_path / "stub_dag.py"
        py_file.write_text("# stub dag")

        with patch("airflow.providers.common.compat.sdk.conf") as mock_conf:
            mock_conf.get.return_value = str(bundles_dir)

            cmd = ExecutableRuntimeCoordinator.task_execution_runtime_cmd(
                what=what,
                dag_file_path=str(py_file),
                bundle_path=str(tmp_path),
                bundle_info=bundle_info,
                comm_addr="127.0.0.1:12345",
                logs_addr="127.0.0.1:12346",
            )

        assert cmd == [
            str(binary.resolve()),
            "--comm=127.0.0.1:12345",
            "--logs=127.0.0.1:12346",
        ]

    def test_python_stub_dag_without_bundles_folder_raises(self, tmp_path):
        """When dag_file_path is not executable and no bundles_folder configured."""
        py_file = tmp_path / "stub_dag.py"
        py_file.write_text("# stub dag")

        what = MagicMock(spec=["dag_id"])
        what.dag_id = "tutorial_dag"

        bundle_info = MagicMock(spec=["name", "version"])

        with patch("airflow.providers.common.compat.sdk.conf") as mock_conf:
            mock_conf.get.return_value = None

            with pytest.raises(ValueError, match="bundles_folder config must be set"):
                ExecutableRuntimeCoordinator.task_execution_runtime_cmd(
                    what=what,
                    dag_file_path=str(py_file),
                    bundle_path=str(tmp_path),
                    bundle_info=bundle_info,
                    comm_addr="127.0.0.1:12345",
                    logs_addr="127.0.0.1:12346",
                )


class TestBundleScanner:
    def test_resolve_finds_matching_dag_id(self, tmp_path):
        """Nested layout: bundle_home/my_bundle + metadata."""
        bundle_home = tmp_path / "my_bundle"
        bundle_home.mkdir()

        binary = bundle_home / "my_bundle"
        _make_executable(binary)
        _write_metadata(bundle_home, ["tutorial_dag", "other_dag"])

        scanner = BundleScanner(tmp_path)
        result = scanner.resolve("tutorial_dag")

        assert isinstance(result, ResolvedExecutableBundle)
        assert result.executable_path == str(binary.resolve())

    def test_resolve_flat_layout(self, tmp_path):
        """Flat layout: executables and metadata directly in bundles_dir."""
        binary = tmp_path / "my_bundle"
        _make_executable(binary)
        _write_metadata(tmp_path, ["tutorial_dag"])

        scanner = BundleScanner(tmp_path)
        result = scanner.resolve("tutorial_dag")

        assert result.executable_path == str(binary.resolve())

    def test_resolve_raises_when_not_found(self, tmp_path):
        scanner = BundleScanner(tmp_path)
        with pytest.raises(FileNotFoundError, match="No executable bundle"):
            scanner.resolve("nonexistent_dag")

    def test_resolve_executable_valid(self, tmp_path):
        binary = tmp_path / "my_bundle"
        _make_executable(binary)
        _write_metadata(tmp_path, ["tutorial_dag"])

        result = BundleScanner.resolve_executable(binary)
        assert result == str(binary.resolve())

    def test_resolve_executable_no_metadata(self, tmp_path):
        binary = tmp_path / "my_bundle"
        _make_executable(binary)

        result = BundleScanner.resolve_executable(binary)
        assert result is None

    def test_resolve_executable_not_executable(self, tmp_path):
        regular_file = tmp_path / "my_bundle"
        regular_file.touch()
        _write_metadata(tmp_path, ["tutorial_dag"])

        result = BundleScanner.resolve_executable(regular_file)
        assert result is None

    def test_resolve_executable_directory_with_binary(self, tmp_path):
        """When path is a directory containing executable + metadata."""
        bundle_dir = tmp_path / "my_bundle"
        bundle_dir.mkdir()

        binary = bundle_dir / "my_bundle"
        _make_executable(binary)
        _write_metadata(bundle_dir, ["tutorial_dag"])

        result = BundleScanner.resolve_executable(bundle_dir)
        assert result == str(binary.resolve())

    def test_resolve_executable_empty_metadata(self, tmp_path):
        binary = tmp_path / "my_bundle"
        _make_executable(binary)

        # Write metadata with empty dags section
        metadata_path = tmp_path / "airflow-metadata.yaml"
        with open(metadata_path, "w") as f:
            yaml.safe_dump({"dags": {}}, f)

        result = BundleScanner.resolve_executable(binary)
        assert result is None


class TestGetCodeFromFile:
    def test_reads_go_source(self, tmp_path):
        binary = tmp_path / "my_bundle"
        _make_executable(binary)

        source = tmp_path / "main.go"
        source.write_text("package main\n\nfunc main() {}\n")

        result = ExecutableRuntimeCoordinator.get_code_from_file(str(binary))
        assert result == "package main\n\nfunc main() {}\n"

    def test_reads_named_source(self, tmp_path):
        binary = tmp_path / "my_bundle"
        _make_executable(binary)

        source = tmp_path / "my_bundle.go"
        source.write_text("package main\n")

        result = ExecutableRuntimeCoordinator.get_code_from_file(str(binary))
        assert result == "package main\n"

    def test_no_source_raises(self, tmp_path):
        binary = tmp_path / "my_bundle"
        _make_executable(binary)

        with pytest.raises(FileNotFoundError, match="No source code found"):
            ExecutableRuntimeCoordinator.get_code_from_file(str(binary))


class TestReadSourceCode:
    def test_prefers_named_go_over_main_go(self, tmp_path):
        binary = tmp_path / "my_bundle"
        (tmp_path / "my_bundle.go").write_text("named source")
        (tmp_path / "main.go").write_text("main source")

        result = read_source_code(binary)
        assert result == "named source"

    def test_rust_source(self, tmp_path):
        binary = tmp_path / "my_bundle"
        (tmp_path / "main.rs").write_text("fn main() {}")

        result = read_source_code(binary)
        assert result == "fn main() {}"

    def test_no_source_returns_none(self, tmp_path):
        binary = tmp_path / "my_bundle"
        result = read_source_code(binary)
        assert result is None
