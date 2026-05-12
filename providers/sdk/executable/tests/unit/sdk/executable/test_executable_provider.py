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
import struct
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from airflow.providers.sdk.executable.bundle_scanner import (
    FOOTER_MAGIC,
    FOOTER_SIZE,
    BundleScanner,
    read_bundle_metadata,
    read_source_code,
)
from airflow.providers.sdk.executable.coordinator import ExecutableCoordinator
from airflow.providers.sdk.executable.get_provider_info import get_provider_info


def test_get_provider_info_exposes_executable_runtime_components():
    info = get_provider_info()
    assert info["package-name"] == "apache-airflow-providers-sdk-executable"
    assert info["name"] == "SDK: Executable"
    assert info["coordinators"] == [
        "airflow.providers.sdk.executable.coordinator.ExecutableCoordinator",
    ]
    assert info["integrations"] == [
        {
            "integration-name": "Native Executable",
            "external-doc-url": "https://airflow.apache.org/",
            "tags": ["software"],
        }
    ]
    assert "bundles_folder" in info["config"]["executable"]["options"]


def test_executable_provider_entrypoints_are_importable():
    assert ExecutableCoordinator.sdk == "executable"
    assert ExecutableCoordinator.file_extension == ""


_DEFAULT_BINARY_PAYLOAD = b"\x7fELF" + b"binary-stub-payload"


def _make_metadata(dag_ids, source_filename: str = "example.go") -> dict:
    return {
        "format_version": "1.0",
        "sdk": {"language": "go", "version": "0.1.0"},
        "source": source_filename,
        "dags": {dag_id: {"tasks": ["task1"]} for dag_id in dag_ids},
    }


def _build_bundle(
    path: Path,
    *,
    dag_ids=("tutorial_dag",),
    source: str | bytes = "package main\n\nfunc main() {}\n",
    source_filename: str = "example.go",
    metadata: dict | bytes | None = None,
    binary_bytes: bytes = _DEFAULT_BINARY_PAYLOAD,
    footer_ver: int = 1,
    magic: bytes = FOOTER_MAGIC,
    reserved: bytes = b"\x00" * 12,
) -> Path:
    """Write a self-contained bundle at *path* and return it.

    The binary region is a short opaque stub; tests only care that it has
    non-zero length so the trailer's bounds-check passes.
    """
    if isinstance(source, str):
        source_bytes = source.encode("utf-8")
    else:
        source_bytes = source

    if metadata is None:
        metadata_dict = _make_metadata(dag_ids, source_filename=source_filename)
        metadata_bytes = yaml.safe_dump(metadata_dict, sort_keys=True).encode("utf-8")
    elif isinstance(metadata, (bytes, bytearray)):
        metadata_bytes = bytes(metadata)
    else:
        metadata_bytes = yaml.safe_dump(metadata, sort_keys=True).encode("utf-8")

    if len(reserved) != 12:
        raise ValueError("reserved must be exactly 12 bytes")
    trailer = struct.pack("<III", len(source_bytes), len(metadata_bytes), footer_ver) + reserved + magic
    assert len(trailer) == FOOTER_SIZE

    path.write_bytes(binary_bytes + source_bytes + metadata_bytes + trailer)
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    return path


def _make_executable(path: Path) -> Path:
    """Create a non-bundle executable file (no AFBNDL01 trailer)."""
    path.write_bytes(b"#!/bin/sh\nexit 0\n")
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    return path


class TestReadBundleMetadata:
    def test_parses_embedded_manifest(self, tmp_path):
        binary = _build_bundle(tmp_path / "my_bundle", dag_ids=["tutorial_dag", "other_dag"])

        metadata = read_bundle_metadata(binary)
        assert metadata is not None
        assert metadata["sdk"] == {"language": "go", "version": "0.1.0"}
        assert set(metadata["dags"].keys()) == {"tutorial_dag", "other_dag"}

    def test_non_bundle_file_returns_none(self, tmp_path):
        regular = _make_executable(tmp_path / "not_a_bundle")
        assert read_bundle_metadata(regular) is None

    def test_short_file_returns_none(self, tmp_path):
        short = tmp_path / "tiny"
        short.write_bytes(b"hi")
        assert read_bundle_metadata(short) is None

    def test_unknown_footer_version_returns_none(self, tmp_path):
        binary = _build_bundle(tmp_path / "future_bundle", footer_ver=99)
        assert read_bundle_metadata(binary) is None

    def test_corrupted_yaml_returns_none(self, tmp_path):
        binary = _build_bundle(tmp_path / "broken", metadata=b"::: not: yaml: [")
        assert read_bundle_metadata(binary) is None


class TestReadSourceCode:
    def test_returns_embedded_source(self, tmp_path):
        binary = _build_bundle(
            tmp_path / "my_bundle",
            source='package main\n\nfunc main() { println("hi") }\n',
        )

        assert read_source_code(binary) == 'package main\n\nfunc main() { println("hi") }\n'

    def test_empty_source_region_returns_none(self, tmp_path):
        binary = _build_bundle(tmp_path / "no_source", source="")
        assert read_source_code(binary) is None

    def test_non_bundle_file_returns_none(self, tmp_path):
        regular = _make_executable(tmp_path / "not_a_bundle")
        assert read_source_code(regular) is None

    def test_invalid_utf8_source_returns_none(self, tmp_path):
        binary = _build_bundle(tmp_path / "binary_source", source=b"\xff\xfe\x00\x00not utf-8")
        assert read_source_code(binary) is None


class TestCanHandleDagFile:
    def test_valid_bundle_with_dag(self, tmp_path):
        binary = _build_bundle(tmp_path / "my_bundle", dag_ids=["tutorial_dag"])

        assert ExecutableCoordinator.can_handle_dag_file("test_bundle", str(binary)) is True

    def test_executable_without_footer(self, tmp_path):
        regular = _make_executable(tmp_path / "my_bundle")

        assert ExecutableCoordinator.can_handle_dag_file("test_bundle", str(regular)) is False

    def test_bundle_with_empty_dags(self, tmp_path):
        binary = _build_bundle(tmp_path / "empty_bundle", dag_ids=[])

        assert ExecutableCoordinator.can_handle_dag_file("test_bundle", str(binary)) is False

    def test_nonexistent_path(self):
        assert ExecutableCoordinator.can_handle_dag_file("test_bundle", "/nonexistent/path") is False

    def test_python_file_not_handled(self, tmp_path):
        py_file = tmp_path / "my_dag.py"
        py_file.write_text("# a python dag")

        assert ExecutableCoordinator.can_handle_dag_file("test_bundle", str(py_file)) is False


class TestDagParsingCmd:
    def test_builds_correct_command(self):
        cmd = ExecutableCoordinator.dag_parsing_cmd(
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


class TestTaskExecutionCmd:
    def test_pure_executable_dag(self, tmp_path):
        """When dag_file_path points directly to the bundle binary."""
        binary = _build_bundle(tmp_path / "my_bundle", dag_ids=["tutorial_dag"])

        what = MagicMock(spec=["dag_id"])
        what.dag_id = "tutorial_dag"

        bundle_info = MagicMock(spec=["name", "version"])

        cmd = ExecutableCoordinator.task_execution_cmd(
            what=what,
            dag_file_path=str(binary),
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

    def test_python_stub_with_exec_bit_set_falls_through_to_bundles_folder(self, tmp_path):
        """Python stub with X_OK set (e.g. bind-mounted in Breeze running as root)
        must still be recognised as a stub and resolve via bundles_folder.

        Regression test for the EACCES seen when running stub-mode DAGs from a
        directory where files inherit the bind-mount's executable bits.
        """
        bundles_dir = tmp_path / "bundles"
        bundles_dir.mkdir()
        binary = _build_bundle(bundles_dir / "my_bundle", dag_ids=["tutorial_dag"])

        what = MagicMock(spec=["dag_id"])
        what.dag_id = "tutorial_dag"

        bundle_info = MagicMock(spec=["name", "version"])

        py_file = tmp_path / "stub_dag.py"
        py_file.write_text("# stub dag")
        py_file.chmod(py_file.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

        with patch("airflow.providers.common.compat.sdk.conf") as mock_conf:
            mock_conf.get.return_value = str(bundles_dir)

            cmd = ExecutableCoordinator.task_execution_cmd(
                what=what,
                dag_file_path=str(py_file),
                bundle_path=str(tmp_path),
                bundle_info=bundle_info,
                comm_addr="127.0.0.1:12345",
                logs_addr="127.0.0.1:12346",
            )

        assert cmd[0] == str(binary.resolve())
        assert cmd[0] != str(py_file)

    def test_py_extension_short_circuits_even_if_trailer_matches(self, tmp_path):
        """A ``.py`` path is always treated as a stub, even if its bytes happen
        to satisfy the AFBNDL01 trailer check. Defensive against a
        pathologically-crafted Python file that could otherwise hit Case 1.
        """
        bundles_dir = tmp_path / "bundles"
        bundles_dir.mkdir()
        real_binary = _build_bundle(bundles_dir / "my_bundle", dag_ids=["tutorial_dag"])

        # Build a file with the AFBNDL01 trailer but a .py name. Even with the
        # trailer present, the .py extension forces the stub path.
        rogue_py = _build_bundle(tmp_path / "stub_dag.py", dag_ids=["tutorial_dag"])

        what = MagicMock(spec=["dag_id"])
        what.dag_id = "tutorial_dag"

        bundle_info = MagicMock(spec=["name", "version"])

        with patch("airflow.providers.common.compat.sdk.conf") as mock_conf:
            mock_conf.get.return_value = str(bundles_dir)

            cmd = ExecutableCoordinator.task_execution_cmd(
                what=what,
                dag_file_path=str(rogue_py),
                bundle_path=str(tmp_path),
                bundle_info=bundle_info,
                comm_addr="127.0.0.1:12345",
                logs_addr="127.0.0.1:12346",
            )

        assert cmd[0] == str(real_binary.resolve())
        assert cmd[0] != str(rogue_py)

    def test_python_stub_dag_with_bundles_folder(self, tmp_path):
        """When dag_file_path is a .py file, resolve from the configured bundles_folder."""
        bundles_dir = tmp_path / "bundles"
        bundles_dir.mkdir()
        binary = _build_bundle(bundles_dir / "my_bundle", dag_ids=["tutorial_dag"])

        what = MagicMock(spec=["dag_id"])
        what.dag_id = "tutorial_dag"

        bundle_info = MagicMock(spec=["name", "version"])

        py_file = tmp_path / "stub_dag.py"
        py_file.write_text("# stub dag")

        with patch("airflow.providers.common.compat.sdk.conf") as mock_conf:
            mock_conf.get.return_value = str(bundles_dir)

            cmd = ExecutableCoordinator.task_execution_cmd(
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
                ExecutableCoordinator.task_execution_cmd(
                    what=what,
                    dag_file_path=str(py_file),
                    bundle_path=str(tmp_path),
                    bundle_info=bundle_info,
                    comm_addr="127.0.0.1:12345",
                    logs_addr="127.0.0.1:12346",
                )


class TestBundleScanner:
    def test_resolve_finds_matching_dag_id(self, tmp_path):
        binary = _build_bundle(tmp_path / "my_bundle", dag_ids=["tutorial_dag", "other_dag"])

        scanner = BundleScanner(tmp_path)
        assert scanner.resolve("tutorial_dag") == str(binary.resolve())

    def test_resolve_picks_matching_bundle_among_many(self, tmp_path):
        _build_bundle(tmp_path / "alpha", dag_ids=["alpha_dag"])
        beta = _build_bundle(tmp_path / "beta", dag_ids=["beta_dag"])
        _build_bundle(tmp_path / "gamma", dag_ids=["gamma_dag"])

        scanner = BundleScanner(tmp_path)
        assert scanner.resolve("beta_dag") == str(beta.resolve())

    def test_resolve_skips_non_bundle_files(self, tmp_path):
        (tmp_path / "README.md").write_text("not a bundle")
        _make_executable(tmp_path / "stray_executable")
        binary = _build_bundle(tmp_path / "real_bundle", dag_ids=["tutorial_dag"])

        scanner = BundleScanner(tmp_path)
        assert scanner.resolve("tutorial_dag") == str(binary.resolve())

    def test_resolve_skips_non_executable_files(self, tmp_path):
        # A bundle file without the executable bit cannot be exec'd, so the
        # scanner must skip it even if its trailer would otherwise match.
        non_exec = _build_bundle(tmp_path / "non_exec", dag_ids=["tutorial_dag"])
        non_exec.chmod(non_exec.stat().st_mode & ~(stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH))

        scanner = BundleScanner(tmp_path)
        with pytest.raises(FileNotFoundError, match="No executable bundle"):
            scanner.resolve("tutorial_dag")

    def test_resolve_raises_when_not_found(self, tmp_path):
        scanner = BundleScanner(tmp_path)
        with pytest.raises(FileNotFoundError, match="No executable bundle"):
            scanner.resolve("nonexistent_dag")

    def test_resolve_raises_when_directory_missing(self, tmp_path):
        scanner = BundleScanner(tmp_path / "does_not_exist")
        with pytest.raises(FileNotFoundError, match="No executable bundle"):
            scanner.resolve("tutorial_dag")

    def test_resolve_executable_valid(self, tmp_path):
        binary = _build_bundle(tmp_path / "my_bundle", dag_ids=["tutorial_dag"])

        result = BundleScanner.resolve_executable(binary)
        assert result == str(binary.resolve())

    def test_resolve_executable_not_a_bundle(self, tmp_path):
        regular = _make_executable(tmp_path / "my_bundle")

        assert BundleScanner.resolve_executable(regular) is None

    def test_resolve_executable_empty_dags(self, tmp_path):
        binary = _build_bundle(tmp_path / "my_bundle", dag_ids=[])

        assert BundleScanner.resolve_executable(binary) is None

    def test_resolve_executable_non_executable_returns_none(self, tmp_path):
        binary = _build_bundle(tmp_path / "my_bundle", dag_ids=["tutorial_dag"])
        binary.chmod(binary.stat().st_mode & ~(stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH))

        assert BundleScanner.resolve_executable(binary) is None

    def test_resolve_executable_directory_returns_none(self, tmp_path):
        bundle_dir = tmp_path / "my_bundle"
        bundle_dir.mkdir()

        assert BundleScanner.resolve_executable(bundle_dir) is None


class TestGetCodeFromFile:
    def test_reads_embedded_source(self, tmp_path):
        source = "package main\n\nfunc main() {}\n"
        binary = _build_bundle(tmp_path / "my_bundle", source=source)

        result = ExecutableCoordinator.get_code_from_file(str(binary))
        assert result == source

    def test_no_source_raises(self, tmp_path):
        binary = _build_bundle(tmp_path / "my_bundle", source="")

        with pytest.raises(FileNotFoundError, match="No source code found"):
            ExecutableCoordinator.get_code_from_file(str(binary))

    def test_non_bundle_raises(self, tmp_path):
        regular = _make_executable(tmp_path / "not_a_bundle")

        with pytest.raises(FileNotFoundError, match="No source code found"):
            ExecutableCoordinator.get_code_from_file(str(regular))
