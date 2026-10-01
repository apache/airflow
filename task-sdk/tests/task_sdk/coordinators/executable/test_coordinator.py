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

import hashlib
import json
import socket
import stat
import struct
import subprocess
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
import yaml
from uuid6 import uuid7

from airflow.sdk.api.datamodels._generated import TaskInstance
from airflow.sdk.coordinators.executable.coordinator import (
    FOOTER_MAGIC,
    FOOTER_SIZE,
    ExecutableCoordinator,
    _BinaryDigestCache,
    _Bundle,
    _digest_cache,
    read_cache_digest,
)
from airflow.sdk.execution_time.coordinator import BaseCoordinator, TaskHandlerCandidate
from airflow.sdk.execution_time.supervisor import ActivitySubprocess

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    pytest.skip("Coordinator is only compatible with Airflow >= 3.3.0", allow_module_level=True)

_DEFAULT_BINARY_PAYLOAD = b"\x7fELF" + b"binary-stub-payload"


def _make_metadata(dag_ids, source_filename: str = "example.go") -> dict:
    return {
        "airflow_bundle_metadata_version": "1.0",
        "sdk": {
            "language": "go",
            "version": "0.1.0",
            "supervisor_schema_version": "2026-06-16",
        },
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
    binary_sha256: bytes | None = None,
) -> Path:
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
    digest = binary_sha256 if binary_sha256 is not None else hashlib.sha256(binary_bytes).digest()
    if len(digest) != 32:
        raise ValueError("binary_sha256 must be exactly 32 bytes")
    trailer = (
        struct.pack("<III", len(source_bytes), len(metadata_bytes), footer_ver) + digest + reserved + magic
    )
    assert len(trailer) == FOOTER_SIZE

    path.write_bytes(binary_bytes + source_bytes + metadata_bytes + trailer)
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    return path


def _make_executable(path: Path) -> Path:
    path.write_bytes(b"#!/bin/sh\nexit 0\n")
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    return path


def _make_ti(dag_id: str = "tutorial_dag", queue: str = "executable") -> TaskInstance:
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


class TestBinaryDigestCache:
    def test_get_returns_none_for_missing_key(self):
        cache = _BinaryDigestCache(maxsize=4)
        assert cache.get(("/p", 0, 0, 0, 0)) is None

    def test_put_then_get_returns_stored_digest(self):
        cache = _BinaryDigestCache(maxsize=4)
        key = ("/p", 100, 1, 2, 3)
        cache.put(key, b"\xaa" * 32)
        assert cache.get(key) == b"\xaa" * 32

    def test_put_updates_existing_key(self):
        cache = _BinaryDigestCache(maxsize=4)
        key = ("/p", 100, 1, 2, 3)
        cache.put(key, b"\xaa" * 32)
        cache.put(key, b"\xbb" * 32)
        assert cache.get(key) == b"\xbb" * 32

    def test_eviction_drops_oldest_when_over_maxsize(self):
        cache = _BinaryDigestCache(maxsize=2)
        keys = [(f"/p{i}", i, 0, 0, 0) for i in range(3)]
        for i, k in enumerate(keys):
            cache.put(k, bytes([i]) * 32)

        # First inserted entry should have been evicted.
        assert cache.get(keys[0]) is None
        assert cache.get(keys[1]) == bytes([1]) * 32
        assert cache.get(keys[2]) == bytes([2]) * 32

    def test_get_promotes_entry_so_it_is_not_evicted_next(self):
        cache = _BinaryDigestCache(maxsize=2)
        key_a = ("/a", 0, 0, 0, 0)
        key_b = ("/b", 0, 0, 0, 0)
        key_c = ("/c", 0, 0, 0, 0)
        cache.put(key_a, b"\x01" * 32)
        cache.put(key_b, b"\x02" * 32)

        # Touch A so B becomes the LRU victim.
        assert cache.get(key_a) == b"\x01" * 32
        cache.put(key_c, b"\x03" * 32)

        assert cache.get(key_a) == b"\x01" * 32
        assert cache.get(key_b) is None
        assert cache.get(key_c) == b"\x03" * 32

    def test_put_promotes_existing_entry(self):
        cache = _BinaryDigestCache(maxsize=2)
        key_a = ("/a", 0, 0, 0, 0)
        key_b = ("/b", 0, 0, 0, 0)
        key_c = ("/c", 0, 0, 0, 0)
        cache.put(key_a, b"\x01" * 32)
        cache.put(key_b, b"\x02" * 32)

        # Re-putting A should refresh it so B is the next victim.
        cache.put(key_a, b"\x01" * 32)
        cache.put(key_c, b"\x03" * 32)

        assert cache.get(key_a) == b"\x01" * 32
        assert cache.get(key_b) is None
        assert cache.get(key_c) == b"\x03" * 32

    def test_clear_drops_all_entries(self):
        cache = _BinaryDigestCache(maxsize=4)
        key = ("/p", 0, 0, 0, 0)
        cache.put(key, b"\xaa" * 32)
        cache.clear()
        assert cache.get(key) is None


class TestBundleFind:
    def test_finds_matching_dag_id(self, tmp_path):
        binary = _build_bundle(tmp_path / "my_bundle", dag_ids=["tutorial_dag", "other_dag"])

        bundle = _Bundle.find([tmp_path], "tutorial_dag")
        assert bundle.path == binary.resolve()

    def test_picks_matching_bundle_among_many(self, tmp_path):
        _build_bundle(tmp_path / "alpha", dag_ids=["alpha_dag"])
        beta = _build_bundle(tmp_path / "beta", dag_ids=["beta_dag"])
        _build_bundle(tmp_path / "gamma", dag_ids=["gamma_dag"])

        bundle = _Bundle.find([tmp_path], "beta_dag")
        assert bundle.path == beta.resolve()

    def test_searches_nested_subdirectories(self, tmp_path):
        nested = tmp_path / "team-a" / "release-2026.05"
        nested.mkdir(parents=True)
        target = _build_bundle(nested / "pipeline", dag_ids=["nested_dag"])

        bundle = _Bundle.find([tmp_path], "nested_dag")
        assert bundle.path == target.resolve()

    def test_searches_multiple_roots(self, tmp_path):
        root_a = tmp_path / "a"
        root_b = tmp_path / "b"
        root_a.mkdir()
        root_b.mkdir()
        _build_bundle(root_a / "alpha", dag_ids=["alpha_dag"])
        target = _build_bundle(root_b / "beta", dag_ids=["beta_dag"])

        bundle = _Bundle.find([root_a, root_b], "beta_dag")
        assert bundle.path == target.resolve()

    def test_skips_non_bundle_files(self, tmp_path):
        (tmp_path / "README.md").write_text("not a bundle")
        _make_executable(tmp_path / "stray_executable")
        binary = _build_bundle(tmp_path / "real_bundle", dag_ids=["tutorial_dag"])

        bundle = _Bundle.find([tmp_path], "tutorial_dag")
        assert bundle.path == binary.resolve()

    def test_skips_non_executable_files(self, tmp_path):
        non_exec = _build_bundle(tmp_path / "non_exec", dag_ids=["tutorial_dag"])
        non_exec.chmod(non_exec.stat().st_mode & ~(stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH))

        with pytest.raises(FileNotFoundError, match="cannot find executable bundle"):
            _Bundle.find([tmp_path], "tutorial_dag")

    def test_raises_when_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="cannot find executable bundle"):
            _Bundle.find([tmp_path], "nonexistent_dag")

    def test_raises_when_directory_missing(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="cannot find executable bundle"):
            _Bundle.find([tmp_path / "does_not_exist"], "tutorial_dag")

    def test_symlink_cycle_does_not_infinite_recurse(self, tmp_path):
        nested = tmp_path / "inner"
        nested.mkdir()
        target = _build_bundle(nested / "pipeline", dag_ids=["loop_dag"])
        loop = nested / "loop"
        try:
            loop.symlink_to(tmp_path)
        except (OSError, NotImplementedError):
            pytest.skip("symlinks not supported on this platform")

        bundle = _Bundle.find([tmp_path], "loop_dag")
        assert bundle.path == target.resolve()

    def test_skips_bundle_with_corrupted_binary_region(self, tmp_path):
        bundle_path = _build_bundle(tmp_path / "tampered", dag_ids=["tutorial_dag"])
        # Flip a byte in the binary region; the embedded SHA-256 no longer matches.
        data = bytearray(bundle_path.read_bytes())
        data[0] ^= 0xFF
        bundle_path.write_bytes(bytes(data))
        bundle_path.chmod(bundle_path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        _digest_cache.clear()

        with patch("airflow.sdk.coordinators.executable.coordinator.log") as mock_log:
            with pytest.raises(FileNotFoundError, match="cannot find executable bundle"):
                _Bundle.find([tmp_path], "tutorial_dag")

        mock_log.debug.assert_any_call(
            "Bundle binary_sha256 mismatch; skipping",
            path=str(bundle_path),
            expected=mock.ANY,
            actual=mock.ANY,
        )

    def test_captures_schema_version_from_metadata(self, tmp_path):
        _build_bundle(tmp_path / "with_schema", dag_ids=["tutorial_dag"])

        bundle = _Bundle.find([tmp_path], "tutorial_dag")
        assert bundle.schema_version == "2026-06-16"

    def test_skips_bundle_when_schema_version_missing(self, tmp_path):
        metadata = _make_metadata(["tutorial_dag"])
        del metadata["sdk"]["supervisor_schema_version"]
        bundle_path = _build_bundle(tmp_path / "no_schema", dag_ids=["tutorial_dag"], metadata=metadata)

        # Converter rejects the missing schema_version, so find() treats the bundle as unusable.
        with patch("airflow.sdk.coordinators.executable.coordinator.log") as mock_log:
            with pytest.raises(FileNotFoundError, match="matching bundles were rejected"):
                _Bundle.find([tmp_path], "tutorial_dag")

        mock_log.debug.assert_any_call(
            "Bundle metadata rejected; skipping",
            path=str(bundle_path),
            error=mock.ANY,
        )

    def test_skips_bundle_with_unknown_schema_version(self, tmp_path):
        metadata = _make_metadata(["tutorial_dag"])
        metadata["sdk"]["supervisor_schema_version"] = "1999-01-01"
        bundle_path = _build_bundle(tmp_path / "bogus_schema", dag_ids=["tutorial_dag"], metadata=metadata)

        with patch("airflow.sdk.coordinators.executable.coordinator.log") as mock_log:
            with pytest.raises(FileNotFoundError, match="matching bundles were rejected") as exc_info:
                _Bundle.find([tmp_path], "tutorial_dag")

        mock_log.debug.assert_any_call(
            "Bundle metadata rejected; skipping",
            path=str(bundle_path),
            error=mock.ANY,
        )
        # The raised error surfaces the rejection so a found-but-unusable bundle
        # is not reported as missing.
        msg = str(exc_info.value)
        assert str(bundle_path.resolve()) in msg
        assert "1999-01-01" in msg

    def test_logs_when_metadata_yaml_is_malformed(self, tmp_path):
        bundle_path = _build_bundle(
            tmp_path / "bad_yaml",
            dag_ids=["tutorial_dag"],
            metadata=b"key: : not: valid: yaml: [",
        )

        with patch("airflow.sdk.coordinators.executable.coordinator.log") as mock_log:
            with pytest.raises(FileNotFoundError, match="cannot find executable bundle"):
                _Bundle.find([tmp_path], "tutorial_dag")

        mock_log.debug.assert_any_call(
            "Cannot decode bundle metadata; skipping",
            path=str(bundle_path),
            error=mock.ANY,
        )

    def test_logs_when_metadata_is_not_a_mapping(self, tmp_path):
        bundle_path = _build_bundle(
            tmp_path / "scalar_meta",
            dag_ids=["tutorial_dag"],
            metadata=b"just-a-scalar\n",
        )

        with patch("airflow.sdk.coordinators.executable.coordinator.log") as mock_log:
            with pytest.raises(FileNotFoundError, match="cannot find executable bundle"):
                _Bundle.find([tmp_path], "tutorial_dag")

        mock_log.debug.assert_any_call(
            "Cannot decode bundle metadata; skipping",
            path=str(bundle_path),
            error=mock.ANY,
        )


class TestExecutableCoordinatorAttributes:
    def test_build_command_scans_passed_roots_in_colocated_mode(self, tmp_path):
        binary = _build_bundle(tmp_path / "my_bundle", dag_ids=["tutorial_dag"])
        coordinator = ExecutableCoordinator()
        with coordinator._set_scan_roots([tmp_path]):
            command, schema_version = coordinator._build_execute_task_command(
                what=_make_ti(dag_id="tutorial_dag")
            )
        assert command == [str(binary.resolve())]
        assert schema_version == "2026-06-16"


class TestBuildExecuteTaskCommand:
    def test_returns_resolved_executable_and_schema_version(self, tmp_path):
        binary = _build_bundle(tmp_path / "my_bundle", dag_ids=["tutorial_dag"])
        ti = _make_ti(dag_id="tutorial_dag")

        coordinator = ExecutableCoordinator()
        with coordinator._set_scan_roots([tmp_path]):
            command, schema_version = coordinator._build_execute_task_command(what=ti)
        assert command == [str(binary.resolve())]
        assert schema_version == "2026-06-16"

    def test_raises_when_bundle_omits_schema_version(self, tmp_path):
        metadata = _make_metadata(["tutorial_dag"])
        del metadata["sdk"]["supervisor_schema_version"]
        _build_bundle(tmp_path / "my_bundle", dag_ids=["tutorial_dag"], metadata=metadata)
        ti = _make_ti(dag_id="tutorial_dag")

        coordinator = ExecutableCoordinator()
        with (
            coordinator._set_scan_roots([tmp_path]),
            pytest.raises(FileNotFoundError, match="matching bundles were rejected"),
        ):
            coordinator._build_execute_task_command(what=ti)

    def test_raises_when_dag_id_not_found(self, tmp_path):
        _build_bundle(tmp_path / "my_bundle", dag_ids=["other_dag"])
        ti = _make_ti(dag_id="tutorial_dag")

        coordinator = ExecutableCoordinator()
        with (
            coordinator._set_scan_roots([tmp_path]),
            pytest.raises(FileNotFoundError, match="cannot find executable bundle"),
        ):
            coordinator._build_execute_task_command(what=ti)


_CACHE_DIGEST = "c" * 64


def _make_metadata_with_digests(**digests: str) -> dict:
    return {**_make_metadata(["etl"]), "digests": digests}


class TestReadCacheDigest:
    @pytest.mark.parametrize(
        "bundle_kwargs",
        [
            pytest.param(
                {"metadata": _make_metadata_with_digests(integrity="a" * 64, cache=_CACHE_DIGEST)},
                id="valid-bundle",
            ),
            pytest.param(
                {"metadata": _make_metadata_with_digests(cache=_CACHE_DIGEST), "binary_sha256": b"\x00" * 32},
                id="binary-digest-mismatch-not-checked",
            ),
        ],
    )
    def test_returns_the_stored_digest(self, tmp_path, bundle_kwargs):
        bundle = _build_bundle(tmp_path / "etl", **bundle_kwargs)

        assert read_cache_digest(bundle) == _CACHE_DIGEST

    @pytest.mark.parametrize(
        "build",
        [
            pytest.param(lambda path: _build_bundle(path), id="no-digests"),
            pytest.param(
                lambda path: _build_bundle(path, metadata=_make_metadata_with_digests(integrity="a" * 64)),
                id="no-cache-digest",
            ),
            pytest.param(
                lambda path: _build_bundle(path, metadata=_make_metadata_with_digests(cache="")),
                id="empty-cache-digest",
            ),
            pytest.param(lambda path: _build_bundle(path, metadata=b"\xff\xfe"), id="undecodable-metadata"),
            pytest.param(_make_executable, id="not-a-bundle"),
            pytest.param(lambda path: path, id="missing-file"),
        ],
    )
    def test_returns_none_without_a_readable_digest(self, tmp_path, build):
        assert read_cache_digest(build(tmp_path / "etl")) is None


class TestBuildParseTaskHandlerCommand:
    def test_returns_the_bundle_and_its_schema_version(self, tmp_path):
        # The Dag ids in the metadata play no part: the Dag processor names the artifact.
        bundle = _build_bundle(tmp_path / "etl", dag_ids=["other_dag"])

        command, schema_version = ExecutableCoordinator()._build_parse_task_handler_command(path=bundle)

        assert command == [str(bundle.resolve())]
        assert schema_version == "2026-06-16"

    def test_returns_an_absolute_path(self, tmp_path, monkeypatch):
        _build_bundle(tmp_path / "etl")
        monkeypatch.chdir(tmp_path)

        command, _ = ExecutableCoordinator()._build_parse_task_handler_command(path=Path("etl"))

        assert command == [str((tmp_path / "etl").resolve())]

    @pytest.mark.parametrize(
        "build",
        [
            pytest.param(_make_executable, id="no-trailer"),
            pytest.param(
                lambda path: _build_bundle(path, binary_sha256=b"\x00" * 32), id="binary-digest-mismatch"
            ),
        ],
    )
    def test_rejects_a_file_that_is_not_a_valid_bundle(self, tmp_path, build):
        bundle = build(tmp_path / "etl")

        with pytest.raises(ValueError, match="is not a valid executable bundle"):
            ExecutableCoordinator()._build_parse_task_handler_command(path=bundle)

    def test_rejects_a_bundle_without_a_schema_version(self, tmp_path):
        metadata = _make_metadata(["etl"])
        del metadata["sdk"]["supervisor_schema_version"]
        bundle = _build_bundle(tmp_path / "etl", metadata=metadata)

        with pytest.raises(ValueError, match="supervisor_schema_version"):
            ExecutableCoordinator()._build_parse_task_handler_command(path=bundle)


def _list_candidates(bundle_path: Path) -> list[TaskHandlerCandidate]:
    return ExecutableCoordinator().list_task_handler_candidates(bundle_path)


class TestListTaskHandlerCandidates:
    def test_lists_trailer_files_whatever_their_executable_bit(self, tmp_path):
        metadata = _make_metadata_with_digests(cache=_CACHE_DIGEST)
        for team in ("team-a", "team-b"):
            (tmp_path / team).mkdir()
        runnable = _build_bundle(tmp_path / "team-a" / "pipeline", metadata=metadata)
        downloaded = _build_bundle(tmp_path / "team-b" / "pipeline", metadata=metadata)
        downloaded.chmod(0o644)
        _make_executable(tmp_path / "run.sh")
        (tmp_path / "README.md").write_text("not a bundle")

        assert _list_candidates(tmp_path) == [
            TaskHandlerCandidate(
                rel_path="team-a/pipeline", size_bytes=runnable.stat().st_size, cache_digest=_CACHE_DIGEST
            ),
            TaskHandlerCandidate(
                rel_path="team-b/pipeline",
                size_bytes=downloaded.stat().st_size,
                cache_digest=_CACHE_DIGEST,
                error=(
                    "team-b/pipeline is not executable. Use a Dag bundle that keeps the executable bit; "
                    "object-store Dag bundles such as S3DagBundle drop it."
                ),
            ),
        ]

    @pytest.mark.parametrize(
        ("bundle_kwargs", "cache_digest"),
        [
            pytest.param(
                {"metadata": _make_metadata_with_digests(cache=_CACHE_DIGEST)}, _CACHE_DIGEST, id="stored"
            ),
            pytest.param(
                {"metadata": _make_metadata_with_digests(cache=_CACHE_DIGEST), "binary_sha256": b"\x00" * 32},
                _CACHE_DIGEST,
                id="binary-digest-mismatch-not-checked",
            ),
            pytest.param({}, None, id="no-digests"),
            pytest.param({"metadata": b"\xff\xfe"}, None, id="undecodable-metadata"),
        ],
    )
    def test_reads_the_stored_cache_digest(self, tmp_path, bundle_kwargs, cache_digest):
        bundle = _build_bundle(tmp_path / "etl", **bundle_kwargs)

        assert _list_candidates(tmp_path) == [
            TaskHandlerCandidate(rel_path="etl", size_bytes=bundle.stat().st_size, cache_digest=cache_digest)
        ]

    def test_lists_a_bundle_it_cannot_run_with_its_trailer_error(self, tmp_path):
        _build_bundle(tmp_path / "etl", footer_ver=2)

        [candidate] = _list_candidates(tmp_path)

        assert candidate.cache_digest is None
        assert candidate.error is not None
        assert "Unsupported bundle footer_ver=2" in candidate.error


@pytest.fixture
def bundles_dir(tmp_path):
    _build_bundle(tmp_path / "my_bundle", dag_ids=["tutorial_dag"])
    return tmp_path


@pytest.fixture
def go_task_handlers(bundles_dir):
    """Register *bundles_dir* as the ``go-task-handlers`` Dag bundle and return that name."""
    bundle = {
        "name": "go-task-handlers",
        "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
        "kwargs": {"path": str(bundles_dir)},
    }
    with conf_vars({("dag_processor", "dag_bundle_config_list"): json.dumps([bundle])}):
        yield bundle["name"]


@pytest.fixture
def mock_client(make_ti_context):
    client = MagicMock()
    client.task_instances.start.return_value = make_ti_context()
    return client


class TestExecutableCoordinatorExecuteTask:
    def _captured_popen_cmd(self, bundle_name: str, mock_client) -> list[str]:
        """Run execute_task with mocked subprocess and return the command list."""
        ti = _make_ti(dag_id="tutorial_dag")
        coordinator = ExecutableCoordinator(task_handler_bundle_name=bundle_name)

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
                dag_rel_path="my_bundle",
                bundle_info=MagicMock(),
                client=mock_client,
                subprocess_logs_to_stdout=False,
            )

        assert popen_calls, "subprocess.Popen was not called"
        return popen_calls[0]

    def test_executable_path_is_first_arg(self, bundles_dir, go_task_handlers, mock_client):
        cmd = self._captured_popen_cmd(go_task_handlers, mock_client)
        expected = str((bundles_dir / "my_bundle").resolve())
        assert cmd[0] == expected

    def test_returns_execution_result(self, go_task_handlers, mock_client):
        ti = _make_ti(dag_id="tutorial_dag")
        coordinator = ExecutableCoordinator(task_handler_bundle_name=go_task_handlers)

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
                dag_rel_path="my_bundle",
                bundle_info=MagicMock(),
                client=mock_client,
                subprocess_logs_to_stdout=False,
            )

        assert isinstance(result, BaseCoordinator.ExecutionResult)
        assert result.exit_code == 0
