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

import contextlib
import hashlib
import os
import stat
import struct
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from task_sdk.coordinators._execute_test_utils import execute_task, register_dag_bundle
from uuid6 import uuid7

from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance
from airflow.sdk.coordinators._subprocess import _PopenActivitySubprocess
from airflow.sdk.coordinators.executable.coordinator import (
    FOOTER_MAGIC,
    FOOTER_SIZE,
    ExecutableCoordinator,
    _BinaryDigestCache,
    read_cache_digest,
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


class TestBuildTaskHandlerCommand:
    def test_returns_the_bundle_and_its_schema_version(self, tmp_path):
        # The Dag ids in the metadata play no part: the Dag processor names the artifact.
        bundle = _build_bundle(tmp_path / "etl", dag_ids=["other_dag"])

        command, schema_version = ExecutableCoordinator()._build_task_handler_command(path=bundle)

        assert command == [str(bundle.resolve())]
        assert schema_version == "2026-06-16"

    def test_returns_an_absolute_path(self, tmp_path, monkeypatch):
        _build_bundle(tmp_path / "etl")
        monkeypatch.chdir(tmp_path)

        command, _ = ExecutableCoordinator()._build_task_handler_command(path=Path("etl"))

        assert command == [str((tmp_path / "etl").resolve())]

    @pytest.mark.parametrize(
        "build",
        [
            pytest.param(_make_executable, id="no-trailer"),
            pytest.param(
                lambda path: _build_bundle(path, binary_sha256=b"\x00" * 32), id="binary-digest-mismatch"
            ),
            pytest.param(
                lambda path: _build_bundle(path, metadata=b"key: : not: valid: yaml: ["), id="malformed-yaml"
            ),
            pytest.param(lambda path: _build_bundle(path, metadata=b"just-a-scalar\n"), id="scalar-metadata"),
        ],
    )
    def test_rejects_a_file_that_is_not_a_valid_bundle(self, tmp_path, build):
        bundle = build(tmp_path / "etl")

        with pytest.raises(ValueError, match="is not a valid executable bundle"):
            ExecutableCoordinator()._build_task_handler_command(path=bundle)

    def test_rejects_a_bundle_without_a_schema_version(self, tmp_path):
        metadata = _make_metadata(["etl"])
        del metadata["sdk"]["supervisor_schema_version"]
        bundle = _build_bundle(tmp_path / "etl", metadata=metadata)

        with pytest.raises(ValueError, match="supervisor_schema_version"):
            ExecutableCoordinator()._build_task_handler_command(path=bundle)


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
    """A directory with one bundle that declares no Dag id, so a task cannot find it by its Dag id."""
    _build_bundle(tmp_path / "my_bundle", dag_ids=[])
    return tmp_path


@pytest.fixture
def go_task_handlers(bundles_dir):
    """Register *bundles_dir* as the ``go-task-handlers`` Dag bundle and return that name."""
    with register_dag_bundle("go-task-handlers", bundles_dir) as name:
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
    dag_rel_path: str = "my_bundle",
    task_handler_artifact: TaskHandlerArtifactRef | None = None,
    coordinator: ExecutableCoordinator | None = None,
) -> tuple[BaseCoordinator.ExecutionResult, list[list[str]]]:
    """Run a task of the Dag bundle *bundle_name* and return the commands the runtime was started with."""
    return execute_task(
        coordinator or ExecutableCoordinator(),
        mock_client,
        what=_make_ti(dag_id="tutorial_dag"),
        dag_rel_path=dag_rel_path,
        bundle_info=BundleInfo(name=bundle_name),
        task_handler_artifact=task_handler_artifact,
    )


@contextlib.contextmanager
def _forbid_listing(directory: Path):
    """Fail when *directory*, or a directory in it, is listed: a task runs its file without searching."""
    real_iterdir = Path.iterdir

    def iterdir(path: Path):
        resolved = path.resolve()
        assert directory.resolve() not in (resolved, *resolved.parents), f"{path} was listed"
        return real_iterdir(path)

    with patch.object(Path, "iterdir", autospec=True, side_effect=iterdir):
        yield


class TestExecutableCoordinatorExecuteTask:
    def test_a_task_without_a_reference_runs_its_dag_file(self, bundles_dir, go_task_handlers, mock_client):
        result, popen_calls = _execute_task(mock_client, go_task_handlers)

        assert popen_calls[0][0] == str((bundles_dir / "my_bundle").resolve())
        assert isinstance(result, BaseCoordinator.ExecutionResult)
        assert result.exit_code == 0

    def test_a_referenced_bundle_runs_even_when_another_registers_the_same_dag_id(
        self, bundles_dir, go_task_handlers, mock_client
    ):
        _build_bundle(bundles_dir / "a_bundle", dag_ids=["tutorial_dag"])
        _build_bundle(bundles_dir / "z_bundle", dag_ids=["tutorial_dag"])
        reference = TaskHandlerArtifactRef(bundle_info=BundleInfo(name=go_task_handlers), rel_path="z_bundle")

        with _forbid_listing(bundles_dir):
            _, popen_calls = _execute_task(
                mock_client, "other-dags", dag_rel_path="dag.py", task_handler_artifact=reference
            )

        assert popen_calls[0][0] == str((bundles_dir / "z_bundle").resolve())

    def test_a_referenced_bundle_runs_whatever_dag_ids_it_declares(
        self, bundles_dir, go_task_handlers, mock_client
    ):
        _build_bundle(bundles_dir / "etl", dag_ids=[])
        reference = TaskHandlerArtifactRef(rel_path="etl")

        _, popen_calls = _execute_task(
            mock_client, go_task_handlers, dag_rel_path="dag.py", task_handler_artifact=reference
        )

        assert popen_calls[0][0] == str((bundles_dir / "etl").resolve())

    def test_task_handler_bundle_name_is_not_read(self, bundles_dir, go_task_handlers, mock_client):
        coordinator = ExecutableCoordinator(task_handler_bundle_name="not-a-configured-bundle")

        _, popen_calls = _execute_task(mock_client, go_task_handlers, coordinator=coordinator)

        assert popen_calls[0][0] == str((bundles_dir / "my_bundle").resolve())

    @patch.object(_PopenActivitySubprocess, "start", autospec=True)
    def test_the_schema_version_of_the_bundle_is_forwarded(self, mock_start, go_task_handlers, mock_client):
        mock_start.return_value.wait.return_value = 0

        ExecutableCoordinator().execute_task(
            what=_make_ti(dag_id="tutorial_dag"),
            dag_rel_path="my_bundle",
            bundle_info=BundleInfo(name=go_task_handlers),
            client=mock_client,
            subprocess_logs_to_stdout=False,
        )

        assert mock_start.call_args.kwargs["subprocess_schema_version"] == "2026-06-16"

    def test_a_python_dag_file_raises_the_unbound_message(self, bundles_dir, go_task_handlers, mock_client):
        (bundles_dir / "dags").mkdir()
        (bundles_dir / "dags" / "etl.py").write_text("print('hello')\n")

        with pytest.raises(TaskHandlerArtifactError) as raised:
            _execute_task(mock_client, go_task_handlers, dag_rel_path="dags/etl.py")

        assert str(raised.value) == (
            "Task 'task_1' of Dag 'tutorial_dag' has no task handler artifact, and its Dag file "
            "'dags/etl.py' is not an artifact that ExecutableCoordinator runs. Queue 'executable' routes it "
            "to a Lang-SDK coordinator, so it must be a @task.stub task the Dag processor bound to an "
            "artifact, or a task of a Dag defined in a Lang SDK. Check the import errors of 'dags/etl.py', "
            "and that the scheduler has the same [sdk] configuration as the Dag processor."
        )

    def test_a_bundle_without_the_executable_bit_raises_with_the_reason(
        self, bundles_dir, go_task_handlers, mock_client
    ):
        (bundles_dir / "my_bundle").chmod(0o644)
        reference = TaskHandlerArtifactRef(rel_path="my_bundle")

        with pytest.raises(TaskHandlerArtifactError) as raised:
            _execute_task(
                mock_client, go_task_handlers, dag_rel_path="dag.py", task_handler_artifact=reference
            )

        assert str(raised.value) == (
            f"Task handler artifact 'my_bundle' in Dag bundle '{go_task_handlers}' cannot run: "
            "my_bundle is not executable. Use a Dag bundle that keeps the executable bit; "
            "object-store Dag bundles such as S3DagBundle drop it."
        )

    @pytest.mark.skipif(os.geteuid() == 0, reason="root opens every file")
    def test_a_bundle_the_worker_cannot_open_raises_with_the_reason(
        self, bundles_dir, go_task_handlers, mock_client
    ):
        (bundles_dir / "my_bundle").chmod(0)
        reference = TaskHandlerArtifactRef(rel_path="my_bundle")

        with pytest.raises(TaskHandlerArtifactError) as raised:
            _execute_task(
                mock_client, go_task_handlers, dag_rel_path="dag.py", task_handler_artifact=reference
            )

        assert str(raised.value) == (
            f"Task handler artifact 'my_bundle' in Dag bundle '{go_task_handlers}' cannot run: "
            f"[Errno 13] Permission denied: {os.fspath(bundles_dir / 'my_bundle')!r}"
        )

    def test_a_bundle_whose_binary_does_not_match_its_digest_raises(
        self, bundles_dir, go_task_handlers, mock_client
    ):
        _build_bundle(bundles_dir / "tampered", binary_sha256=b"\x00" * 32)
        reference = TaskHandlerArtifactRef(rel_path="tampered")

        with pytest.raises(TaskHandlerArtifactError, match="is not a valid executable bundle"):
            _execute_task(
                mock_client, go_task_handlers, dag_rel_path="dag.py", task_handler_artifact=reference
            )

    def test_a_jar_is_not_an_artifact_of_this_coordinator(self, bundles_dir, go_task_handlers, mock_client):
        (bundles_dir / "etl.jar").write_bytes(b"PK\x03\x04")
        reference = TaskHandlerArtifactRef(rel_path="etl.jar")

        with pytest.raises(TaskHandlerArtifactError) as raised:
            _execute_task(
                mock_client, go_task_handlers, dag_rel_path="dag.py", task_handler_artifact=reference
            )

        assert str(raised.value) == (
            f"Task handler artifact 'etl.jar' in Dag bundle '{go_task_handlers}' is not an artifact "
            "that ExecutableCoordinator runs. A newer parse may route this task to another coordinator."
        )

    def test_a_bundle_with_an_unknown_schema_version_raises_before_the_runtime_starts(
        self, bundles_dir, go_task_handlers, mock_client
    ):
        metadata = _make_metadata(["tutorial_dag"])
        metadata["sdk"]["supervisor_schema_version"] = "1999-01-01"
        _build_bundle(bundles_dir / "bogus", metadata=metadata)
        reference = TaskHandlerArtifactRef(rel_path="bogus")

        with patch.object(_PopenActivitySubprocess, "start", autospec=True) as mock_start:
            with pytest.raises(TaskHandlerArtifactError, match="uses supervisor schema version '1999-01-01'"):
                _execute_task(
                    mock_client, go_task_handlers, dag_rel_path="dag.py", task_handler_artifact=reference
                )

        mock_start.assert_not_called()

    def test_a_missing_referenced_file_raises(self, go_task_handlers, mock_client):
        reference = TaskHandlerArtifactRef(rel_path="gone")

        with pytest.raises(
            TaskHandlerArtifactError,
            match=rf"^Task handler artifact 'gone' is not a file in Dag bundle '{go_task_handlers}'",
        ):
            _execute_task(
                mock_client, go_task_handlers, dag_rel_path="dag.py", task_handler_artifact=reference
            )
