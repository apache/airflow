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
import stat
from dataclasses import replace
from pathlib import Path

import pytest

from airflow.dag_processing.bundles import manifest as manifest_module
from airflow.dag_processing.bundles.manifest import (
    MANIFEST_FILE_NAME,
    SHA256_VERSION_PREFIX,
    BundleManifestError,
    BundleManifestSourceChangedError,
    build_bundle_version_manifest,
    collect_bundle_source_snapshot,
    compute_bundle_version,
    is_ignored_bundle_file_name,
    is_sha256_hex,
    is_valid_bundle_version,
    serialize_bundle_version_manifest,
    validate_bundle_relative_path,
    validate_bundle_version,
    verify_bundle_version_manifest,
)


def _build_manifest(**kwargs):
    return build_bundle_version_manifest(**kwargs).manifest


def _write_file(root, relative_path, content):
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def test_manifest_hash_is_stable_across_filesystem_creation_order(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"

    _write_file(first, "dags/a.py", "print('a')")
    _write_file(first, "dags/nested/b.py", "print('b')")
    _write_file(second, "dags/nested/b.py", "print('b')")
    _write_file(second, "dags/a.py", "print('a')")

    first_manifest = _build_manifest(bundle_name="manifest-local", root=first, backend_type="local")
    second_manifest = _build_manifest(bundle_name="manifest-local", root=second, backend_type="local")

    assert first_manifest["version"] == second_manifest["version"]
    assert first_manifest["files"] == second_manifest["files"]


def test_manifest_hash_changes_when_file_content_changes(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "dags/example.py", "print('first')")
    first_manifest = _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")

    _write_file(source, "dags/example.py", "print('second')")
    second_manifest = _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")

    assert first_manifest["version"] != second_manifest["version"]


def test_bundle_version_only_uses_file_identity_fields():
    file_info = {
        "path": "example.py",
        "sha256": "0" * 64,
        "size": 1,
        "executable": False,
        "future_metadata": "ignored",
    }

    assert compute_bundle_version([file_info]) == compute_bundle_version(
        [{key: value for key, value in file_info.items() if key != "future_metadata"}]
    )


def test_manifest_uses_complete_precomputed_file_hashes(tmp_path, monkeypatch):
    source = tmp_path / "source"
    content = "print('dag')"
    _write_file(source, "dags/example.py", content)
    source_snapshot = collect_bundle_source_snapshot(source)
    expected_digest = hashlib.sha256(content.encode()).hexdigest()

    def fail_hash(_):
        raise AssertionError("precomputed hashes must skip a second source read")

    monkeypatch.setattr(manifest_module, "compute_file_sha256", fail_hash)
    result = build_bundle_version_manifest(
        bundle_name="manifest-s3",
        root=source,
        backend_type="local",
        source_snapshot=source_snapshot,
        precomputed_file_sha256={"dags/example.py": expected_digest},
    )

    assert result.manifest["files"][0]["sha256"] == expected_digest


@pytest.mark.parametrize(
    ("precomputed_hashes", "expected_message"),
    [
        ({}, "Precomputed file hashes do not match"),
        ({"other.py": "0" * 64}, "Precomputed file hashes do not match"),
        ({"dags/example.py": "not-a-sha256"}, "Precomputed file hash is invalid"),
        ({"dags/example.py": "z" * 64}, "Precomputed file hash is invalid"),
        ({"dags/example.py": "A" * 64}, "Precomputed file hash is invalid"),
    ],
)
def test_manifest_rejects_invalid_precomputed_file_hashes(tmp_path, precomputed_hashes, expected_message):
    source = tmp_path / "source"
    _write_file(source, "dags/example.py", "print('dag')")

    with pytest.raises(BundleManifestError, match=expected_message):
        build_bundle_version_manifest(
            bundle_name="manifest-s3",
            root=source,
            backend_type="local",
            precomputed_file_sha256=precomputed_hashes,
        )


def test_manifest_hash_changes_when_executable_bit_changes(tmp_path):
    source = tmp_path / "source"
    script = _write_file(source, "scripts/run.sh", "#!/bin/sh\n")
    first_manifest = _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")

    script.chmod(stat.S_IMODE(script.stat().st_mode) | stat.S_IXUSR)
    second_manifest = _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")

    assert first_manifest["version"] != second_manifest["version"]
    assert first_manifest["files"][0]["executable"] is False
    assert second_manifest["files"][0]["executable"] is True


def test_manifest_rejects_source_symlink(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    target = tmp_path / "outside.py"
    target.write_text("print('outside')")
    try:
        (source / "linked.py").symlink_to(target)
    except OSError as e:
        pytest.skip(f"Symlinks are not supported in this test environment: {e}")

    with pytest.raises(BundleManifestError, match="symlinked file"):
        _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")


def test_manifest_rejects_source_symlinked_directory(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    target = tmp_path / "outside"
    target.mkdir()
    try:
        (source / "linked").symlink_to(target, target_is_directory=True)
    except OSError as e:
        pytest.skip(f"Symlinks are not supported in this test environment: {e}")

    with pytest.raises(BundleManifestError, match="symlinked directory"):
        _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")


@pytest.mark.parametrize(
    ("file_name", "expected"),
    [
        (".airflow-bundle-manifest.json", True),
        (".git", True),
        ("example.pyc", True),
        ("example.py", False),
        (".pyc", False),
    ],
)
def test_ignored_bundle_file_names(file_name, expected):
    assert is_ignored_bundle_file_name(file_name) is expected


def test_manifest_rejects_source_walk_error(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()

    def fail_walk(*args, onerror, **kwargs):
        onerror(PermissionError(13, "Permission denied", str(source / "unreadable")))

    monkeypatch.setattr(manifest_module.os, "walk", fail_walk)

    with pytest.raises(BundleManifestSourceChangedError, match="changed or became unreadable"):
        _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")


def test_manifest_ignores_cache_and_vcs_files(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "dags/example.py", "print('dag')")
    manifest_before = _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")

    _write_file(source, ".git/config", "[core]")
    _write_file(source, MANIFEST_FILE_NAME, "{}")
    _write_file(source, "dags/__pycache__/example.cpython-312.pyc", "compiled")
    manifest_after = _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")

    assert manifest_after["version"] == manifest_before["version"]
    assert [file_info["path"] for file_info in manifest_after["files"]] == ["dags/example.py"]


def test_manifest_ignores_symlinked_ignored_directory(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "dags/example.py", "print('dag')")
    manifest_before = _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")

    real_git_dir = tmp_path / "real-git"
    real_git_dir.mkdir()
    try:
        (source / ".git").symlink_to(real_git_dir, target_is_directory=True)
    except OSError as e:
        pytest.skip(f"Symlinks are not supported in this test environment: {e}")
    manifest_after = _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")

    assert manifest_after["version"] == manifest_before["version"]


def test_manifest_ignores_git_worktree_pointer_file(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "dags/example.py", "print('dag')")
    manifest_before = _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")

    _write_file(source, ".git", "gitdir: /repos/main/.git/worktrees/dags")
    manifest_after = _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")

    assert manifest_after["version"] == manifest_before["version"]
    assert [file_info["path"] for file_info in manifest_after["files"]] == ["dags/example.py"]


def test_manifest_paths_are_relative_and_sorted(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "z.py", "print('z')")
    _write_file(source, "a/nested.py", "print('nested')")
    _write_file(source, "a.py", "print('a')")

    manifest = _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")
    paths = [file_info["path"] for file_info in manifest["files"]]

    assert paths == ["a.py", "a/nested.py", "z.py"]
    assert str(source) not in json.dumps(manifest)


def test_ref_payload_is_compact_and_points_to_manifest(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "dags/example.py", "print('dag')")

    result = build_bundle_version_manifest(bundle_name="manifest-local", root=source, backend_type="local")

    assert result.ref_payload == {
        "schema_version": 1,
        "bundle_name": "manifest-local",
        "version": result.version,
        "backend": {"type": "local"},
        "manifest": {
            "path": MANIFEST_FILE_NAME,
            "sha256": result.ref_payload["manifest"]["sha256"],
        },
        "file_count": 1,
        "total_size": len("print('dag')"),
    }
    assert result.ref_payload["manifest"]["sha256"].startswith("sha256:")
    assert "files" not in result.ref_payload
    assert str(source) not in json.dumps(result.ref_payload)


@pytest.mark.parametrize(
    ("source_name", "source_factory", "expected_exception"),
    [
        ("missing", lambda path: None, FileNotFoundError),
        ("file", lambda path: path.write_text("not a directory"), NotADirectoryError),
    ],
)
def test_manifest_rejects_invalid_source_roots(tmp_path, source_name, source_factory, expected_exception):
    source = tmp_path / source_name
    source_factory(source)

    with pytest.raises(expected_exception):
        collect_bundle_source_snapshot(source)


def test_manifest_rejects_snapshot_from_another_root(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    _write_file(first, "example.py", "first")
    _write_file(second, "example.py", "second")

    with pytest.raises(ValueError, match="source_snapshot root"):
        build_bundle_version_manifest(
            bundle_name="manifest-local",
            root=second,
            backend_type="local",
            source_snapshot=collect_bundle_source_snapshot(first),
        )


def test_manifest_rejects_file_removed_after_source_snapshot(tmp_path):
    source = tmp_path / "source"
    source_file = _write_file(source, "example.py", "content")
    source_snapshot = collect_bundle_source_snapshot(source)
    source_file.unlink()

    with pytest.raises(BundleManifestSourceChangedError, match="disappeared while building manifest"):
        build_bundle_version_manifest(
            bundle_name="manifest-local",
            root=source,
            backend_type="local",
            source_snapshot=source_snapshot,
        )


def test_manifest_rejects_file_metadata_changed_after_source_snapshot(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "example.py", "content")
    source_snapshot = collect_bundle_source_snapshot(source)
    changed_file = replace(source_snapshot.files[0], size=source_snapshot.files[0].size + 1)

    with pytest.raises(BundleManifestSourceChangedError, match="changed while building manifest"):
        build_bundle_version_manifest(
            bundle_name="manifest-local",
            root=source,
            backend_type="local",
            source_snapshot=replace(source_snapshot, files=(changed_file,)),
        )


def test_manifest_verification_accepts_prefixed_and_bare_digests(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "example.py", "content")
    manifest = _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")
    digest = hashlib.sha256(serialize_bundle_version_manifest(manifest)).hexdigest()

    verify_bundle_version_manifest(manifest, digest)
    verify_bundle_version_manifest(manifest, f"sha256:{digest}")


def test_manifest_verification_rejects_digest_mismatch(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "example.py", "content")
    manifest = _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")

    with pytest.raises(BundleManifestError, match="manifest digest mismatch"):
        verify_bundle_version_manifest(manifest, "0" * 64)


def test_computed_bundle_version_passes_its_own_validator(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "dags/example.py", "print('dag')")

    version = _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")["version"]

    assert version.startswith(SHA256_VERSION_PREFIX)
    assert is_valid_bundle_version(version)
    assert validate_bundle_version(version, source="test") == version


@pytest.mark.parametrize(
    "version",
    [
        "",
        "../outside",
        "sha256-bad",
        f"sha256-{'A' * 64}",
        f"sha256-{'a' * 63}",
        f"sha256-{'a' * 65}",
        f"sha256:{'a' * 64}",
        "a" * 64,
        f"sha256-{'a' * 63}/",
        f"sha256-sha256-{'a' * 50}",
    ],
)
def test_bundle_version_validation_rejects_malformed_versions(version):
    assert is_valid_bundle_version(version) is False
    with pytest.raises(BundleManifestError, match="valid sha256 version"):
        validate_bundle_version(version, source="pinned bundle version")


@pytest.mark.parametrize("relative_path", ["a.py", "dags/example.py", "dags/nested/example.py"])
def test_bundle_relative_path_validation_keeps_accepted_paths_under_the_root(tmp_path, relative_path):
    resolved = (tmp_path / validate_bundle_relative_path(relative_path)).resolve()

    assert resolved.is_relative_to(tmp_path.resolve())
    assert resolved != tmp_path.resolve()


@pytest.mark.parametrize(
    "relative_path",
    [
        "",
        "..",
        "../evil.py",
        "dags/../../evil.py",
        "/tmp/evil.py",
        "dags//example.py",
        "dags/./example.py",
        "dags/",
        ".",
        "./example.py",
        "/",
        "dags/ex\x00ample.py",
        "dags/ex\nample.py",
        "dags/ex\tample.py",
    ],
)
def test_bundle_relative_path_validation_rejects_unsafe_paths(relative_path):
    with pytest.raises(BundleManifestError, match="unsafe relative path"):
        validate_bundle_relative_path(relative_path)


def test_source_verification_does_not_follow_symlinks(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source_file = _write_file(source, "example.py", "content")
    source_snapshot = collect_bundle_source_snapshot(source)

    followed: list[Path] = []
    real_stat = Path.stat

    # Path.lstat() delegates to Path.stat(follow_symlinks=False), so only the
    # following calls distinguish "verified the file itself" from "verified its target".
    def recording_stat(self, *, follow_symlinks=True):
        if follow_symlinks:
            followed.append(Path(self))
        return real_stat(self, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(Path, "stat", recording_stat)
    build_bundle_version_manifest(
        bundle_name="manifest-local",
        root=source,
        backend_type="local",
        source_snapshot=source_snapshot,
    )

    assert source_file not in followed


def test_bundle_version_validation_names_the_source_in_the_message():
    with pytest.raises(BundleManifestError, match="pinned bundle version"):
        validate_bundle_version("sha256-bad", source="pinned bundle version")


@pytest.mark.parametrize(
    ("value", "expected"),
    [("a" * 64, True), ("0123456789abcdef" * 4, True), ("A" * 64, False), ("z" * 64, False)],
)
def test_sha256_hex_recognition(value, expected):
    assert is_sha256_hex(value) is expected


def test_manifest_ignores_non_bytecode_files_under_pycache(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "dags/example.py", "print('dag')")
    manifest_before = _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")

    _write_file(source, "dags/__pycache__/notes.txt", "not bytecode")
    manifest_after = _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")

    assert manifest_after["version"] == manifest_before["version"]
    assert [file_info["path"] for file_info in manifest_after["files"]] == ["dags/example.py"]


def test_manifest_serialization_is_independent_of_key_insertion_order(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "dags/example.py", "print('dag')")
    manifest = _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")
    reordered = dict(reversed(list(manifest.items())))

    assert list(reordered) != list(manifest)
    assert serialize_bundle_version_manifest(reordered) == serialize_bundle_version_manifest(manifest)


def test_source_snapshot_signature_covers_every_metadata_field(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "example.py", "content")
    source_snapshot = collect_bundle_source_snapshot(source)

    assert source_snapshot.signature.startswith("sha256:")
    for field, value in [("size", 1), ("mtime_ns", 1), ("ctime_ns", 1), ("mode", 0o600)]:
        changed = replace(source_snapshot.files[0], **{field: value})
        assert changed.build_signature_record() != source_snapshot.files[0].build_signature_record()


@pytest.mark.parametrize(
    ("field", "value"),
    [("size", 999), ("mtime_ns", 1), ("ctime_ns", 1), ("mode", 0o600)],
)
def test_manifest_rejects_any_source_metadata_field_changing(tmp_path, field, value):
    source = tmp_path / "source"
    _write_file(source, "example.py", "content")
    source_snapshot = collect_bundle_source_snapshot(source)
    changed_file = replace(source_snapshot.files[0], **{field: value})

    with pytest.raises(BundleManifestSourceChangedError, match="changed while building manifest"):
        build_bundle_version_manifest(
            bundle_name="manifest-local",
            root=source,
            backend_type="local",
            source_snapshot=replace(source_snapshot, files=(changed_file,)),
        )


def test_manifest_rejects_file_removed_between_hashing_and_verification(tmp_path):
    source = tmp_path / "source"
    source_file = _write_file(source, "example.py", "content")
    source_snapshot = collect_bundle_source_snapshot(source)
    digest = hashlib.sha256(b"content").hexdigest()
    source_file.unlink()

    # Precomputed hashes skip the read, so verification is the first access to the file.
    with pytest.raises(BundleManifestSourceChangedError, match="disappeared while verifying"):
        build_bundle_version_manifest(
            bundle_name="manifest-local",
            root=source,
            backend_type="local",
            source_snapshot=source_snapshot,
            precomputed_file_sha256={"example.py": digest},
        )


def test_backslash_in_a_posix_file_name_survives_publish_and_validation(tmp_path):
    source = tmp_path / "source"
    _write_file(source, "dags/weird\\name.py", "print('dag')")

    manifest = _build_manifest(bundle_name="manifest-local", root=source, backend_type="local")
    recorded_path = manifest["files"][0]["path"]

    assert recorded_path == "dags/weird\\name.py"
    assert validate_bundle_relative_path(recorded_path) == Path(recorded_path)
