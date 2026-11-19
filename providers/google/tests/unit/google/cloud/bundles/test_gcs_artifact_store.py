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

import builtins
import importlib.util
import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.google.version_compat import get_base_airflow_version_tuple

if get_base_airflow_version_tuple() >= (3, 5, 0):
    from airflow.dag_processing.bundles.artifact_store import (
        ArtifactStoreConflictError,
        BundleManifestNotFoundError,
        BundleManifestRefDurabilityError,
    )
    from airflow.dag_processing.bundles.manifest import (
        MANIFEST_FILE_NAME,
        BundleManifestError,
        BundleManifestSourceChangedError,
        build_bundle_version_manifest,
        serialize_bundle_version_manifest,
    )
    from airflow.providers.google.cloud.bundles import gcs_artifact_store as gcs_store_module
    from airflow.providers.google.cloud.bundles.gcs_artifact_store import (
        GCSArtifactStore,
        parse_gcs_published_root,
    )

else:
    pytest.skip("Manifest bundles require Airflow 3.5+", allow_module_level=True)

from unit.google.cloud.bundles.gcs_fakes import FakeGCSStoreError, FakeStoreGCSBlob, FakeStoreGCSClient

BUNDLE_NAME = "manifest-gcs"
BUCKET = "release-bucket"


def _write_source(root: Path) -> Path:
    (root / "dags").mkdir(parents=True, exist_ok=True)
    (root / "dags" / "example.py").write_text("print('dag')\n")
    return root


def _two_file_source(root: Path) -> Path:
    _write_source(root)
    (root / "dags" / "second.py").write_text("print('second')\n")
    return root


def _manifest_result(source: Path):
    return build_bundle_version_manifest(
        bundle_name=BUNDLE_NAME,
        root=source,
        backend_type="gcs",
    )


@pytest.fixture
def fake_gcs(monkeypatch):
    client = FakeStoreGCSClient()
    seen_conn_ids: list[str] = []

    class FakeHook:
        default_conn_name = "google_cloud_default"

        def __init__(self, *, gcp_conn_id: str) -> None:
            seen_conn_ids.append(gcp_conn_id)

        def get_conn(self):
            return client

    monkeypatch.setattr(gcs_store_module, "GCSHook", FakeHook)
    client.seen_conn_ids = seen_conn_ids
    return client


def _gcs_artifact_store(prefix: str = "releases") -> GCSArtifactStore:
    root = f"gs://{BUCKET}/{prefix}" if prefix else f"gs://{BUCKET}"
    return GCSArtifactStore(bundle_name=BUNDLE_NAME, published_root=root)


class TestParseGCSPublishedRoot:
    def test_bucket_and_prefix(self):
        assert parse_gcs_published_root("gs://bucket/some/prefix/") == ("bucket", "some/prefix")

    def test_bucket_only(self):
        assert parse_gcs_published_root("gs://bucket") == ("bucket", "")

    @pytest.mark.parametrize(
        "url",
        ["http://bucket/prefix", "gs://", "gs:///prefix", "gs://bucket/prefix?versionId=1"],
    )
    def test_rejects_invalid_urls(self, url):
        with pytest.raises(TypeError):
            parse_gcs_published_root(url)


class TestGCSStoreDocumentCAS:
    def _ref_key(self) -> str:
        return f"releases/refs/{BUNDLE_NAME}/latest.json"

    def test_first_write_conditions_on_absence(self, fake_gcs):
        store = _gcs_artifact_store()
        with pytest.raises(BundleManifestNotFoundError):
            store.read_ref()
        store.write_ref({"schema_version": 1})
        assert fake_gcs.put_conditions == [(self._ref_key(), 0)]

    def test_write_without_prior_read_establishes_a_baseline(self, fake_gcs):
        store = _gcs_artifact_store()
        store.write_ref({"schema_version": 1})
        assert fake_gcs.put_conditions == [(self._ref_key(), 0)]

    def test_replacement_conditions_on_the_read_generation(self, fake_gcs):
        expected_generation = fake_gcs.put(self._ref_key(), b"{}", bucket=BUCKET)
        store = _gcs_artifact_store()
        store.read_ref()
        store.write_ref({"schema_version": 1})
        assert fake_gcs.put_conditions == [(self._ref_key(), expected_generation)]

    def test_lost_create_race_with_different_content_conflicts(self, fake_gcs):
        store = _gcs_artifact_store()
        with pytest.raises(BundleManifestNotFoundError):
            store.read_ref()
        fake_gcs.put(self._ref_key(), b'{"winner":true}', bucket=BUCKET)
        with pytest.raises(ArtifactStoreConflictError):
            store.write_ref({"schema_version": 1})
        # The conflict refreshed the baseline, so a retry replaces the winner cleanly.
        store.write_ref({"schema_version": 2})
        assert json.loads(fake_gcs.get(self._ref_key(), bucket=BUCKET)) == {"schema_version": 2}

    def test_lost_race_with_identical_content_is_an_idempotent_win(self, fake_gcs):
        store = _gcs_artifact_store()
        with pytest.raises(BundleManifestNotFoundError):
            store.read_ref()
        payload = {"schema_version": 1, "bundle_name": BUNDLE_NAME}
        fake_gcs.put(self._ref_key(), serialize_bundle_version_manifest(payload), bucket=BUCKET)
        store.write_ref(payload)

    def test_stale_baseline_conflicts_when_another_publisher_won(self, fake_gcs):
        fake_gcs.put(self._ref_key(), b"{}", bucket=BUCKET)
        store = _gcs_artifact_store()
        store.read_ref()
        fake_gcs.put(self._ref_key(), b'{"winner":true}', bucket=BUCKET)
        with pytest.raises(ArtifactStoreConflictError):
            store.write_ref({"schema_version": 1})

    def test_missing_bucket_is_a_configuration_error_not_a_missing_release(self, fake_gcs):
        fake_gcs.missing_buckets = {BUCKET}
        store = _gcs_artifact_store()
        with pytest.raises(BundleManifestError, match="does not exist. Fix the published_root"):
            store.read_ref()


class TestGCSStorePublishSnapshot:
    def test_manifest_object_is_committed_last(self, fake_gcs, tmp_path):
        source = _write_source(tmp_path / "source")
        result = _manifest_result(source)
        store = _gcs_artifact_store()
        created = store.publish_snapshot(
            manifest=result.manifest,
            source_root=source,
            validate_existing=lambda tree: None,
        )
        assert created.created is True
        snapshot_puts = [key for key in fake_gcs.put_sequence if f"/{result.version}/" in key]
        assert snapshot_puts[-1].endswith(f"/{MANIFEST_FILE_NAME}")
        assert len(snapshot_puts) == len(result.manifest["files"]) + 1

    def test_tampered_committed_manifest_is_refused(self, fake_gcs, tmp_path):
        source = _write_source(tmp_path / "source")
        result = _manifest_result(source)
        fake_gcs.put(
            f"releases/versions/{BUNDLE_NAME}/{result.version}/{MANIFEST_FILE_NAME}",
            b'{"tampered": true}',
            bucket=BUCKET,
        )
        store = _gcs_artifact_store()
        with pytest.raises(BundleManifestError, match="schema_version"):
            store.publish_snapshot(
                manifest=result.manifest,
                source_root=source,
                validate_existing=lambda tree: None,
            )

    def test_source_drift_aborts_before_the_manifest_commits(self, fake_gcs, tmp_path):
        source = _write_source(tmp_path / "source")
        result = _manifest_result(source)
        (source / "dags" / "example.py").write_text("print('drifted')\n")
        store = _gcs_artifact_store()
        with pytest.raises(BundleManifestSourceChangedError):
            store.publish_snapshot(
                manifest=result.manifest,
                source_root=source,
                validate_existing=lambda tree: None,
            )
        assert store.snapshot_exists(result.version) is False


def _gcs_copy_hints_for(fake_gcs, source: Path, result, *, endpoint=None):
    hints = {}
    for file_info in result.manifest["files"]:
        name = f"dags-src/{file_info['path']}"
        generation = fake_gcs.put(name, (source / file_info["path"]).read_bytes(), bucket="source-bucket")
        hints[file_info["path"]] = {
            "type": "gcs",
            "endpoint": endpoint,
            "bucket": "source-bucket",
            "name": name,
            "generation": generation,
        }
    return hints


class TestGCSStoreServerSideRewrite:
    def _publish_with_hints(self, source, hints):
        result = _manifest_result(source)
        store = _gcs_artifact_store()
        store.copy_hints = hints
        created = store.publish_snapshot(
            manifest=result.manifest,
            source_root=source,
            validate_existing=lambda tree: None,
        )
        return store, result, created

    def test_matching_endpoint_rewrites_instead_of_uploading(self, fake_gcs, tmp_path):
        source = _two_file_source(tmp_path / "source")
        result = _manifest_result(source)
        hints = _gcs_copy_hints_for(fake_gcs, source, result)

        store, result, created = self._publish_with_hints(source, hints)

        assert created.created is True
        assert len(fake_gcs.rewrites) == len(result.manifest["files"])
        snapshot_puts = [key for key in fake_gcs.put_sequence if f"/{result.version}/" in key]
        assert snapshot_puts == [f"releases/versions/{BUNDLE_NAME}/{result.version}/{MANIFEST_FILE_NAME}"]
        destination = tmp_path / "dest"
        destination.mkdir()
        store.fetch_snapshot(result.version, destination, structural_validator=lambda tree: None)
        assert (destination / "dags" / "second.py").read_text() == "print('second')\n"

    def test_endpoint_mismatch_uploads_everything(self, fake_gcs, tmp_path):
        source = _two_file_source(tmp_path / "source")
        result = _manifest_result(source)
        hints = _gcs_copy_hints_for(fake_gcs, source, result, endpoint="https://elsewhere.example.test")

        store, result, created = self._publish_with_hints(source, hints)

        assert created.created is True
        assert fake_gcs.rewrites == []
        assert store.snapshot_exists(result.version)

    def test_stale_source_generation_falls_back_for_that_file_only(self, fake_gcs, tmp_path):
        source = _two_file_source(tmp_path / "source")
        result = _manifest_result(source)
        hints = _gcs_copy_hints_for(fake_gcs, source, result)
        stale = hints["dags/example.py"]
        fake_gcs.put(stale["name"], b"moved on", bucket=stale["bucket"])

        _store, result, created = self._publish_with_hints(source, hints)

        assert created.created is True
        assert len(fake_gcs.rewrites) == 1
        uploaded = [
            key
            for key in fake_gcs.put_sequence
            if f"/{result.version}/" in key and not key.endswith(MANIFEST_FILE_NAME)
        ]
        assert uploaded == [f"releases/versions/{BUNDLE_NAME}/{result.version}/dags/example.py"]
        # The published object carries the manifest's bytes, not the moved-on source.
        assert (
            fake_gcs.get(
                f"releases/versions/{BUNDLE_NAME}/{result.version}/dags/example.py",
                bucket=BUCKET,
            )
            == b"print('dag')\n"
        )

    def test_systemic_rewrite_failure_disables_further_attempts(self, fake_gcs, tmp_path):
        source = _two_file_source(tmp_path / "source")
        result = _manifest_result(source)
        hints = _gcs_copy_hints_for(fake_gcs, source, result)
        fake_gcs.fail_rewrite_with = 403

        store, result, created = self._publish_with_hints(source, hints)

        assert created.created is True
        assert fake_gcs.rewrite_attempts == 1
        assert fake_gcs.rewrites == []
        assert store.snapshot_exists(result.version)


@pytest.mark.parametrize("document", ["ref", "state"])
def test_publication_guard_discards_reads_from_previous_guard(fake_gcs, document):
    store = _gcs_artifact_store()
    read = getattr(store, f"read_{document}")
    write = getattr(store, f"write_{document}")
    with store.publication_guard():
        write({"old": True})
        read()
    other = _gcs_artifact_store()
    with other.publication_guard():
        getattr(other, f"write_{document}")({"other": True})
    with store.publication_guard(wait=False):
        write({"new": True})
    assert read() == {"new": True}


@pytest.mark.parametrize("document", ["ref", "state"])
@pytest.mark.parametrize("body", [b"{", b"[]", b"\xff"])
def test_malformed_documents_can_be_replaced_conditionally(fake_gcs, document, body):
    store = _gcs_artifact_store()
    key = store._ref_key if document == "ref" else store._state_key
    generation = fake_gcs.put(key, body, bucket=BUCKET)
    with store.publication_guard():
        with pytest.raises(BundleManifestError, match="not a JSON object"):
            getattr(store, f"read_{document}")()
        getattr(store, f"write_{document}")({"fixed": True})
    assert fake_gcs.put_conditions == [(key, generation)]


@pytest.mark.parametrize("code", [403, 500])
def test_reference_write_reports_uncertain_completion_only_after_ambiguous_failure(fake_gcs, code):
    fake_gcs.fail_conditional_writes_with = code
    store = _gcs_artifact_store()
    with pytest.raises(BundleManifestError) as error:
        store.write_ref({"version": "release"})
    assert isinstance(error.value, BundleManifestRefDurabilityError) is (code == 500)


def test_snapshot_race_keeps_first_manifest_with_its_object_versions(fake_gcs, tmp_path, monkeypatch):
    source = _write_source(tmp_path / "source")
    result = _manifest_result(source)
    winner = deepcopy(result.manifest)
    winner["files"][0]["version_id"] = "other-generation"
    original_upload = FakeStoreGCSBlob.upload_from_string

    def publish_competitor(blob, body, **kwargs):
        if blob.name.endswith(MANIFEST_FILE_NAME):
            fake_gcs.put(blob.name, serialize_bundle_version_manifest(winner), bucket=BUCKET)
        return original_upload(blob, body, **kwargs)

    monkeypatch.setattr(FakeStoreGCSBlob, "upload_from_string", publish_competitor)
    store = _gcs_artifact_store()
    publication = store.publish_snapshot(
        manifest=result.manifest, source_root=source, validate_existing=lambda tree: None
    )
    assert publication.created is False
    assert publication.manifest == winner
    assert (
        json.loads(fake_gcs.get(store._build_snapshot_key(result.version, MANIFEST_FILE_NAME), bucket=BUCKET))
        == winner
    )


def test_republish_preserves_manifest_for_identical_content(fake_gcs, tmp_path):
    source = _write_source(tmp_path / "source")
    result = _manifest_result(source)
    store = _gcs_artifact_store()
    first = store.publish_snapshot(
        manifest=result.manifest, source_root=source, validate_existing=lambda tree: None
    )
    updated = deepcopy(result.manifest)
    updated["files"][0]["version_id"] = "new-generation"
    puts = list(fake_gcs.put_sequence)
    second = store.publish_snapshot(manifest=updated, source_root=source, validate_existing=lambda tree: None)
    assert second.created is False
    assert second.manifest == first.manifest
    assert fake_gcs.put_sequence == puts


@pytest.mark.parametrize("failure", ["missing", "corrupt"])
def test_fetch_rejects_unavailable_or_corrupt_snapshot_files(fake_gcs, tmp_path, failure):
    source = _write_source(tmp_path / "source")
    result = _manifest_result(source)
    store = _gcs_artifact_store()
    store.publish_snapshot(manifest=result.manifest, source_root=source, validate_existing=lambda tree: None)
    key = store._build_snapshot_key(result.version, "dags/example.py")
    if failure == "missing":
        fake_gcs.pop(key, bucket=BUCKET)
    else:
        fake_gcs.put(key, b"corrupt", bucket=BUCKET)
    with pytest.raises(BundleManifestError, match="missing manifest entry|does not match"):
        store.fetch_snapshot(result.version, tmp_path / "cache", structural_validator=lambda tree: None)


@pytest.mark.parametrize("path", ["../escape.py", "/escape.py", MANIFEST_FILE_NAME])
def test_fetch_validates_manifest_paths_before_creating_files(fake_gcs, tmp_path, path):
    source = _write_source(tmp_path / "source")
    result = _manifest_result(source)
    store = _gcs_artifact_store()
    result.manifest["files"][0]["path"] = path
    fake_gcs.put(
        store._build_snapshot_key(result.version, MANIFEST_FILE_NAME),
        serialize_bundle_version_manifest(result.manifest),
        bucket=BUCKET,
    )
    with pytest.raises(BundleManifestError):
        store.fetch_snapshot(result.version, tmp_path / "cache", structural_validator=lambda tree: None)
    assert not (tmp_path / "cache").exists()
    assert not (tmp_path / "escape.py").exists()


def test_fetch_requires_an_empty_destination(fake_gcs, tmp_path):
    destination = tmp_path / "cache"
    destination.mkdir()
    (destination / "existing").write_bytes(b"keep")
    with pytest.raises(ValueError, match="absent or empty"):
        _gcs_artifact_store().fetch_snapshot(
            "sha256-" + "a" * 64, destination, structural_validator=lambda tree: None
        )
    assert (destination / "existing").read_bytes() == b"keep"


@pytest.mark.parametrize("connection", ["", 123])
def test_store_rejects_invalid_connection_ids(connection):
    with pytest.raises(TypeError, match="published_root_conn_id"):
        GCSArtifactStore(bundle_name=BUNDLE_NAME, published_root="gs://releases", gcp_conn_id=connection)


@pytest.mark.parametrize("failure", ["constructor", "client"])
@mock.patch.object(gcs_store_module, "GCSHook", autospec=True)
def test_store_wraps_connection_errors(hook, failure):
    store = GCSArtifactStore(bundle_name=BUNDLE_NAME, published_root="gs://releases", gcp_conn_id="releases")
    if failure == "constructor":
        hook.side_effect = RuntimeError("credentials unavailable")
    else:
        hook.return_value.get_conn.side_effect = RuntimeError("credentials unavailable")
    with pytest.raises(BundleManifestError, match="client for published_root"):
        store.read_ref()
    hook.assert_called_once_with(gcp_conn_id="releases")


@pytest.mark.parametrize("generation", [None, True, 0, "1"])
@mock.patch.object(FakeStoreGCSBlob, "download_as_bytes", autospec=True)
def test_read_without_generation_cannot_establish_a_cas_baseline(download, fake_gcs, generation):
    def respond(blob):
        blob.generation = generation
        return b"{}"

    download.side_effect = respond
    store = _gcs_artifact_store()
    with pytest.raises(BundleManifestError, match="did not return the generation"):
        store.write_ref({"version": "candidate"})
    assert fake_gcs.put_sequence == []


@pytest.mark.parametrize("operation", ["read_ref", "snapshot_exists"])
@pytest.mark.parametrize("code", [403, 500])
@mock.patch.object(FakeStoreGCSBlob, "download_as_bytes", autospec=True)
@mock.patch.object(FakeStoreGCSBlob, "reload", autospec=True)
def test_store_read_failures_are_not_absence(reload, download, fake_gcs, operation, code):
    reload.side_effect = download.side_effect = FakeGCSStoreError(code)
    store = _gcs_artifact_store()
    args = () if operation == "read_ref" else ("sha256-" + "0" * 64,)
    with pytest.raises(BundleManifestError, match="Could not read"):
        getattr(store, operation)(*args)


@pytest.mark.parametrize("body", [b"invalid JSON", b"[]"])
def test_fetch_rejects_invalid_snapshot_manifests(fake_gcs, tmp_path, body):
    store = _gcs_artifact_store()
    version = "sha256-" + "0" * 64
    fake_gcs.put(store._build_snapshot_key(version, MANIFEST_FILE_NAME), body, bucket=BUCKET)
    destination = tmp_path / "cache"
    with pytest.raises(BundleManifestError, match="manifest"):
        store.fetch_snapshot(version, destination, structural_validator=lambda _: None)
    assert not destination.exists()


def test_fetch_missing_snapshot_reports_not_found(fake_gcs, tmp_path):
    store = _gcs_artifact_store()
    with pytest.raises(BundleManifestNotFoundError, match="not published"):
        store.fetch_snapshot("sha256-" + "0" * 64, tmp_path / "cache", structural_validator=lambda _: None)


@pytest.mark.parametrize("failure", ["file_upload", "manifest_upload", "missing_source"])
def test_failed_publication_leaves_no_commit_marker(fake_gcs, tmp_path, monkeypatch, failure):
    store = _gcs_artifact_store()
    source = _write_source(tmp_path / "source")
    manifest = _manifest_result(source).manifest
    if failure == "missing_source":
        (source / "dags/example.py").unlink()
    else:
        original = FakeStoreGCSBlob.upload_from_string

        def fail_upload(blob, body, **kwargs):
            if (failure == "manifest_upload") == blob.name.endswith(MANIFEST_FILE_NAME):
                raise FakeGCSStoreError(403)
            return original(blob, body, **kwargs)

        monkeypatch.setattr(FakeStoreGCSBlob, "upload_from_string", fail_upload)
    with pytest.raises(BundleManifestError, match="Could not write|disappeared"):
        store.publish_snapshot(manifest=manifest, source_root=source, validate_existing=lambda _: None)
    assert not store.snapshot_exists(manifest["version"])


@pytest.mark.parametrize("body", [b"not JSON", b"{}"])
def test_republish_does_not_overwrite_corrupt_committed_manifest(fake_gcs, tmp_path, body):
    store = _gcs_artifact_store()
    source = _write_source(tmp_path / "source")
    manifest = _manifest_result(source).manifest
    key = store._build_snapshot_key(manifest["version"], MANIFEST_FILE_NAME)
    fake_gcs.put(key, body, bucket=BUCKET)
    with pytest.raises(BundleManifestError, match="manifest"):
        store.publish_snapshot(manifest=manifest, source_root=source, validate_existing=lambda _: None)
    assert fake_gcs.get(key, bucket=BUCKET) == body
    assert fake_gcs.put_sequence == []


@mock.patch.object(FakeStoreGCSBlob, "rewrite", autospec=True)
def test_rewrite_follows_continuation_token_with_generation_condition(rewrite, fake_gcs, tmp_path):
    store = _gcs_artifact_store()
    source = _write_source(tmp_path / "source")
    result = _manifest_result(source)
    store.copy_hints = _gcs_copy_hints_for(fake_gcs, source, result)
    rewrite.side_effect = [("continue", 1, 13), (None, 13, 13)]
    store.publish_snapshot(manifest=result.manifest, source_root=source, validate_existing=lambda _: None)
    assert rewrite.call_count == 2
    first, second = rewrite.call_args_list
    assert "token" not in first.kwargs
    assert second.kwargs == {
        "token": "continue",
        "if_source_generation_match": first.kwargs["if_source_generation_match"],
    }
    assert second.args[1].generation == second.kwargs["if_source_generation_match"]


@pytest.mark.parametrize("hint", [None, {"type": "s3"}, {"type": "gcs", "generation": False}])
def test_invalid_copy_hint_uploads_verified_local_file(fake_gcs, tmp_path, hint):
    store = _gcs_artifact_store()
    source = _write_source(tmp_path / "source")
    manifest = _manifest_result(source).manifest
    store.copy_hints = {"dags/example.py": hint}
    store.publish_snapshot(manifest=manifest, source_root=source, validate_existing=lambda _: None)
    assert fake_gcs.rewrite_attempts == 0
    assert (
        fake_gcs.get(store._build_snapshot_key(manifest["version"], "dags/example.py"), bucket=BUCKET)
        == (source / "dags/example.py").read_bytes()
    )


@pytest.mark.parametrize("endpoint", [None, "https://storage.googleapis.com/", "https://custom.example/"])
def test_endpoint_identity_supports_client_options(endpoint):
    client = SimpleNamespace(_client_options=SimpleNamespace(api_endpoint=endpoint))
    expected = "https://custom.example" if endpoint and "custom" in endpoint else None
    assert gcs_store_module.get_gcs_api_endpoint(client) == expected


@pytest.mark.parametrize("module_name", ["manifest_gcs", "gcs_artifact_store"])
def test_manifest_features_explain_missing_core_support(monkeypatch, module_name):
    original_import = builtins.__import__

    def import_without_manifest_support(name, *args, **kwargs):
        if name.startswith("airflow.dag_processing.bundles.artifact_store"):
            raise ImportError("manifest support unavailable")
        return original_import(name, *args, **kwargs)

    source = Path(gcs_store_module.__file__).with_name(f"{module_name}.py")
    spec = importlib.util.spec_from_file_location(f"unsupported_{module_name}", source)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setattr(builtins, "__import__", import_without_manifest_support)
    with pytest.raises(AirflowOptionalProviderFeatureException, match="Airflow 3.5 or later"):
        spec.loader.exec_module(module)


def test_missing_document_after_conflict_is_not_a_successful_write(fake_gcs):
    store = _gcs_artifact_store()
    fake_gcs.put(store._state_key, b"{}", bucket=BUCKET)
    store.read_state()
    fake_gcs.pop(store._state_key, bucket=BUCKET)
    with pytest.raises(ArtifactStoreConflictError):
        store.write_state({"candidate": "new"})
    assert (BUCKET, store._state_key) not in fake_gcs.records


@mock.patch.object(FakeStoreGCSBlob, "download_as_bytes", autospec=True)
def test_callable_error_code_preserves_not_found_semantics(download, fake_gcs):
    error = RuntimeError("object not found")
    error.code = lambda: 404
    download.side_effect = error
    with pytest.raises(BundleManifestNotFoundError):
        _gcs_artifact_store().read_ref()


def test_write_response_without_generation_reestablishes_baseline(fake_gcs, monkeypatch):
    store = _gcs_artifact_store()
    original = FakeStoreGCSBlob.upload_from_string

    def upload_without_response_generation(blob, body, **kwargs):
        original(blob, body, **kwargs)
        blob.generation = None

    monkeypatch.setattr(FakeStoreGCSBlob, "upload_from_string", upload_without_response_generation)
    store.write_state({"candidate": "first"})
    first_generation = fake_gcs.generation_of(store._state_key, bucket=BUCKET)
    store.write_state({"candidate": "second"})
    assert fake_gcs.put_conditions[-1] == (store._state_key, first_generation)


def test_snapshot_conflict_requires_a_committed_winner(fake_gcs, tmp_path):
    store = _gcs_artifact_store()
    source = _write_source(tmp_path / "source")
    manifest = _manifest_result(source).manifest
    fake_gcs.fail_conditional_writes_with = 412
    with pytest.raises(BundleManifestError, match="manifest disappeared"):
        store.publish_snapshot(manifest=manifest, source_root=source, validate_existing=lambda _: None)
    assert not store.snapshot_exists(manifest["version"])


@mock.patch.object(FakeStoreGCSBlob, "open", autospec=True)
def test_snapshot_read_permission_failure_is_not_a_missing_snapshot(open_blob, fake_gcs, tmp_path):
    store = _gcs_artifact_store()
    source = _write_source(tmp_path / "source")
    manifest = _manifest_result(source).manifest
    store.publish_snapshot(manifest=manifest, source_root=source, validate_existing=lambda _: None)
    open_blob.side_effect = FakeGCSStoreError(403)
    with pytest.raises(BundleManifestError, match="Could not read"):
        store.fetch_snapshot(manifest["version"], tmp_path / "cache", structural_validator=lambda _: None)


def test_missing_bucket_during_write_is_a_configuration_error(fake_gcs):
    store = _gcs_artifact_store()
    with pytest.raises(BundleManifestNotFoundError):
        store.read_state()
    fake_gcs.missing_buckets.add(BUCKET)
    with pytest.raises(BundleManifestError, match="bucket .* does not exist"):
        store.write_state({"candidate": "new"})


def test_store_locators_use_normalized_root(fake_gcs):
    store = GCSArtifactStore(bundle_name=BUNDLE_NAME, published_root=f"gs://{BUCKET}/releases/")
    assert store.root == f"gs://{BUCKET}/releases"
    assert store.snapshots_root == f"gs://{BUCKET}/releases/versions/{BUNDLE_NAME}"
    assert store.state_locator == f"gs://{BUCKET}/releases/_state/{BUNDLE_NAME}/auto-publish.json"
