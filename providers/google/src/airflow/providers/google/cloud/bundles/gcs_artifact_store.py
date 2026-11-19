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

"""
GCS artifact store: publish and consume bundle artifacts in Google Cloud Storage.

A GCS ``published_root`` (``gs://bucket/prefix``) holds the same logical layout as a
filesystem one — ``versions/<bundle>/<version>/``, ``refs/<bundle>/latest.json``,
``_state/<bundle>/auto-publish.json``:

- The embedded snapshot manifest object is written **last** during publication, so
  its presence is the commit marker. A version prefix without its manifest is
  invisible, and re-publishing the same content completes it.
- There is no cross-host lock. The two mutable documents are protected by
  generation-match preconditions (``if_generation_match``; ``0`` asserts absence); a
  lost race surfaces as ``ArtifactStoreConflictError`` unless the winner wrote
  identical bytes, which is an idempotent success. Snapshot uploads need no
  protection: content-addressed keys make concurrent same-content publications write
  identical objects.

"""

from __future__ import annotations

import hashlib
import json
import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

try:
    from airflow.dag_processing.bundles.artifact_store import (
        ArtifactStore,
        ArtifactStoreConflictError,
        BundleManifestNotFoundError,
        BundleManifestRefDurabilityError,
        PublishedSnapshot,
    )
    from airflow.dag_processing.bundles.manifest import (
        MANIFEST_FILE_NAME,
        BundleManifestError,
        BundleManifestSourceChangedError,
        serialize_bundle_version_manifest,
        validate_bundle_relative_path,
        validate_bundle_version,
        validate_bundle_version_manifest_structure,
    )
except ImportError as e:
    raise AirflowOptionalProviderFeatureException(
        "Manifest GCS bundles require Apache Airflow 3.5 or later"
    ) from e


if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping
    from pathlib import Path

from google.cloud.storage.retry import DEFAULT_RETRY

from airflow.providers.google.cloud.hooks.gcs import GCSHook

log = logging.getLogger(__name__)

AUTO_PUBLISH_STATE_FILE_NAME = "auto-publish.json"

GCS_PUBLISHED_ROOT_SCHEME = "gs://"
#: The library-default public endpoint. It is normalized to ``None`` wherever an
#: endpoint identity is compared, so default deployments never depend on how a
#: google-cloud-storage release spells its internal endpoint attributes.
GCS_PUBLIC_API_ENDPOINT = "https://storage.googleapis.com"

#: CAS baseline recording that the document did not exist when last read.
_MISSING = object()


def _get_gcs_error_code(error: Exception) -> int | None:
    code = getattr(error, "code", None)
    if callable(code):
        code = code()
    try:
        return int(code)
    except (TypeError, ValueError):
        return None


def _is_missing_gcs_error(error: Exception) -> bool:
    return _get_gcs_error_code(error) == 404 or type(error).__name__ == "NotFound"


def _is_gcs_precondition_error(error: Exception) -> bool:
    return _get_gcs_error_code(error) == 412 or type(error).__name__ in {
        "FailedPrecondition",
        "PreconditionFailed",
    }


def _is_missing_gcs_bucket_error(error: Exception) -> bool:
    # GCS reports a missing bucket and a missing object with the same 404; the
    # message is the only discriminator the API offers, so this is best effort.
    # An unrecognized missing-bucket message is treated as a missing object on reads.
    return _is_missing_gcs_error(error) and "bucket does not exist" in str(error).lower()


def get_gcs_api_endpoint(client: Any) -> str | None:
    """
    Best-effort endpoint identity of a google-cloud-storage client.

    The attributes are library internals, so the default endpoint must map to a
    stable value: a missing attribute and the public endpoint both become ``None``;
    only a configured custom endpoint stays visible. Trailing slashes are stripped
    so a library spelling change cannot alter the identity of a custom endpoint.
    """
    connection = getattr(client, "_connection", None)
    endpoint = getattr(connection, "API_BASE_URL", None)
    if not isinstance(endpoint, str):
        client_options = getattr(client, "_client_options", None)
        endpoint = getattr(client_options, "api_endpoint", None)
    if not isinstance(endpoint, str) or not endpoint:
        return None
    endpoint = endpoint.rstrip("/")
    if endpoint == GCS_PUBLIC_API_ENDPOINT:
        return None
    return endpoint


def parse_gcs_published_root(published_root: str) -> tuple[str, str]:
    """Split ``gs://bucket[/prefix]`` into (bucket, normalized prefix without slashes)."""
    if not published_root[: len(GCS_PUBLISHED_ROOT_SCHEME)].lower() == GCS_PUBLISHED_ROOT_SCHEME:
        raise TypeError(f"published_root {published_root!r} is not a gs:// URL")
    remainder = published_root[len(GCS_PUBLISHED_ROOT_SCHEME) :]
    bucket, _, prefix = remainder.partition("/")
    if not bucket:
        raise TypeError(f"published_root {published_root!r} does not contain a bucket name")
    if "?" in remainder or "#" in remainder:
        raise TypeError(f"published_root {published_root!r} must not contain a query or fragment")
    return bucket, prefix.strip("/")


class GCSArtifactStore(ArtifactStore):
    """Object-store artifact backend: CAS-coordinated publication, manifest-last commits."""

    backend_type = "gcs"

    def __init__(
        self,
        *,
        bundle_name: str,
        published_root: str,
        gcp_conn_id: str | None = None,
    ) -> None:
        if gcp_conn_id is not None and (not isinstance(gcp_conn_id, str) or not gcp_conn_id):
            raise TypeError("published_root_conn_id must be a non-empty string")
        self.bundle_name = bundle_name
        self.bucket_name, self.prefix = parse_gcs_published_root(published_root)
        self.gcp_conn_id = gcp_conn_id if gcp_conn_id is not None else GCSHook.default_conn_name
        self._gcs_hook: GCSHook | None = None
        # CAS baselines for the mutable documents: key -> generation from the last
        # read or write, or _MISSING when the last read found no document. Absent
        # means no baseline; the next write re-reads to establish one.
        self._document_tokens: dict[str, Any] = {}
        self.copy_hints: Mapping[str, dict[str, Any]] | None = None

    def _build_key(self, *parts: str) -> str:
        segments = [self.prefix, *parts] if self.prefix else list(parts)
        return "/".join(segments)

    def _build_url(self, key: str) -> str:
        return f"{GCS_PUBLISHED_ROOT_SCHEME}{self.bucket_name}/{key}" if key else self.root

    @property
    def root(self) -> str:
        suffix = f"/{self.prefix}" if self.prefix else ""
        return f"{GCS_PUBLISHED_ROOT_SCHEME}{self.bucket_name}{suffix}"

    @property
    def ref_locator(self) -> str:
        return self._build_url(self._ref_key)

    @property
    def state_locator(self) -> str:
        return self._build_url(self._state_key)

    @property
    def snapshots_root(self) -> str:
        return self._build_url(self._build_key("versions", self.bundle_name))

    def snapshot_locator(self, version: str) -> str:
        return self._build_url(self._build_snapshot_key(version))

    @property
    def _ref_key(self) -> str:
        return self._build_key("refs", self.bundle_name, "latest.json")

    @property
    def _state_key(self) -> str:
        return self._build_key("_state", self.bundle_name, AUTO_PUBLISH_STATE_FILE_NAME)

    def _build_snapshot_key(self, version: str, *parts: str) -> str:
        return self._build_key("versions", self.bundle_name, version, *parts)

    @property
    def gcs_hook(self) -> GCSHook:
        if self._gcs_hook is None:
            try:
                self._gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
            except Exception as e:
                raise BundleManifestError(
                    f"Could not create a GCS client for published_root {self.root}"
                ) from e
        return self._gcs_hook

    def _get_client(self) -> Any:
        try:
            return self.gcs_hook.get_conn()
        except BundleManifestError:
            raise
        except Exception as e:
            raise BundleManifestError(f"Could not create a GCS client for published_root {self.root}") from e

    def _get_blob(self, key: str, *, generation: int | None = None) -> Any:
        return self._get_client().bucket(self.bucket_name).blob(key, generation=generation)

    def _get_object(self, key: str) -> tuple[bytes, int] | None:
        """Return (body, generation) for the object, or None when it does not exist."""
        blob = self._get_blob(key)
        try:
            data = blob.download_as_bytes()
        except Exception as e:
            if _is_missing_gcs_error(e) and not _is_missing_gcs_bucket_error(e):
                return None
            raise self._build_read_error(key, e) from e
        # The download response carries the served object's generation; the pair is
        # therefore consistent even under concurrent replacement.
        generation = getattr(blob, "generation", None)
        if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
            raise BundleManifestError(f"GCS did not return the generation of {self._build_url(key)}")
        return data, generation

    def _get_object_bytes(self, key: str) -> bytes | None:
        result = self._get_object(key)
        return None if result is None else result[0]

    def _object_exists(self, key: str) -> bool:
        # reload(), not exists(): the library's exists() also reports a missing
        # bucket as False, which would read as "not published" instead of the
        # configuration error below.
        blob = self._get_blob(key)
        try:
            blob.reload()
        except Exception as e:
            if _is_missing_gcs_error(e) and not _is_missing_gcs_bucket_error(e):
                return False
            raise self._build_read_error(key, e) from e
        return True

    def _build_read_error(self, key: str, error: Exception) -> BundleManifestError:
        if _is_missing_gcs_bucket_error(error):
            return BundleManifestError(
                f"GCS bucket {self.bucket_name!r} for published_root {self.root} does not "
                "exist. Fix the published_root configuration before refreshing this bundle."
            )
        return BundleManifestError(
            f"Could not read {self._build_url(key)} from the object-store published_root"
        )

    def _read_json_object(self, key: str, *, missing_message: str, invalid_message: str) -> dict[str, Any]:
        result = self._get_object(key)
        if result is None:
            self._document_tokens[key] = _MISSING
            raise BundleManifestNotFoundError(missing_message)
        data, generation = result
        # Record the CAS baseline before parsing: a later write may deliberately
        # replace a corrupt document.
        self._record_document_token(key, generation)
        try:
            payload = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise BundleManifestError(invalid_message) from e
        if not isinstance(payload, dict):
            raise BundleManifestError(invalid_message)
        return payload

    def _record_document_token(self, key: str, generation: int | None) -> None:
        if generation:
            self._document_tokens[key] = generation
        else:
            self._document_tokens.pop(key, None)

    def read_ref(self) -> dict[str, Any]:
        missing_message = f"Release reference {self.ref_locator} is not published"
        invalid_message = f"Release reference {self.ref_locator} is not a JSON object"
        return self._read_json_object(
            self._ref_key, missing_message=missing_message, invalid_message=invalid_message
        )

    def read_state(self) -> dict[str, Any]:
        missing_message = f"Automatic-publication state {self.state_locator} is not published"
        invalid_message = f"Automatic-publication state {self.state_locator} is not a JSON object"
        return self._read_json_object(
            self._state_key, missing_message=missing_message, invalid_message=invalid_message
        )

    def write_ref(self, payload: dict[str, Any]) -> None:
        self._write_json_document(self._ref_key, payload)

    def write_state(self, payload: dict[str, Any]) -> None:
        self._write_json_document(self._state_key, payload)

    def _write_json_document(self, key: str, payload: dict[str, Any]) -> None:
        token = self._document_tokens.get(key)
        if token is None:
            # No baseline from this session; establish one so the write stays CAS.
            result = self._get_object(key)
            token = result[1] if result is not None and result[1] else _MISSING
            self._document_tokens[key] = token
        body = serialize_bundle_version_manifest(payload)
        expected_generation = 0 if token is _MISSING else token
        blob = self._get_blob(key)
        try:
            blob.upload_from_string(
                body,
                content_type="application/json",
                if_generation_match=expected_generation,
            )
        except Exception as e:
            if _is_gcs_precondition_error(e):
                current = self._get_object(key)
                if current is None:
                    self._document_tokens[key] = _MISSING
                else:
                    self._record_document_token(key, current[1])
                    if current[0] == body:
                        # A concurrent publisher wrote identical bytes: idempotent win.
                        return
                raise ArtifactStoreConflictError(
                    f"Another publisher updated {self._build_url(key)} concurrently"
                ) from e
            code = _get_gcs_error_code(e)
            if key == self._ref_key and (code is None or code == 408 or code >= 500):
                raise BundleManifestRefDurabilityError(
                    ref_locator=self.ref_locator, version=payload.get("version")
                ) from e
            raise self._build_write_error(key, e) from e
        generation = getattr(blob, "generation", None)
        self._record_document_token(key, generation if isinstance(generation, int) else None)

    @contextmanager
    def publication_guard(self, *, wait: bool = True) -> Iterator[None]:
        self._document_tokens.clear()
        yield

    def prepare_publish_areas(self) -> None:
        """Nothing to create: object-store prefixes appear with their first object."""

    def snapshot_exists(self, version: str) -> bool:
        # The embedded manifest is written last during publication, so its presence is
        # the commit marker; a version prefix without it is an incomplete upload.
        validate_bundle_version(version, source="snapshot version")
        return self._object_exists(self._build_snapshot_key(version, MANIFEST_FILE_NAME))

    def fetch_snapshot(
        self,
        version: str,
        destination: Path,
        *,
        structural_validator: Callable[[Path], None],
    ) -> None:
        # There is no remote tree to structurally pre-validate; the manifest is the
        # authority, every downloaded byte is hash-verified below, and the caller runs
        # the full validation pass on the materialized copy afterwards.
        validate_bundle_version(version, source="snapshot version")
        if destination.is_symlink() or (
            destination.exists() and (not destination.is_dir() or any(destination.iterdir()))
        ):
            raise ValueError("Snapshot destination must be absent or empty")
        manifest_bytes = self._get_object_bytes(self._build_snapshot_key(version, MANIFEST_FILE_NAME))
        if manifest_bytes is None:
            raise BundleManifestNotFoundError(
                f"Bundle '{self.bundle_name}' version '{version}' is not published at "
                f"{self.snapshot_locator(version)}. Publish or restore the immutable snapshot before "
                "running pinned work."
            )
        try:
            manifest = json.loads(manifest_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise BundleManifestError(
                f"Bundle snapshot manifest is not valid JSON: "
                f"{self._build_url(self._build_snapshot_key(version, MANIFEST_FILE_NAME))}"
            ) from e
        validate_bundle_version_manifest_structure(
            manifest,
            expected_version=version,
            expected_bundle_name=self.bundle_name,
            expected_backend_type=self.backend_type,
        )
        destination.mkdir(parents=True, exist_ok=True)
        (destination / MANIFEST_FILE_NAME).write_bytes(manifest_bytes)
        for file_info in manifest["files"]:
            # Path safety must hold before anything is written to disk; the full
            # manifest/ref consistency checks run in the caller's validation pass.
            relative_path = validate_bundle_relative_path(file_info["path"]).as_posix()
            file_destination = destination / relative_path
            file_destination.parent.mkdir(parents=True, exist_ok=True)
            self._download_snapshot_file(
                key=self._build_snapshot_key(version, *relative_path.split("/")),
                destination=file_destination,
                file_info=file_info,
            )

    def _download_snapshot_file(self, *, key: str, destination: Path, file_info: dict[str, Any]) -> None:
        blob = self._get_blob(key)
        digest = hashlib.sha256()
        size = 0
        try:
            blob.reload()
            with destination.open("wb") as file, blob.open("rb", raw_download=True) as reader:
                for chunk in iter(lambda: reader.read(1024 * 1024), b""):
                    file.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
        except Exception as e:
            if _is_missing_gcs_error(e) and not _is_missing_gcs_bucket_error(e):
                raise BundleManifestError(
                    f"Bundle snapshot at {self._build_url(key)} is missing manifest entry "
                    f"{file_info['path']!r}"
                ) from e
            raise self._build_read_error(key, e) from e
        if size != file_info["size"] or digest.hexdigest() != file_info["sha256"]:
            raise BundleManifestError(
                f"Bundle snapshot file {file_info['path']!r} does not match the snapshot manifest"
            )
        destination.chmod(0o755 if file_info["executable"] else 0o644)

    def publish_snapshot(
        self,
        *,
        manifest: dict[str, Any],
        source_root: Path,
        validate_existing: Callable[[Path], None],
    ) -> PublishedSnapshot:
        version = manifest["version"]
        validate_bundle_version_manifest_structure(
            manifest,
            expected_version=version,
            expected_bundle_name=self.bundle_name,
            expected_backend_type=self.backend_type,
        )
        copy_hints = self.copy_hints
        # The committed manifest may carry different source generations for identical
        # content. Keep it and derive the release reference from the returned manifest.
        manifest_bytes = serialize_bundle_version_manifest(manifest)
        manifest_key = self._build_snapshot_key(version, MANIFEST_FILE_NAME)
        existing = self._get_object_bytes(manifest_key)
        if existing is not None:
            return PublishedSnapshot(manifest=self._parse_snapshot_manifest(existing, version), created=False)

        # One failed rewrite that is not a per-object precondition miss disables
        # further attempts for this publication: a systemic cause (permissions,
        # cross-project restrictions) would otherwise fail once per file.
        copy_disabled = not copy_hints
        for file_info in manifest["files"]:
            relative_path = validate_bundle_relative_path(file_info["path"]).as_posix()
            destination_key = self._build_snapshot_key(version, *relative_path.split("/"))
            if copy_hints and not copy_disabled:
                copied, copy_disabled = self._try_server_side_rewrite(
                    hint=copy_hints.get(relative_path),
                    destination_key=destination_key,
                )
                if copied:
                    continue
            source = source_root / relative_path
            try:
                data = source.read_bytes()
            except FileNotFoundError as e:
                raise BundleManifestSourceChangedError(
                    f"Bundle source file disappeared while publishing bundle version "
                    f"{version}: {relative_path}"
                ) from e
            # Hash what is actually uploaded so source drift between manifest
            # construction and upload cannot commit a snapshot that contradicts its
            # own content address (mirrors the filesystem store's inline check).
            if len(data) != file_info["size"] or hashlib.sha256(data).hexdigest() != file_info["sha256"]:
                raise BundleManifestSourceChangedError(
                    f"Bundle source changed while publishing bundle version {version}"
                )
            self._put_object(destination_key, data)

        try:
            self._get_blob(manifest_key).upload_from_string(
                manifest_bytes, content_type="application/json", if_generation_match=0
            )
        except Exception as e:
            if not _is_gcs_precondition_error(e):
                raise self._build_write_error(manifest_key, e) from e
            winner = self._get_object_bytes(manifest_key)
            if winner is None:
                raise BundleManifestError("Committed snapshot manifest disappeared") from e
            return PublishedSnapshot(manifest=self._parse_snapshot_manifest(winner, version), created=False)
        return PublishedSnapshot(manifest=manifest, created=True)

    def _parse_snapshot_manifest(self, data: bytes, version: str) -> dict[str, Any]:
        try:
            manifest = json.loads(data)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise BundleManifestError("Snapshot manifest is not valid JSON") from e
        validate_bundle_version_manifest_structure(
            manifest,
            expected_version=version,
            expected_bundle_name=self.bundle_name,
            expected_backend_type=self.backend_type,
        )
        return manifest

    def _try_server_side_rewrite(
        self,
        *,
        hint: dict[str, Any] | None,
        destination_key: str,
    ) -> tuple[bool, bool]:
        """
        Attempt one server-side rewrite. Returns (copied, disable_further_attempts).

        The rewrite is pinned to the exact generation the manifest hashes were
        computed from: if the source moved on, the store refuses the rewrite and the
        caller uploads the prepared local bytes instead — which still match the
        manifest. A lying object store is caught at fetch time, where every file is
        hash-verified against the manifest before use.
        """
        if not isinstance(hint, dict) or hint.get("type") != "gcs":
            return False, False
        bucket = hint.get("bucket")
        name = hint.get("name")
        generation = hint.get("generation")
        if (
            not all(isinstance(value, str) and value for value in (bucket, name))
            or not isinstance(generation, int)
            or isinstance(generation, bool)
            or generation < 1
        ):
            return False, False
        client = self._get_client()
        if hint.get("endpoint") != get_gcs_api_endpoint(client):
            # Different endpoints cannot rewrite server-side; no point retrying per file.
            return False, True
        try:
            source_blob = client.bucket(bucket).blob(name, generation=generation)
            destination_blob = client.bucket(self.bucket_name).blob(destination_key)
            token, _, _ = destination_blob.rewrite(source_blob, if_source_generation_match=generation)
            while token is not None:
                token, _, _ = destination_blob.rewrite(
                    source_blob,
                    token=token,
                    if_source_generation_match=generation,
                )
        except Exception as e:
            if _is_gcs_precondition_error(e) or (
                _is_missing_gcs_error(e) and not _is_missing_gcs_bucket_error(e)
            ):
                # This one object changed or vanished since it was observed; the
                # prepared local copy still matches the manifest, so upload it and
                # keep trying rewrites for the remaining files.
                log.debug(
                    "Server-side rewrite precondition failed; uploading the prepared copy. destination=%s",
                    self._build_url(destination_key),
                )
                return False, False
            log.info(
                "Server-side rewrite failed; publishing by upload instead. destination=%s",
                self._build_url(destination_key),
                exc_info=True,
            )
            return False, True
        return True, False

    def _put_object(self, key: str, body: bytes) -> None:
        # The library does not retry unconditioned uploads by default. These
        # writes are retry-safe because snapshot objects live at content-addressed keys.
        blob = self._get_blob(key)
        try:
            blob.upload_from_string(body, retry=DEFAULT_RETRY)
        except Exception as e:
            raise self._build_write_error(key, e) from e

    def _build_write_error(self, key: str, error: Exception) -> BundleManifestError:
        if _is_missing_gcs_bucket_error(error):
            return BundleManifestError(
                f"GCS bucket {self.bucket_name!r} for published_root {self.root} does not "
                "exist. Fix the published_root configuration before publishing."
            )
        return BundleManifestError(
            f"Could not write {self._build_url(key)} to the object-store published_root"
        )


__all__ = [
    "GCS_PUBLIC_API_ENDPOINT",
    "GCS_PUBLISHED_ROOT_SCHEME",
    "GCSArtifactStore",
    "get_gcs_api_endpoint",
    "parse_gcs_published_root",
]
