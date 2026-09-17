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
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

import structlog
from boto3.s3.transfer import S3Transfer

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.version_compat import AIRFLOW_V_3_3_PLUS, AIRFLOW_V_3_4_PLUS
from airflow.providers.common.compat.sdk import AirflowException

if AIRFLOW_V_3_3_PLUS:
    from airflow.dag_processing.bundles.base import BundleVersion


_MANIFEST_SCHEMA_VERSION = 1
_POINTER_SCHEMA_VERSION = 1
_STAGING_DIR_PREFIX = ".s3-staging-"
_COMPLETION_MARKER = ".airflow-s3-generation.json"
_RELEASE_MANIFESTS_SUFFIX = ".releases"


class S3DagBundleConfigError(AirflowException):
    """Raised when an S3 Dag bundle manifest is configured incorrectly."""


class S3DagBundleManifestError(AirflowException):
    """Raised when an S3 Dag bundle manifest cannot be loaded or validated."""


class S3DagBundleIntegrityError(AirflowException):
    """Raised when a local S3 Dag bundle generation fails integrity validation."""


@dataclass(frozen=True)
class _ManifestObject:
    key: str
    relative_path: PurePosixPath
    version_id: str
    size: int
    sha256: str

    def as_dict(self) -> dict[str, str | int]:
        return {
            "key": self.key,
            "sha256": self.sha256,
            "size": self.size,
            "version_id": self.version_id,
        }


@dataclass(frozen=True)
class _Manifest:
    bucket_name: str
    prefix: str
    objects: tuple[_ManifestObject, ...]

    @property
    def canonical_data(self) -> dict[str, Any]:
        return {
            "bucket_name": self.bucket_name,
            "objects": [obj.as_dict() for obj in self.objects],
            "prefix": self.prefix,
            "schema_version": _MANIFEST_SCHEMA_VERSION,
        }

    @property
    def version(self) -> str:
        payload = json.dumps(
            self.canonical_data,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class _PublishedGeneration:
    path: Path
    bundle_version: BundleVersion


class S3DagBundle(BaseDagBundle):
    """
    S3 Dag bundle - exposes a directory in S3 as a Dag bundle.

    This allows Airflow to load Dags directly from an S3 bucket. By default, the bundle synchronizes the
    latest objects under ``prefix`` into one local directory. Supplying ``manifest_key`` enables versioning
    and atomic, last-known-good publication. In that mode, the publisher uploads versioned objects, an
    immutable content-addressed release manifest, and finally a small current-version pointer.

    :param aws_conn_id: Airflow connection ID for AWS. Defaults to AwsBaseHook.default_conn_name.
    :param bucket_name: The name of the S3 bucket containing the Dag files.
    :param prefix: Optional prefix within the S3 bucket where the Dags are stored.
    :param manifest_key: Optional S3 key for a publisher-managed deployment manifest. This requires Airflow
        3.4 or later and S3 bucket versioning. The configured object is the current-version pointer;
        immutable release manifests live below ``<manifest_key>.releases/`` and contain the exact S3
        VersionId, size, and SHA-256 digest for every object in the bundle.
    :param requester_pays: Whether requests to the S3 bucket should include ``RequestPayer="requester"``.
    """

    supports_versioning = False

    def __init__(
        self,
        *,
        aws_conn_id: str = AwsBaseHook.default_conn_name,
        bucket_name: str,
        prefix: str = "",
        manifest_key: str | None = None,
        requester_pays: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if manifest_key is not None and not AIRFLOW_V_3_4_PLUS:
            raise S3DagBundleConfigError("S3 Dag bundle manifests require Airflow 3.4 or later")
        if manifest_key is not None and (
            not manifest_key
            or manifest_key.startswith("/")
            or manifest_key.lower().startswith("s3://")
            or manifest_key.endswith("/")
            or "\\" in manifest_key
            or "\0" in manifest_key
            or any(part in {"", ".", ".."} for part in manifest_key.split("/"))
        ):
            raise S3DagBundleConfigError("manifest_key must identify an S3 object")
        if manifest_key is not None:
            longest_release_key = f"{manifest_key}{_RELEASE_MANIFESTS_SUFFIX}/{'0' * 64}.json"
            if len(longest_release_key.encode()) > 1024:
                raise S3DagBundleConfigError("manifest_key is too long for derived S3 release keys")
            if self.version is not None and (
                not isinstance(self.version, str) or re.fullmatch(r"[0-9a-f]{64}", self.version) is None
            ):
                raise S3DagBundleConfigError("S3 Dag bundle version must be a lowercase SHA-256 digest")
            normalized_prefix = prefix.rstrip("/")
            if (
                prefix.startswith("/")
                or prefix.endswith("//")
                or "\\" in prefix
                or "\0" in prefix
                or any(part in {"", ".", ".."} for part in normalized_prefix.split("/"))
            ) and prefix != "":
                raise S3DagBundleConfigError("prefix must be a safe S3 key prefix in manifest mode")

        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.manifest_key = manifest_key
        self.requester_pays = requester_pays
        # This is intentionally an instance attribute. Manifest mode is opt-in and must not change legacy
        # S3DagBundle instances in the same process.
        self.supports_versioning = manifest_key is not None
        self.refreshes_to_versioned_paths = self.supports_versioning
        self._s3_hook: S3Hook | None = None
        self._published_generation: _PublishedGeneration | None = None

        if self.version and self.supports_versioning:
            self.s3_dags_dir = self.versions_dir / self.version
        else:
            # Keep mutable legacy synchronization outside ``versions`` so file discovery and stale deletion
            # can never traverse immutable generations.
            self.s3_dags_dir = self.base_dir / "tracking"

        log = structlog.get_logger(__name__)
        self._log = log.bind(
            bundle_name=self.name,
            version=self.version,
            bucket_name=self.bucket_name,
            prefix=self.prefix,
            manifest_key=self.manifest_key,
            aws_conn_id=self.aws_conn_id,
            requester_pays=self.requester_pays,
        )

    def _initialize(self) -> None:
        with self.lock():
            if self.supports_versioning:
                self.versions_dir.mkdir(parents=True, exist_ok=True)
                if self.version and self._publish_existing_generation(self.version):
                    return
            else:
                self.s3_dags_dir.mkdir(parents=True, exist_ok=True)
                if not self.s3_dags_dir.is_dir():
                    raise S3DagBundleConfigError(f"Local Dags path: {self.s3_dags_dir} is not a directory.")

            if not self.s3_hook.check_for_bucket(bucket_name=self.bucket_name):
                raise S3DagBundleConfigError(f"S3 bucket '{self.bucket_name}' does not exist.")

            if not self.supports_versioning:
                if self.prefix and not self.s3_hook.check_for_prefix(
                    bucket_name=self.bucket_name, prefix=self.prefix, delimiter="/"
                ):
                    raise S3DagBundleConfigError(
                        f"S3 prefix 's3://{self.bucket_name}/{self.prefix}' does not exist."
                    )
            self.refresh()

    def initialize(self) -> None:
        self._initialize()
        super().initialize()

    @property
    def s3_hook(self) -> S3Hook:
        if self._s3_hook is None:
            self._s3_hook = S3Hook(
                aws_conn_id=self.aws_conn_id,
                requester_pays=self.requester_pays,
            )
        return self._s3_hook

    def __repr__(self) -> str:
        return (
            f"<S3DagBundle("
            f"name={self.name!r}, "
            f"bucket_name={self.bucket_name!r}, "
            f"prefix={self.prefix!r}, "
            f"manifest_key={self.manifest_key!r}, "
            f"requester_pays={self.requester_pays!r}, "
            f"version={self.version!r}"
            f")>"
        )

    def _requester_pays_args(self) -> dict[str, str]:
        if self.requester_pays:
            return {"RequestPayer": "requester"}
        return {}

    def _download_extra_args(self) -> dict[str, Any]:
        return {
            name: value
            for name, value in self.s3_hook.extra_args.items()
            if name in S3Transfer.ALLOWED_DOWNLOAD_ARGS and name not in {"RequestPayer", "VersionId"}
        }

    def _read_json_object(self, key: str) -> Any:
        if self.manifest_key is None:
            raise S3DagBundleConfigError("S3 Dag bundle manifest mode is not enabled")

        request: dict[str, Any] = {
            "Bucket": self.bucket_name,
            "Key": key,
            **self._download_extra_args(),
            **self._requester_pays_args(),
        }

        try:
            response = self.s3_hook.get_conn().get_object(**request)
            body = response["Body"]
            try:
                payload = body.read()
            finally:
                body.close()
            manifest_data = json.loads(payload)
        except Exception as e:
            raise S3DagBundleManifestError(f"Could not read S3 Dag bundle metadata {key!r}") from e

        return manifest_data

    def _release_manifest_key(self, version: str) -> str:
        if self.manifest_key is None:
            raise S3DagBundleConfigError("S3 Dag bundle manifest mode is not enabled")
        return f"{self.manifest_key}{_RELEASE_MANIFESTS_SUFFIX}/{version}.json"

    def _read_current_pointer(self) -> str:
        if self.manifest_key is None:
            raise S3DagBundleConfigError("S3 Dag bundle manifest mode is not enabled")
        data = self._read_json_object(self.manifest_key)
        if (
            not isinstance(data, dict)
            or set(data) != {"schema_version", "bundle_version"}
            or type(data.get("schema_version")) is not int
            or data.get("schema_version") != _POINTER_SCHEMA_VERSION
        ):
            raise S3DagBundleManifestError(
                f"S3 Dag bundle pointer must use schema_version {_POINTER_SCHEMA_VERSION}"
            )
        version = data.get("bundle_version")
        if not isinstance(version, str) or re.fullmatch(r"[0-9a-f]{64}", version) is None:
            raise S3DagBundleManifestError("S3 Dag bundle pointer has an invalid version")
        return version

    def _read_release_manifest(self, version: str) -> _Manifest:
        manifest = self._parse_manifest(self._read_json_object(self._release_manifest_key(version)))
        if manifest.version != version:
            raise S3DagBundleManifestError(
                f"S3 Dag bundle release manifest content hash {manifest.version!r} "
                f"does not match requested version {version!r}"
            )
        return manifest

    def _parse_manifest(self, data: Any) -> _Manifest:
        if (
            not isinstance(data, dict)
            or set(data) != {"schema_version", "bucket_name", "prefix", "objects"}
            or type(data.get("schema_version")) is not int
            or data.get("schema_version") != _MANIFEST_SCHEMA_VERSION
        ):
            raise S3DagBundleManifestError(
                f"S3 Dag bundle manifest must use schema_version {_MANIFEST_SCHEMA_VERSION}"
            )
        prefix = self.prefix.rstrip("/")
        if data.get("bucket_name") != self.bucket_name or data.get("prefix") != prefix:
            raise S3DagBundleManifestError(
                "S3 Dag bundle release manifest bucket_name and prefix must match bundle configuration"
            )
        raw_objects = data.get("objects")
        if not isinstance(raw_objects, list):
            raise S3DagBundleManifestError("S3 Dag bundle manifest objects must be a list")

        expected_key_prefix = f"{prefix}/" if prefix else ""
        parsed_objects: list[_ManifestObject] = []
        relative_paths: set[str] = set()

        for index, raw_object in enumerate(raw_objects):
            if not isinstance(raw_object, dict):
                raise S3DagBundleManifestError(f"S3 Dag bundle manifest object {index} must be an object")
            if set(raw_object) != {"key", "version_id", "size", "sha256"}:
                raise S3DagBundleManifestError(f"S3 Dag bundle manifest object {index} has invalid fields")
            key = raw_object.get("key")
            version_id = raw_object.get("version_id")
            size = raw_object.get("size")
            sha256 = raw_object.get("sha256")
            if not isinstance(key, str) or not key or key.endswith("/"):
                raise S3DagBundleManifestError(f"S3 Dag bundle manifest object {index} has an invalid key")
            if self.manifest_key is not None and (
                key == self.manifest_key or key.startswith(f"{self.manifest_key}{_RELEASE_MANIFESTS_SUFFIX}/")
            ):
                raise S3DagBundleManifestError("S3 Dag bundle manifest must not include bundle metadata")
            if expected_key_prefix and not key.startswith(expected_key_prefix):
                raise S3DagBundleManifestError(
                    f"S3 Dag bundle manifest key {key!r} is outside configured prefix {self.prefix!r}"
                )

            relative_key = key[len(expected_key_prefix) :] if expected_key_prefix else key
            path_parts = relative_key.split("/")
            if (
                any(part in {"", ".", ".."} for part in path_parts)
                or "\\" in relative_key
                or "\0" in relative_key
            ):
                raise S3DagBundleManifestError(
                    f"S3 Dag bundle manifest key {key!r} is not a safe relative path"
                )
            relative_path = PurePosixPath(*path_parts)
            normalized_relative_path = relative_path.as_posix()
            if normalized_relative_path == _COMPLETION_MARKER or normalized_relative_path.startswith(
                f"{_COMPLETION_MARKER}/"
            ):
                raise S3DagBundleManifestError(
                    f"S3 Dag bundle manifest path {_COMPLETION_MARKER!r} and its subtree are reserved"
                )
            if normalized_relative_path in relative_paths:
                raise S3DagBundleManifestError(
                    f"S3 Dag bundle manifest contains duplicate path {normalized_relative_path!r}"
                )
            if not isinstance(version_id, str) or not version_id or version_id == "null":
                raise S3DagBundleManifestError(
                    f"S3 Dag bundle manifest object {key!r} has an invalid version_id"
                )
            if not isinstance(size, int) or isinstance(size, bool) or size < 0:
                raise S3DagBundleManifestError(f"S3 Dag bundle manifest object {key!r} has an invalid size")
            if not isinstance(sha256, str) or re.fullmatch(r"[0-9a-f]{64}", sha256) is None:
                raise S3DagBundleManifestError(f"S3 Dag bundle manifest object {key!r} has an invalid sha256")

            relative_paths.add(normalized_relative_path)
            parsed_objects.append(
                _ManifestObject(
                    key=key,
                    relative_path=relative_path,
                    version_id=version_id,
                    size=size,
                    sha256=sha256,
                )
            )

        for relative_path in relative_paths:
            parts = relative_path.split("/")
            for index in range(1, len(parts)):
                parent = "/".join(parts[:index])
                if parent in relative_paths:
                    raise S3DagBundleManifestError(
                        f"S3 Dag bundle manifest path {relative_path!r} conflicts with file {parent!r}"
                    )

        return _Manifest(
            bucket_name=self.bucket_name,
            prefix=prefix,
            objects=tuple(sorted(parsed_objects, key=lambda obj: obj.key)),
        )

    @staticmethod
    def _bundle_version(manifest: _Manifest) -> BundleVersion:
        return BundleVersion(version=manifest.version)

    def get_current_version(self) -> str | BundleVersion | None:
        """Return the locally published manifest version, or ``None`` for legacy mode."""
        if not self.supports_versioning:
            return None
        if self._published_generation is not None:
            return self._published_generation.bundle_version

        version = self.version or self._read_current_pointer()
        return BundleVersion(version=version)

    @property
    def path(self) -> Path:
        """Return the local path to the Dag files."""
        if self._published_generation is not None:
            return self._published_generation.path
        return self.s3_dags_dir

    def _remove_orphaned_staging_dirs(self) -> None:
        for stage_path in self.versions_dir.glob(f"{_STAGING_DIR_PREFIX}*"):
            if stage_path.is_dir() and not stage_path.is_symlink():
                self._log.warning("Removing incomplete S3 Dag bundle staging directory", path=stage_path)
                shutil.rmtree(stage_path)

    @staticmethod
    def _generation_is_published(path: Path, version: str) -> bool:
        # Reuse is intentionally O(1): first publication verifies every object before an atomic rename.
        # Airflow treats its private bundle storage as trusted, as Git bundle worktrees do; synchronous
        # rehashing here would read the whole bundle during every task and callback startup.
        if path.is_symlink():
            return False
        marker_path = path / _COMPLETION_MARKER
        if marker_path.is_symlink():
            return False
        try:
            marker = json.loads(marker_path.read_text())
        except (OSError, ValueError, TypeError):
            return False
        return (
            isinstance(marker, dict)
            and set(marker) == {"schema_version", "bundle_version"}
            and type(marker.get("schema_version")) is int
            and marker["schema_version"] == 1
            and marker["bundle_version"] == version
        )

    def _get_published_generation_path(self, version: str) -> Path | None:
        path = self.versions_dir / version
        if not path.exists() and not path.is_symlink():
            return None
        if not path.is_dir() or not self._generation_is_published(path, version):
            raise S3DagBundleIntegrityError(
                f"Existing S3 Dag bundle generation {path} is incomplete or corrupted"
            )
        return path

    def _publish_existing_generation(self, version: str) -> bool:
        if not (published_path := self._get_published_generation_path(version)):
            return False
        self._published_generation = _PublishedGeneration(
            path=published_path,
            bundle_version=BundleVersion(version=version),
        )
        return True

    @staticmethod
    def _file_sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _download_generation(self, manifest: _Manifest, target: Path) -> None:
        client = self.s3_hook.get_conn()
        requester_pays_args = self._requester_pays_args()
        hook_extra_args = self._download_extra_args()
        for obj in manifest.objects:
            local_path = target.joinpath(*obj.relative_path.parts)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            extra_args = {**hook_extra_args, "VersionId": obj.version_id, **requester_pays_args}
            try:
                client.download_file(
                    Bucket=self.bucket_name,
                    Key=obj.key,
                    Filename=os.fspath(local_path),
                    ExtraArgs=extra_args,
                    Config=self.s3_hook.transfer_config,
                )
            except Exception as e:
                raise S3DagBundleIntegrityError(
                    f"Could not download S3 object {obj.key!r} at version {obj.version_id!r}"
                ) from e
            actual_size = local_path.stat().st_size
            if actual_size != obj.size:
                raise S3DagBundleIntegrityError(
                    f"Downloaded S3 object {obj.key!r} at version {obj.version_id!r} has size "
                    f"{actual_size}, expected {obj.size}"
                )
            actual_sha256 = self._file_sha256(local_path)
            if actual_sha256 != obj.sha256:
                raise S3DagBundleIntegrityError(
                    f"Downloaded S3 object {obj.key!r} at version {obj.version_id!r} has SHA-256 "
                    f"{actual_sha256}, expected {obj.sha256}"
                )

    def _materialize_generation(self, manifest: _Manifest, version: str) -> Path:
        if published_path := self._get_published_generation_path(version):
            return published_path
        final_path = self.versions_dir / version

        staging_path = Path(tempfile.mkdtemp(prefix=_STAGING_DIR_PREFIX, dir=self.versions_dir))
        try:
            self._download_generation(manifest, staging_path)
            marker_path = staging_path / _COMPLETION_MARKER
            marker_path.write_text(json.dumps({"schema_version": 1, "bundle_version": version}))
            for obj in manifest.objects:
                staging_path.joinpath(*obj.relative_path.parts).chmod(0o444)
            marker_path.chmod(0o444)
            try:
                staging_path.rename(final_path)
            except FileExistsError:
                if not final_path.is_dir() or not self._generation_is_published(final_path, version):
                    raise S3DagBundleIntegrityError(
                        f"Concurrent S3 Dag bundle generation {final_path} is incomplete or corrupted"
                    ) from None
            return final_path
        finally:
            if staging_path.exists():
                shutil.rmtree(staging_path)

    def _refresh_versioned(self) -> None:
        self._remove_orphaned_staging_dirs()
        version = self.version or self._read_current_pointer()
        if self._publish_existing_generation(version):
            return
        manifest = self._read_release_manifest(version)
        bundle_version = self._bundle_version(manifest)

        generation_path = self._materialize_generation(manifest, bundle_version.version)
        # Publish instance state last. A failed download or validation therefore leaves both the visible path
        # and get_current_version() pinned to the previous complete generation.
        self._published_generation = _PublishedGeneration(
            path=generation_path,
            bundle_version=bundle_version,
        )

    def refresh(self) -> None:
        """Refresh the Dag bundle from S3."""
        with self.lock():
            if self.supports_versioning:
                self._refresh_versioned()
                return
            if self.version:
                raise S3DagBundleConfigError("Refreshing a specific version is not supported")

            self._log.debug(
                "Downloading Dags from s3://%s/%s to %s",
                self.bucket_name,
                self.prefix,
                self.s3_dags_dir,
            )
            self.s3_hook.sync_to_local_dir(
                bucket_name=self.bucket_name,
                s3_prefix=self.prefix,
                local_dir=self.s3_dags_dir,
                delete_stale=True,
            )

    def view_url(self, version: str | None = None) -> str | None:
        """
        Return a URL for viewing the Dags in S3.

        This method is deprecated and will be removed when the minimum supported Airflow version is 3.1.
        Use ``view_url_template`` instead.
        """
        return self.view_url_template()

    def view_url_template(self) -> str | None:
        """Return a URL for viewing the Dags in S3."""
        if self.version and not self.supports_versioning:
            raise S3DagBundleConfigError("S3 url with version is not supported")
        if hasattr(self, "_view_url_template") and self._view_url_template:
            # Because we use this method in the view_url method, we need to handle backward compatibility for
            # Airflow versions that don't have the _view_url_template attribute. Remove with Airflow 3.0 support.
            return self._view_url_template
        url = f"https://{self.bucket_name}.s3"
        if self.s3_hook.region_name:
            url += f".{self.s3_hook.region_name}"
        url += ".amazonaws.com"
        if self.prefix:
            url += f"/{self.prefix}"

        return url
