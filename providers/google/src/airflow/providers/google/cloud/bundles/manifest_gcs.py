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

"""Read-only GCS source adapter for manifest-backed Dag bundles."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

try:
    from airflow.dag_processing.bundles.artifact_store import BundleManifestNotFoundError
    from airflow.dag_processing.bundles.manifest import (
        BundleManifestError,
        BundleManifestSourceChangedError,
    )
    from airflow.dag_processing.bundles.object_source import (
        DEFAULT_MAX_FILE_COUNT,
        DEFAULT_MAX_FILE_SIZE_BYTES,
        DEFAULT_MAX_TOTAL_SIZE_BYTES,
        ObjectSourceObservation,
        ObjectStoreSourceDagBundleBase,
        _get_relative_object_path,
        publish_manifest_object_store_dag_bundle,
    )
except ImportError as e:
    raise AirflowOptionalProviderFeatureException(
        "Manifest GCS bundles require Apache Airflow 3.5 or later"
    ) from e

from airflow.providers.google.cloud.bundles.gcs_artifact_store import (
    GCSArtifactStore,
    _is_gcs_precondition_error,
    _is_missing_gcs_error,
    get_gcs_api_endpoint,
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if TYPE_CHECKING:
    from airflow.dag_processing.bundles.artifact_store import ArtifactStore
    from airflow.dag_processing.bundles.manifest_bundle import BundlePublishResult

DEFAULT_GCP_CONN_ID = GoogleBaseHook.default_conn_name
GCS_SOURCE_OBSERVATION_SCHEMA_VERSION = 1
GCS_MIRROR_STATE_SCHEMA_VERSION = 1
GCS_MIRROR_STATE_FILE_NAME = "_gcs_source_state.json"


@dataclass(frozen=True)
class GCSObjectObservation:
    """One safe GCS object and the metadata that identifies its generation."""

    relative_path: str
    name: str
    size: int
    generation: int
    metageneration: int
    updated: str
    etag: str

    @property
    def remote_name(self) -> str:
        return self.name

    def build_signature_record(self) -> dict[str, Any]:
        return {
            "path": self.relative_path,
            "name": self.name,
            "size": self.size,
            "generation": self.generation,
            "metageneration": self.metageneration,
            "updated": self.updated,
            "etag": self.etag,
        }


@dataclass(frozen=True)
class GCSSourceObservation(ObjectSourceObservation):
    """One canonical observation of a GCS source folder."""


class ManifestGCSDagBundle(ObjectStoreSourceDagBundleBase):
    """
    Mirror a GCS folder into local staging and publish immutable snapshots.

    The GCS folder is the mutable source, never parsed or executed from directly.
    Releases go to ``published_root``: a ``gs://`` prefix (recommended — pinned
    execution then reads the releases prefix with the artifact store's GCS client),
    or a shared filesystem path (pinned execution then needs no GCS access at all).
    An ``s3://`` published_root is rejected: cross-cloud publication is not
    supported.

    :param bucket_name: Bucket containing the mutable Dag source.
    :param prefix: Folder prefix within the source bucket.
    :param gcp_conn_id: Connection used to read the mutable source.
    :param published_root_conn_id: Connection used for a ``gs://`` published root.
        Defaults to ``google_cloud_default`` independently of the source connection.
    :param deployment_marker_key: Relative object name removed before changing source
        files and written last after a deployment. Source changes require a new marker generation.
    :param max_file_count: Maximum number of included objects in one source observation.
    :param max_file_size_bytes: Maximum size of one included object.
    :param max_total_size_bytes: Maximum total size of all included objects.
    :param auto_publish: Publish source changes during unpinned refreshes. Disable this
        option when using :func:`publish_manifest_gcs_dag_bundle` to control releases.
    """

    _source_type = "gcs"
    _source_label = "GCS"
    _remote_name_noun = "object name"
    _source_url_scheme = "gs"
    _mirror_dir_name = "_gcs_source"
    _mirror_state_file_name = GCS_MIRROR_STATE_FILE_NAME
    _observation_schema_version = GCS_SOURCE_OBSERVATION_SCHEMA_VERSION
    _mirror_state_schema_version = GCS_MIRROR_STATE_SCHEMA_VERSION
    _observation_class = GCSSourceObservation
    _marker_key_requirement = "a non-empty relative GCS object name"
    _marker_key_safe_requirement = "a safe relative GCS object name"
    _compatible_store_backends = ("local", "gcs")

    def __init__(
        self,
        *,
        gcp_conn_id: str = DEFAULT_GCP_CONN_ID,
        published_root_conn_id: str | None = None,
        bucket_name: str,
        prefix: str = "",
        deployment_marker_key: str | None = None,
        max_file_count: int = DEFAULT_MAX_FILE_COUNT,
        max_file_size_bytes: int = DEFAULT_MAX_FILE_SIZE_BYTES,
        max_total_size_bytes: int = DEFAULT_MAX_TOTAL_SIZE_BYTES,
        auto_publish: bool = True,
        **kwargs: Any,
    ) -> None:
        if not isinstance(gcp_conn_id, str) or not gcp_conn_id:
            raise TypeError("gcp_conn_id must be a non-empty string")
        self.gcp_conn_id = gcp_conn_id
        self.published_root_conn_id = published_root_conn_id
        super().__init__(
            bucket_name=bucket_name,
            prefix=prefix,
            deployment_marker_key=deployment_marker_key,
            max_file_count=max_file_count,
            max_file_size_bytes=max_file_size_bytes,
            max_total_size_bytes=max_total_size_bytes,
            auto_publish=auto_publish,
            **kwargs,
        )
        self._gcs_hook: GCSHook | None = None
        if isinstance(self._store, GCSArtifactStore) and self._store.bucket_name == self.bucket_name:
            source = self._normalized_prefix
            target = self._store.prefix
            if (
                not source
                or not target
                or source == target
                or source.startswith(target + "/")
                or target.startswith(source + "/")
            ):
                raise TypeError("GCS source and published_root prefixes must not overlap")

    def create_artifact_store(self) -> ArtifactStore:
        root = self._configured_published_root
        if isinstance(root, str) and root.lower().startswith("gs://"):
            return GCSArtifactStore(
                bundle_name=self.name, published_root=root, gcp_conn_id=self.published_root_conn_id
            )
        if self.published_root_conn_id is not None:
            raise TypeError("published_root_conn_id requires a gs:// published_root")
        return super().create_artifact_store()

    @property
    def gcs_dags_dir(self) -> Path:
        return self._source_mirror_dir

    @property
    def gcs_mirror_state_path(self) -> Path:
        return self._mirror_state_path

    @property
    def gcs_hook(self) -> GCSHook:
        """Construct the provider hook only when an unpinned source operation needs it."""
        if self._gcs_hook is None:
            try:
                self._gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
            except Exception as e:
                raise BundleManifestError(
                    f"Could not create a GCS client for bucket {self.bucket_name!r}"
                ) from e
        return self._gcs_hook

    def view_url_template(self) -> str | None:
        configured_template = self._view_url_template
        if configured_template:
            return configured_template
        if not isinstance(self._store, GCSArtifactStore):
            return None
        snapshots_path = quote(self._store.snapshots_root.removeprefix("gs://"), safe="/")
        return (
            f"https://console.cloud.{GoogleBaseHook.get_high_value_cookie_domain()}"
            f"/storage/browser/{snapshots_path}/{{version}}"
        )

    def _prepare_artifact_store(self, client: Any, observation: ObjectSourceObservation) -> None:
        if not isinstance(self._store, GCSArtifactStore):
            return
        endpoint = self._get_source_endpoint(client)
        self._store.copy_hints = {
            entry.relative_path: {
                "type": "gcs",
                "endpoint": endpoint,
                "bucket": self.bucket_name,
                "name": entry.name,
                "generation": entry.generation,
            }
            for entry in observation.entries
        }

    def _validate_source_configuration(self, client: Any) -> None:
        try:
            client.get_bucket(self.bucket_name)
        except Exception as e:
            if _is_missing_gcs_error(e):
                raise BundleManifestNotFoundError(f"GCS bucket {self.bucket_name!r} does not exist") from e
            raise BundleManifestError(f"Could not access GCS bucket {self.bucket_name!r}") from e
        with self._translate_source_error("validate source"):
            if self.prefix and not self.allow_empty_source and self.deployment_marker_key is None:
                probe = client.list_blobs(
                    self.bucket_name,
                    prefix=f"{self._normalized_prefix}/",
                    max_results=1,
                )
                if next(iter(probe), None) is None:
                    raise BundleManifestNotFoundError(
                        f"GCS prefix {self.publish_source_description!r} does not exist"
                    )

    def _get_source_client(self) -> Any:
        with self._translate_source_error("create client"):
            return self.gcs_hook.get_conn()

    def _get_source_endpoint(self, client: Any) -> str | None:
        return get_gcs_api_endpoint(client)

    def _list_source_objects(self, client: Any) -> Iterator[GCSObjectObservation]:
        listing_prefix = f"{self._normalized_prefix}/" if self._normalized_prefix else ""
        with self._translate_source_error("list source"):
            for blob in client.list_blobs(self.bucket_name, prefix=listing_prefix):
                observation = self._build_object_observation(blob)
                if observation is not None:
                    yield observation

    def _read_deployment_marker(self, client: Any) -> GCSObjectObservation | None:
        marker_name = self._marker_remote_name
        if marker_name is None:
            return None
        try:
            blob = client.bucket(self.bucket_name).get_blob(marker_name)
        except Exception as e:
            if _is_missing_gcs_error(e):
                blob = None
            else:
                raise BundleManifestError(
                    f"Could not read the deployment marker for GCS source {self.publish_source_description}"
                ) from e
        if blob is None:
            raise BundleManifestSourceChangedError(
                f"GCS deployment marker {marker_name!r} is missing from bucket {self.bucket_name!r}; "
                "the deployment is not complete"
            )
        return self._build_object_observation(blob)

    def _build_object_observation(self, blob: Any) -> GCSObjectObservation | None:
        name = getattr(blob, "name", None)
        size = getattr(blob, "size", None)
        generation = getattr(blob, "generation", None)
        metageneration = getattr(blob, "metageneration", None)
        updated = getattr(blob, "updated", None)
        etag = getattr(blob, "etag", None)
        if hasattr(updated, "isoformat"):
            updated = updated.isoformat()
        if (
            not isinstance(name, str)
            or not isinstance(size, int)
            or isinstance(size, bool)
            or size < 0
            or not isinstance(generation, int)
            or isinstance(generation, bool)
            or generation < 1
            or not isinstance(metageneration, int)
            or isinstance(metageneration, bool)
            or metageneration < 1
            or not isinstance(updated, str)
            or not updated
            or not isinstance(etag, str)
            or not etag
        ):
            raise BundleManifestError("GCS listing returned invalid object metadata")

        relative_path = _get_relative_object_path(
            name=name,
            normalized_prefix=self._normalized_prefix,
            label=self._source_label,
            noun=self._remote_name_noun,
        )
        if relative_path is None:
            return None
        content_encoding = getattr(blob, "content_encoding", None)
        if content_encoding and content_encoding.lower() != "identity":
            raise BundleManifestError(
                f"GCS object {name!r} has unsupported Content-Encoding {content_encoding!r}. "
                "Upload source objects without content encoding so the stored bytes are directly usable."
            )
        return GCSObjectObservation(
            relative_path=relative_path,
            name=name,
            size=size,
            generation=generation,
            metageneration=metageneration,
            updated=updated,
            etag=etag,
        )

    def _download_entry(self, client: Any, *, entry: Any, tmp_path: Path) -> None:
        blob = client.bucket(self.bucket_name).blob(entry.name, generation=entry.generation)
        try:
            blob.download_to_filename(
                str(tmp_path),
                if_generation_match=entry.generation,
                raw_download=True,
            )
        except Exception as e:
            if _is_missing_gcs_error(e) or _is_gcs_precondition_error(e):
                raise BundleManifestSourceChangedError(
                    f"GCS object {entry.name!r} changed before it could be downloaded"
                ) from e
            raise BundleManifestError(
                f"Could not download object {entry.name!r} from GCS source {self.publish_source_description}"
            ) from e


def publish_manifest_gcs_dag_bundle(
    *,
    bundle: ManifestGCSDagBundle,
    expected_current_version: str | None = None,
) -> BundlePublishResult:
    """Publish the configured GCS source as an immutable manifest-backed snapshot."""
    return publish_manifest_object_store_dag_bundle(
        bundle=bundle,
        expected_current_version=expected_current_version,
    )


__all__ = [
    "DEFAULT_GCP_CONN_ID",
    "DEFAULT_MAX_FILE_COUNT",
    "DEFAULT_MAX_FILE_SIZE_BYTES",
    "DEFAULT_MAX_TOTAL_SIZE_BYTES",
    "GCSObjectObservation",
    "GCSSourceObservation",
    "ManifestGCSDagBundle",
    "publish_manifest_gcs_dag_bundle",
]
