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
"""This module contains Google Cloud Storage to Azure Blob Storage operator."""

from __future__ import annotations

import os
from collections.abc import Sequence
from typing import TYPE_CHECKING

from packaging.version import Version

from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

try:
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
except ModuleNotFoundError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

if TYPE_CHECKING:
    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.sdk import Context


class GCSToAzureBlobStorageOperator(BaseOperator):
    """
    Synchronizes objects from a Google Cloud Storage bucket to Azure Blob Storage.

    .. note::
        When ``flatten_structure=True``, it takes precedence over ``keep_directory_structure``.
        For example, with ``flatten_structure=True``, ``folder/subfolder/file.txt`` becomes
        ``file.txt`` regardless of the ``keep_directory_structure`` setting.

        Objects whose names end with ``/`` (GCS console folder markers) and keys that become an
        empty destination path after ``flatten_structure`` are skipped.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GCSToAzureBlobStorageOperator`

    :param gcs_bucket: The GCS bucket to list objects from. (templated)
    :param prefix: Prefix to filter object names under the bucket. (templated)
    :param gcp_conn_id: Airflow connection ID for Google Cloud.
    :param google_impersonation_chain: Optional impersonation chain for GCP credentials.
    :param gcp_user_project: Requester-pays billing project for GCS requests, if required.
    :param match_glob: Optional glob filter for object names (requires
        ``apache-airflow-providers-google>=10.3.0``).
    :param container_name: Azure Blob container to upload into. (templated)
    :param blob_prefix: Base blob path for uploaded objects. (templated)
    :param wasb_conn_id: Airflow connection ID for Azure Blob Storage.
    :param replace: If ``True``, overwrite existing blobs (``overwrite=True`` on upload) and
        upload all listed objects. If ``False``, skip objects that already exist under
        ``blob_prefix`` with the same relative path and pass ``overwrite=False`` on upload.
    :param keep_directory_structure: When ``False`` and ``prefix`` is set (and
        ``flatten_structure`` is ``False``), append ``prefix`` to ``blob_prefix``.
    :param flatten_structure: If ``True``, upload each object using only its file name
        under ``blob_prefix``. Takes precedence over ``keep_directory_structure``.
    :param create_container: If ``True``, create the container when missing before upload.
    """

    template_fields: Sequence[str] = (
        "gcs_bucket",
        "prefix",
        "blob_prefix",
        "container_name",
        "google_impersonation_chain",
        "gcp_user_project",
        "match_glob",
    )
    ui_color = "#f0eee4"

    def __init__(
        self,
        *,
        gcs_bucket: str,
        container_name: str,
        blob_prefix: str = "",
        prefix: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        google_impersonation_chain: str | Sequence[str] | None = None,
        wasb_conn_id: str = "wasb_default",
        replace: bool = False,
        keep_directory_structure: bool = True,
        flatten_structure: bool = False,
        match_glob: str | None = None,
        gcp_user_project: str | None = None,
        create_container: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcs_bucket = gcs_bucket
        self.prefix = prefix
        self.gcp_conn_id = gcp_conn_id
        self.google_impersonation_chain = google_impersonation_chain
        self.container_name = container_name
        self.blob_prefix = blob_prefix
        self.wasb_conn_id = wasb_conn_id
        self.replace = replace
        self.keep_directory_structure = keep_directory_structure
        self.flatten_structure = flatten_structure
        self.gcp_user_project = gcp_user_project
        self.create_container = create_container

        if self.flatten_structure and self.keep_directory_structure:
            self.log.warning("flatten_structure=True takes precedence over keep_directory_structure=True")

        try:
            from airflow.providers.google import __version__ as _GOOGLE_PROVIDER_VERSION

            if Version(_GOOGLE_PROVIDER_VERSION) >= Version("10.3.0"):
                self._is_match_glob_supported = True
            else:
                self._is_match_glob_supported = False
        except ImportError:
            self._is_match_glob_supported = False
        if not self._is_match_glob_supported and match_glob:
            raise ValueError("The 'match_glob' parameter requires 'apache-airflow-providers-google>=10.3.0'.")
        self.match_glob = match_glob

    def _transform_file_path(self, file_path: str) -> str:
        """
        Transform the GCS file path according to the specified options.

        :param file_path: The original GCS file path
        :return: The transformed file path for Azure Blob destination
        """
        if self.flatten_structure:
            return os.path.basename(file_path)
        return file_path

    @staticmethod
    def _should_skip_gcs_object(name: str) -> bool:
        """
        Return True if this object name should not be copied.

        The GCS console creates zero-byte "folder" markers whose keys end with ``/``.
        Those yield an empty basename when ``flatten_structure=True`` and odd blob paths
        on Azure when copied as-is.
        """
        if not name or not name.strip():
            return True
        return name.endswith("/")

    def execute(self, context: Context) -> list[str]:
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.google_impersonation_chain,
        )
        wasb_hook = WasbHook(wasb_conn_id=self.wasb_conn_id)

        self.log.info(
            "Getting list of the files. Bucket: %s; Prefix: %s",
            self.gcs_bucket,
            self.prefix,
        )

        list_kwargs: dict = {
            "bucket_name": self.gcs_bucket,
            "prefix": self.prefix,
            "user_project": self.gcp_user_project,
        }
        if self._is_match_glob_supported:
            list_kwargs["match_glob"] = self.match_glob

        gcs_files = gcs_hook.list(**list_kwargs)  # type: ignore[call-arg]

        gcs_files = [f for f in gcs_files if not self._should_skip_gcs_object(f)]

        blob_prefix = self.blob_prefix
        if not self.keep_directory_structure and self.prefix and not self.flatten_structure:
            blob_prefix = os.path.join(blob_prefix, self.prefix)

        existing_blobs_set: set[str] = set()
        if not self.replace:
            existing_blobs = (
                wasb_hook.get_blobs_list_recursive(
                    container_name=self.container_name,
                    prefix=blob_prefix or None,
                )
                or []
            )
            if blob_prefix:
                prefix_str = blob_prefix.rstrip("/") + "/"
                existing_blobs = [b.removeprefix(prefix_str) for b in existing_blobs]
            existing_blobs_set = set(existing_blobs)

        filtered_files: list[str] = []
        seen_transformed: set[str] = set()
        for file in gcs_files:
            transformed = self._transform_file_path(file)
            if transformed in existing_blobs_set:
                continue
            if transformed in seen_transformed:
                self.log.warning(
                    "Skipping duplicate file %s (transforms to %s)",
                    file,
                    transformed,
                )
                continue
            filtered_files.append(file)
            seen_transformed.add(transformed)

        gcs_files = filtered_files

        uploaded_blobs: list[str] = []
        if gcs_files:
            for file in gcs_files:
                with gcs_hook.provide_file(
                    object_name=file, bucket_name=str(self.gcs_bucket), user_project=self.gcp_user_project
                ) as local_tmp_file:
                    transformed_path = self._transform_file_path(file)
                    dest_blob = os.path.join(blob_prefix, transformed_path)
                    self.log.info("Saving file from %s to %s", file, dest_blob)
                    wasb_hook.load_file(
                        file_path=local_tmp_file.name,
                        container_name=self.container_name,
                        blob_name=dest_blob,
                        create_container=self.create_container,
                        overwrite=self.replace,
                    )
                    uploaded_blobs.append(dest_blob)
            self.log.info("All done, uploaded %d files to Azure Blob Storage", len(uploaded_blobs))
        else:
            self.log.info("In sync, no files needed to be uploaded to Azure Blob Storage")

        return uploaded_blobs

    def get_openlineage_facets_on_start(self) -> OperatorLineage:
        from airflow.providers.common.compat.openlineage.facet import Dataset
        from airflow.providers.openlineage.extractors import OperatorLineage

        wasb_hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        account_name = wasb_hook.get_conn().account_name

        return OperatorLineage(
            inputs=[Dataset(namespace=f"gs://{self.gcs_bucket}", name=self.prefix or "/")],
            outputs=[
                Dataset(
                    namespace=f"wasbs://{self.container_name}@{account_name}",
                    name=self.blob_prefix or "/",
                )
            ],
        )
