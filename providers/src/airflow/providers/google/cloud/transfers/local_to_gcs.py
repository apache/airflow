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
"""This module contains operator for uploading local file(s) to GCS."""

from __future__ import annotations

import os
from collections.abc import Sequence
from glob import glob
from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class LocalFilesystemToGCSOperator(BaseOperator):
    """
    Uploads a file or list of files to Google Cloud Storage; optionally can compress the file for upload; optionally can upload the data in multiple chunks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:LocalFilesystemToGCSOperator`

    :param src: Path to the local file, or list of local files. Path can be either absolute
        (e.g. /path/to/file.ext) or relative (e.g. ../../foo/*/*.csv). (templated)
    :param dst: Destination path within the specified bucket on GCS (e.g. /path/to/file.ext).
        If multiple files are being uploaded, specify object prefix with trailing backslash
        (e.g. /path/to/directory/) (templated)
    :param bucket: The bucket to upload to. (templated)
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param mime_type: The mime-type string
    :param gzip: Allows for file to be compressed and uploaded as gzip
    :param chunk_size: Blob chunk size in bytes. This must be a multiple of 262144 bytes (256 KiB)
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "src",
        "dst",
        "bucket",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        src: str | list[str],
        dst: str,
        bucket: str,
        gcp_conn_id: str = "google_cloud_default",
        mime_type: str = "application/octet-stream",
        gzip: bool = False,
        chunk_size: int | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.src = src
        self.dst = dst
        self.bucket = bucket
        self.gcp_conn_id = gcp_conn_id
        self.mime_type = mime_type
        self.gzip = gzip
        self.chunk_size = chunk_size
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        """Upload a file or list of files to Google Cloud Storage."""
        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        filepaths = self.src if isinstance(self.src, list) else glob(self.src)
        if not filepaths:
            raise FileNotFoundError(self.src)
        if os.path.basename(self.dst):  # path to a file
            if len(filepaths) > 1:  # multiple file upload
                raise ValueError(
                    "'dst' parameter references filepath. Please specify "
                    "directory (with trailing backslash) to upload multiple "
                    "files. e.g. /path/to/directory/"
                )
            object_paths = [self.dst]
        else:  # directory is provided
            object_paths = [os.path.join(self.dst, os.path.basename(filepath)) for filepath in filepaths]

        for filepath, object_path in zip(filepaths, object_paths):
            hook.upload(
                bucket_name=self.bucket,
                object_name=object_path,
                mime_type=self.mime_type,
                filename=filepath,
                gzip=self.gzip,
                chunk_size=self.chunk_size,
            )

    def get_openlineage_facets_on_start(self):
        from airflow.providers.common.compat.openlineage.facet import (
            Dataset,
            Identifier,
            SymlinksDatasetFacet,
        )
        from airflow.providers.google.cloud.openlineage.utils import WILDCARD, extract_ds_name_from_gcs_path
        from airflow.providers.openlineage.extractors import OperatorLineage

        source_facets = {}
        if isinstance(self.src, str):  # Single path provided, possibly relative or with wildcard
            original_src = f"{self.src}"
            absolute_src = os.path.abspath(self.src)
            resolved_src = extract_ds_name_from_gcs_path(absolute_src)
            if original_src.startswith("/") and not resolved_src.startswith("/"):
                resolved_src = "/" + resolved_src
            source_objects = [resolved_src]

            if WILDCARD in original_src or absolute_src != resolved_src:
                # We attach a symlink with unmodified path.
                source_facets = {
                    "symlink": SymlinksDatasetFacet(
                        identifiers=[Identifier(namespace="file", name=original_src, type="file")]
                    ),
                }
        else:
            source_objects = self.src

        dest_object = self.dst if os.path.basename(self.dst) else extract_ds_name_from_gcs_path(self.dst)

        return OperatorLineage(
            inputs=[Dataset(namespace="file", name=src, facets=source_facets) for src in source_objects],
            outputs=[Dataset(namespace=f"gs://{self.bucket}", name=dest_object)],
        )
