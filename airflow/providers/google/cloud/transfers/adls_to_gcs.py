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
"""
This module contains Azure Data Lake Storage to
Google Cloud Storage operator.
"""
from __future__ import annotations

import os
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Sequence

from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook
from airflow.providers.microsoft.azure.operators.adls import ADLSListOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ADLSToGCSOperator(ADLSListOperator):
    """
    Synchronizes an Azure Data Lake Storage path with a GCS bucket

    :param src_adls: The Azure Data Lake path to find the objects (templated)
    :param dest_gcs: The Google Cloud Storage bucket and prefix to
        store the objects. (templated)
    :param replace: If true, replaces same-named files in GCS
    :param gzip: Option to compress file for upload
    :param azure_data_lake_conn_id: The connection ID to use when
        connecting to Azure Data Lake Storage.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param delegate_to: Google account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param google_impersonation_chain: Optional Google service account to impersonate using
        short-term credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    **Examples**:
        The following Operator would copy a single file named
        ``hello/world.avro`` from ADLS to the GCS bucket ``mybucket``. Its full
        resulting gcs path will be ``gs://mybucket/hello/world.avro`` ::

            copy_single_file = AdlsToGoogleCloudStorageOperator(
                task_id='copy_single_file',
                src_adls='hello/world.avro',
                dest_gcs='gs://mybucket',
                replace=False,
                azure_data_lake_conn_id='azure_data_lake_default',
                gcp_conn_id='google_cloud_default'
            )

        The following Operator would copy all parquet files from ADLS
        to the GCS bucket ``mybucket``. ::

            copy_all_files = AdlsToGoogleCloudStorageOperator(
                task_id='copy_all_files',
                src_adls='*.parquet',
                dest_gcs='gs://mybucket',
                replace=False,
                azure_data_lake_conn_id='azure_data_lake_default',
                gcp_conn_id='google_cloud_default'
            )

         The following Operator would copy all parquet files from ADLS
         path ``/hello/world``to the GCS bucket ``mybucket``. ::

            copy_world_files = AdlsToGoogleCloudStorageOperator(
                task_id='copy_world_files',
                src_adls='hello/world/*.parquet',
                dest_gcs='gs://mybucket',
                replace=False,
                azure_data_lake_conn_id='azure_data_lake_default',
                gcp_conn_id='google_cloud_default'
            )
    """

    template_fields: Sequence[str] = (
        'src_adls',
        'dest_gcs',
        'google_impersonation_chain',
    )
    ui_color = '#f0eee4'

    def __init__(
        self,
        *,
        src_adls: str,
        dest_gcs: str,
        azure_data_lake_conn_id: str,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: str | None = None,
        replace: bool = False,
        gzip: bool = False,
        google_impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:

        super().__init__(path=src_adls, azure_data_lake_conn_id=azure_data_lake_conn_id, **kwargs)

        self.src_adls = src_adls
        self.dest_gcs = dest_gcs
        self.replace = replace
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.gzip = gzip
        self.google_impersonation_chain = google_impersonation_chain

    def execute(self, context: Context):
        # use the super to list all files in an Azure Data Lake path
        files = super().execute(context)
        g_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.google_impersonation_chain,
        )

        if not self.replace:
            # if we are not replacing -> list all files in the ADLS path
            # and only keep those files which are present in
            # ADLS and not in Google Cloud Storage
            bucket_name, prefix = _parse_gcs_url(self.dest_gcs)
            existing_files = g_hook.list(bucket_name=bucket_name, prefix=prefix)
            files = list(set(files) - set(existing_files))

        if files:
            hook = AzureDataLakeHook(azure_data_lake_conn_id=self.azure_data_lake_conn_id)

            for obj in files:
                with NamedTemporaryFile(mode='wb', delete=True) as f:
                    hook.download_file(local_path=f.name, remote_path=obj)
                    f.flush()
                    dest_gcs_bucket, dest_gcs_prefix = _parse_gcs_url(self.dest_gcs)
                    dest_path = os.path.join(dest_gcs_prefix, obj)
                    self.log.info("Saving file to %s", dest_path)

                    g_hook.upload(
                        bucket_name=dest_gcs_bucket, object_name=dest_path, filename=f.name, gzip=self.gzip
                    )

            self.log.info("All done, uploaded %d files to GCS", len(files))
        else:
            self.log.info("In sync, no files needed to be uploaded to GCS")

        return files
