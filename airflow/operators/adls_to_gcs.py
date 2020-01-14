# -*- coding: utf-8 -*-
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
import os
import warnings
from tempfile import NamedTemporaryFile
from typing import Optional

from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook
from airflow.contrib.operators.adls_list_operator import AzureDataLakeStorageListOperator
from airflow.gcp.hooks.gcs import GCSHook, _parse_gcs_url
from airflow.utils.decorators import apply_defaults


class AdlsToGoogleCloudStorageOperator(AzureDataLakeStorageListOperator):
    """
    Synchronizes an Azure Data Lake Storage path with a GCS bucket

    :param src_adls: The Azure Data Lake path to find the objects (templated)
    :type src_adls: str
    :param dest_gcs: The Google Cloud Storage bucket and prefix to
        store the objects. (templated)
    :type dest_gcs: str
    :param replace: If true, replaces same-named files in GCS
    :type replace: bool
    :param gzip: Option to compress file for upload
    :type gzip: bool
    :param azure_data_lake_conn_id: The connection ID to use when
        connecting to Azure Data Lake Storage.
    :type azure_data_lake_conn_id: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud
        Platform. This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str

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
    template_fields = ('src_adls', 'dest_gcs')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 src_adls: str,
                 dest_gcs: str,
                 azure_data_lake_conn_id: str,
                 gcp_conn_id: str = 'google_cloud_default',
                 google_cloud_storage_conn_id: Optional[str] = None,
                 delegate_to: Optional[str] = None,
                 replace: bool = False,
                 gzip: bool = False,
                 *args,
                 **kwargs) -> None:

        super().__init__(
            path=src_adls,
            azure_data_lake_conn_id=azure_data_lake_conn_id,
            *args,
            **kwargs
        )

        if google_cloud_storage_conn_id:
            warnings.warn(
                "The google_cloud_storage_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = google_cloud_storage_conn_id

        self.src_adls = src_adls
        self.dest_gcs = dest_gcs
        self.replace = replace
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.gzip = gzip

    def execute(self, context):
        # use the super to list all files in an Azure Data Lake path
        files = super().execute(context)
        g_hook = GCSHook(
            google_cloud_storage_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to)

        if not self.replace:
            # if we are not replacing -> list all files in the ADLS path
            # and only keep those files which are present in
            # ADLS and not in Google Cloud Storage
            bucket_name, prefix = _parse_gcs_url(self.dest_gcs)
            existing_files = g_hook.list(bucket_name=bucket_name, prefix=prefix)
            files = set(files) - set(existing_files)

        if files:
            hook = AzureDataLakeHook(
                azure_data_lake_conn_id=self.azure_data_lake_conn_id
            )

            for obj in files:
                with NamedTemporaryFile(mode='wb', delete=True) as f:
                    hook.download_file(local_path=f.name, remote_path=obj)
                    f.flush()
                    dest_gcs_bucket, dest_gcs_prefix = _parse_gcs_url(self.dest_gcs)
                    dest_path = os.path.join(dest_gcs_prefix, obj)
                    self.log.info("Saving file to %s", dest_path)

                    g_hook.upload(
                        bucket_name=dest_gcs_bucket,
                        object_name=dest_path,
                        filename=f.name,
                        gzip=self.gzip
                    )

            self.log.info("All done, uploaded %d files to GCS", len(files))
        else:
            self.log.info("In sync, no files needed to be uploaded to GCS")

        return files
