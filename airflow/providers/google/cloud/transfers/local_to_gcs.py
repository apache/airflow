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
import os
import warnings
from glob import glob
from typing import Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.decorators import apply_defaults


class LocalFilesystemToGCSOperator(BaseOperator):
    """
    Uploads a file or list of files to Google Cloud Storage.
    Optionally can compress the file for upload.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:LocalFilesystemToGCSOperator`

    :param src: Path to the local file, or list of local files. Path can be either absolute
        (e.g. /path/to/file.ext) or relative (e.g. ../../foo/*/*.csv). (templated)
    :type src: str or list
    :param dst: Destination path within the specified bucket on GCS (e.g. /path/to/file.ext).
        If multiple files are being uploaded, specify object prefix with trailing backslash
        (e.g. /path/to/directory/) (templated)
    :type dst: str
    :param bucket: The bucket to upload to. (templated)
    :type bucket: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type google_cloud_storage_conn_id: str
    :param mime_type: The mime-type string
    :type mime_type: str
    :param delegate_to: The account to impersonate, if any
    :type delegate_to: str
    :param gzip: Allows for file to be compressed and uploaded as gzip
    :type gzip: bool
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'src',
        'dst',
        'bucket',
        'impersonation_chain',
    )

    @apply_defaults
    def __init__(
        self,
        *,
        src,
        dst,
        bucket,
        gcp_conn_id='google_cloud_default',
        google_cloud_storage_conn_id=None,
        mime_type='application/octet-stream',
        delegate_to=None,
        gzip=False,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        if google_cloud_storage_conn_id:
            warnings.warn(
                "The google_cloud_storage_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.",
                DeprecationWarning,
                stacklevel=3,
            )
            gcp_conn_id = google_cloud_storage_conn_id

        self.src = src
        self.dst = dst
        self.bucket = bucket
        self.gcp_conn_id = gcp_conn_id
        self.mime_type = mime_type
        self.delegate_to = delegate_to
        self.gzip = gzip
        self.impersonation_chain = impersonation_chain

    def execute(self, context):
        """Uploads a file or list of files to Google Cloud Storage"""
        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        filepaths = self.src if isinstance(self.src, list) else glob(self.src)
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
            )
