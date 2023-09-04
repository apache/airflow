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
from __future__ import annotations

import tempfile
from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class S3ToAzureBlobOperator(BaseOperator):
    """
    This operator enables the transferring of files from an S3 Bucket to Azure Blob Storage container.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3ToAzureBlobOperator`

    :param aws_conn_id: reference to a specific AWS connection
    :param wasb_conn_id: Reference to the wasb connection.
    :param s3_bucket: The targeted s3 bucket. This is the S3 bucket from
        where the file is downloaded.
    :param s3_key: The targeted s3 key. This is the specified file path for
        downloading the file from S3.
    :param blob_name: Name of the blob
    :param container_name: Name of the container
    :param create_container: Attempt to create the target container prior to uploading the blob. This is
            useful if the target container may not exist yet. Defaults to False.
    """

    def __init__(
        self,
        *,
        aws_conn_id="aws_default",
        wasb_conn_id="wasb_default",
        s3_bucket: str,
        s3_key: str,
        blob_name: str,
        container_name: str,
        create_container: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.wasb_conn_id = wasb_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.blob_name = blob_name
        self.container_name = container_name
        self.create_container = create_container

    def execute(self, context: Context) -> None:
        s3_hook = S3Hook(self.aws_conn_id)
        s3_client = s3_hook.get_conn()
        azure_hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        with tempfile.NamedTemporaryFile() as temp_file:
            self.log.info("Downloading data from S3 bucket %s at key %s", self.s3_bucket, self.s3_key)
            s3_client.download_file(self.s3_bucket, self.s3_key, temp_file.name)
            self.log.info("Uploading data to blob %s", self.blob_name)
            azure_hook.load_file(
                file_path=temp_file.name,
                container_name=self.container_name,
                blob_name=self.blob_name,
                create_container=self.create_container,
            )
            self.log.info(
                "Resources have been uploaded from key %s in S3 bucket %s to blob %s",
                self.s3_key,
                self.s3_bucket,
                self.blob_name,
            )
