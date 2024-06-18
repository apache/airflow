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
from typing import TYPE_CHECKING, Sequence, List

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class S3ToAzureBlobStorageOperator(BaseOperator):
    """
    Operator to move data from and AWS S3 Bucket to Microsoft Azure Blob Storage. A similar class
    exists to move data from Microsoft Azure Blob Storage to an AWS S3 Bucket, and lives in the
    airflow/providers/amazon/aws/transfers/azure_blob_to_s3.py file

    TODO: Complete doc-string
    """

    template_fields: Sequence[str] = (
        "s3_bucket",
        "container_name",
        "s3_prefix",
        "s3_key",
        "blob_prefix",
        "blob_name"
    )

    def __init__(
        self,
        *,
        aws_conn_id: str = "aws_default",
        wasb_conn_id: str = "wasb_default",
        s3_bucket: str,
        container_name: str,
        s3_prefix: str | None = None,  # Only use this to pull an entire directory of files
        s3_key: str | None = None,  # Only use this to pull a single file
        blob_prefix: str | None = None,
        blob_name: str | None = None,
        delimiter: str | None = None,
        create_container: bool = False,
        replace: bool = False,
        s3_verify: bool = False,
        s3_extra_args: dict | None = None,
        wasb_extra_args: dict | None = None,
        **kwargs
    ):
        # Call to constructor of the inherited BaseOperator class
        super().__init__(**kwargs)

        self.aws_conn_id = aws_conn_id
        self.wasb_conn_id = wasb_conn_id
        self.s3_bucket = s3_bucket
        self.container_name = container_name
        self.s3_prefix = s3_prefix if s3_prefix.endswith("/") else s3_prefix + "/"
        self.s3_key = s3_key
        self.blob_prefix = blob_prefix if blob_prefix.endswith("/") else blob_prefix + "/"
        self.blob_name = blob_name
        self.delimiter = delimiter
        self.create_container = create_container
        self.replace = replace
        self.s3_verify = s3_verify
        self.s3_extra_args = s3_extra_args
        self.wasb_extra_args = wasb_extra_args

    def execute(self, context: Context) -> List[str]:
        """
        execute

        Description:
            execute() method is called when the Operator is triggered in a DAG

        Params:
            context (Content)

        Returns:
            None
        """
        # First, create a WasbHook and an S3Hook using the conn's that were provided
        wasb_hook: WasbHook = WasbHook(wasb_conn_id=self.wasb_conn_id, **self.wasb_extra_args)
        s3_hook: S3Hook = S3Hook(
            aws_conn_id=self.aws_conn_id,
            verify=self.s3_verify,
            **self.s3_extra_args,
        )

        self.log.info(
            f"Getting list of files in the Bucket: {self.s3_bucket}"
            f"Getting file: {self.s3_key}" if self.s3_key else f"Getting files from: {self.s3_prefix}"
        )

        # First, try to use the S3 key that is passed in. Otherwise, pull all files from the S3 bucket/path
        # prefix and remove the prefix
        if self.s3_key:
            # Only pull the file name from the s3_key
            files_to_move: List[str] = [self.s3_key.split("/")[-1]]
        else:
            # Pull the keys from the s3_bucket using the provided prefix. Remove the prefix from the file
            # name, and add to the list of files to move
            s3_keys: List[str] = s3_hook.list_keys(
                bucket_name=self.s3_bucket,
                prefix=self.s3_prefix,
                delimiter=self.delimiter  # Optional filter by the "extension" of the file
            )
            files_to_move = [s3_key.replace(f"{self.s3_prefix}/", "", 1) for s3_key in s3_keys]

        if not self.replace:
            # Only grab the files from S3 that are not in Azure Blob already. This will prevent any files that
            # exist in both S3 and Azure Blob from being overwritten. If a blob_name is provided, create a
            # list with only this value
            azure_blob_files: List[str] = [self.blob_name] if self.blob_name else \
                wasb_hook.get_blobs_list_recursive(
                container_name=self.container_name,
                prefix=self.blob_prefix,
                endswith=self.delimiter  # Optional filter by the "extension" of the file
            )
            existing_files = azure_blob_files if azure_blob_files else []  # TODO: Clean blob names
            files_to_move = list(set(files_to_move) - set(existing_files))

        if files_to_move:
            for file in files_to_move:
                with tempfile.NamedTemporaryFile() as temp_file:
                    # If using an s3_key, this creates a scenario where the only file in the files_to_move
                    # list is going to be the name pulled from the s3_key. It's not verbose, but provides
                    # standard implementation across the operator
                    source_s3_key: str = self.s3_key if self.s3_key else self.s3_prefix + file
                    s3_hook.download_file(
                        local_path=temp_file.name,  # Make sure to look at this
                        bucket_name=self.s3_bucket,
                        key=source_s3_key
                    )

                    # Load the file to Azure Blob using either the key that has been passed in, or the key
                    # from the list of files present in the s3_prefix, plus the blob_prefix
                    destination_azure_blob_name: str = self.blob_name if self.blob_name \
                        else self.blob_prefix + file
                    wasb_hook.load_file(
                        file_path=temp_file.name,
                        container_name=self.container_name,
                        blob_name=destination_azure_blob_name,
                        create_container=self.create_container,
                        **self.wasb_extra_args
                    )

            self.log.info(f"All done, uploaded {len(files_to_move)} to Azure Blob.")

        else:
            self.log.info("There are no files to move!")

        # Return a list of the files that were moved
        return files_to_move
