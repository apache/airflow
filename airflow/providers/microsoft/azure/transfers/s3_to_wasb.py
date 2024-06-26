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

from functools import cached_property
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

    Either an explicit S3 key can be provided, or a prefix containing the files that are to be transferred to
    Azure blob storage. The same holds for a Blob name; an explicit name can be passed, or a Blob prefix can
    be provided for the file to be stored to
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
        **kwargs,
    ):
        # Call to constructor of the inherited BaseOperator class
        super().__init__(**kwargs)

        self.aws_conn_id = aws_conn_id
        self.wasb_conn_id = wasb_conn_id
        self.s3_bucket = s3_bucket
        self.container_name = container_name
        self.s3_prefix = s3_prefix
        self.s3_key = s3_key
        self.blob_prefix = blob_prefix
        self.blob_name = blob_name
        self.delimiter = delimiter
        self.create_container = create_container
        self.replace = replace
        self.s3_verify = s3_verify
        self.s3_extra_args = s3_extra_args or {}
        self.wasb_extra_args = wasb_extra_args or {}

    # These cached properties come in handy when working with hooks. Rather than closing and opening new
    # hooks, the same hook can be used across multiple methods (without having to use the constructor to
    # create the hook)
    @cached_property
    def s3_hook(self) -> S3Hook:
        """Create and return an S3Hook."""
        return S3Hook(
            aws_conn_id=self.aws_conn_id,
            verify=self.s3_verify,
            **self.s3_extra_args
        )

    @cached_property
    def wasb_hook(self) -> WasbHook:
        """Create and return a WasbHook."""
        return WasbHook(wasb_conn_id=self.wasb_conn_id, **self.wasb_extra_args)

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
        self.log.info(
            f"Getting list of files in the Bucket: {self.s3_bucket}"
            f"Getting file: {self.s3_key}" if self.s3_key else f"Getting files from: {self.s3_prefix}"
        )

        # Pull a list of files to move from S3 to Azure Blob storage
        files_to_move: List[str] = self.get_files_to_move()

        # Check to see if there are indeed files to move. If so, move each of these files. Otherwise, output
        # a logging message that denotes there are no files to move
        if files_to_move:
            for file_name in files_to_move:
                self.move_file(file_name)

            # Assuming that files_to_move is a list (which it always should be), this will get "hit" after the
            # last file is moved from S3 -> Azure Blob
            self.log.info(f"All done, uploaded {len(files_to_move)} to Azure Blob.")

        else:
            # If there are no files to move, a message will be logged. May want to consider alternative
            # functionality (should an exception instead be raised?)
            self.log.info("There are no files to move!")

        # Return a list of the files that were moved
        return files_to_move

    def get_files_to_move(self) -> List[str]:
        """
        get_files_to_move

        Description:
            This "helper" method will determine the list of files that need to be moved, and return the name
            of each of these files

        Params: None
        Returns:
            files_to_move (List[str])
        """
        if self.s3_key:
            # Only pull the file name from the s3_key, drop the rest of the key
            files_to_move: List[str] = [self.s3_key.split("/")[-1]]
        else:
            # Pull the keys from the s3_bucket using the provided prefix. Remove the prefix from the file
            # name, and add to the list of files to move
            s3_keys: List[str] = self.s3_hook.list_keys(bucket_name=self.s3_bucket, prefix=self.s3_prefix)
            files_to_move = [s3_key.replace(f"{self.s3_prefix}/", "", 1) for s3_key in s3_keys]

        if not self.replace:
            # Only grab the files from S3 that are not in Azure Blob already. This will prevent any files that
            # exist in both S3 and Azure Blob from being overwritten. If a blob_name is provided, check to
            # see if that blob exists
            if self.blob_name:
                azure_blob_files = [self.blob_name.split("/")[-1]] if \
                    self.wasb_hook.check_for_blob(self.container_name, self.blob_name) else \
                    []
            else:
                azure_blob_files: List[str] = self.wasb_hook.get_blobs_list_recursive(
                    container_name=self.container_name,
                    prefix=self.blob_prefix,
                    endswith=self.delimiter  # Optional filter by the "extension" of the file
                )
            # This conditional block only does one thing - it alters the elements in the files_to_move list.
            # This list is being trimmed to remove the existing files in the Azure Blob (as mentioned above)
            existing_files = azure_blob_files if azure_blob_files else []
            files_to_move = list(set(files_to_move) - set(existing_files))

        return files_to_move

    def move_file(self, file_name: str) -> None:
        """
        move_file

        Description:
            Move file from S3 to Azure Blob storage

        Params:
            file_name (str)
        Returns: None
        """
        with tempfile.NamedTemporaryFile("w") as temp_file:
            # If using an s3_key, this creates a scenario where the only file in the files_to_move
            # list is going to be the name pulled from the s3_key. It's not verbose, but provides
            # standard implementation across the operator
            self.log.info(f"File name: {file_name}")
            source_s3_key: str = self.s3_key if self.s3_key else self.s3_prefix + "/" + file_name
            self.s3_hook.get_conn().download_file(
                self.s3_bucket,
                source_s3_key,
                temp_file.name
            )

            # Load the file to Azure Blob using either the key that has been passed in, or the key
            # from the list of files present in the s3_prefix, plus the blob_prefix. There may be
            # desire to only pass in an S3 key, in which case, the blob_name should be derived from
            # the S3 key
            destination_azure_blob_name: str = self.blob_name if self.blob_name \
                else self.blob_prefix + "/" + file_name
            self.wasb_hook.load_file(
                file_path=temp_file.name,
                container_name=self.container_name,
                blob_name=destination_azure_blob_name,
                create_container=self.create_container,
                **self.wasb_extra_args
            )
