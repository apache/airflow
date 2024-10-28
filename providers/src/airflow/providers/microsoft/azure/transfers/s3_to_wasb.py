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
from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


# Create three custom exception that are
class TooManyFilesToMoveException(Exception):
    """Custom exception thrown when attempting to move multiple files from S3 to a single Azure Blob."""

    def __init__(self, number_of_files: int):
        # Call the parent constructor with a simple message
        message: str = f"{number_of_files} cannot be moved to a single Azure Blob."
        super().__init__(message)


class InvalidAzureBlobParameters(Exception):
    """Custom exception raised when neither a blob_prefix or blob_name are passed to the operator."""

    def __init__(self):
        message: str = "One of blob_name or blob_prefix must be provided."
        super().__init__(message)


class InvalidKeyComponents(Exception):
    """Custom exception raised when neither a full_path or file_name + prefix are provided to _create_key."""

    def __init__(self):
        message = "Either full_path of prefix and file_name must not be None"
        super().__init__(message)


class S3ToAzureBlobStorageOperator(BaseOperator):
    """
    Operator to move data from and AWS S3 Bucket to Microsoft Azure Blob Storage.

    A similar class exists to move data from Microsoft Azure Blob Storage to an AWS S3 Bucket, and lives in
    the airflow/providers/amazon/aws/transfers/azure_blob_to_s3.py file

    Either an explicit S3 key can be provided, or a prefix containing the files that are to be transferred to
    Azure blob storage. The same holds for a Blob name; an explicit name can be passed, or a Blob prefix can
    be provided for the file to be stored to

    .. seealso:
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator::SFTPToWasbOperator`

    :param aws_conn_id: ID for the AWS S3 connection to use.
    :param wasb_conn_id: ID for the Azure Blob Storage connection to use.
    :param s3_bucket: The name of the AWS S3 bucket that an object (or objects)  would be transferred from.
        (templated)
    :param container_name: The name of the Azure Storage Blob container an object (or objects) would be
        transferred to. (templated)
    :param s3_prefix: Prefix string that filters any S3 objects that begin with this prefix. (templated)
    :param s3_key: An explicit S3 key (object) to be transferred. (templated)
    :param blob_prefix: Prefix string that would provide a path in the Azure Storage Blob container for an
        object (or objects) to be moved to. (templated)
    :param blob_name: An explicit blob name that an object would be transferred to. This can only be used
        if a single file is being moved. If there are multiple files in an S3 bucket that are to be moved
        to a single Azure blob, an exception will be raised. (templated)
    :param create_container: True if a container should be created if it did not already exist, False
        otherwise.
    :param replace: If a blob exists in the container and replace takes a value of true, it will be
        overwritten. If replace is False and a blob exists in the container, the file will NOT be
        overwritten.
    :param s3_verify: Whether or not to verify SSL certificates for S3 connection.
        By default, SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :param s3_extra_args: kwargs to pass to S3Hook.
    :param wasb_extra_args: kwargs to pass to WasbHook.
    """

    template_fields: Sequence[str] = (
        "s3_bucket",
        "container_name",
        "s3_prefix",
        "s3_key",
        "blob_prefix",
        "blob_name",
    )

    def __init__(
        self,
        *,
        aws_conn_id: str = "aws_default",
        wasb_conn_id: str = "wasb_default",
        s3_bucket: str,
        container_name: str,
        s3_prefix: str
        | None = None,  # Only use this to pull an entire directory of files
        s3_key: str | None = None,  # Only use this to pull a single file
        blob_prefix: str | None = None,
        blob_name: str | None = None,
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
            aws_conn_id=self.aws_conn_id, verify=self.s3_verify, **self.s3_extra_args
        )

    @cached_property
    def wasb_hook(self) -> WasbHook:
        """Create and return a WasbHook."""
        return WasbHook(wasb_conn_id=self.wasb_conn_id, **self.wasb_extra_args)

    def execute(self, context: Context) -> list[str]:
        """Execute logic below when operator is executed as a task."""
        self.log.info(
            "Getting %s from %s"
            if self.s3_key
            else "Getting all files start with %s from %s",
            self.s3_key if self.s3_key else self.s3_prefix,
            self.s3_bucket,
        )

        # Pull a list of files to move from S3 to Azure Blob storage
        files_to_move: list[str] = self.get_files_to_move()

        # Check to see if there are indeed files to move. If so, move each of these files. Otherwise, output
        # a logging message that denotes there are no files to move
        if files_to_move:
            for file_name in files_to_move:
                self.move_file(file_name)

            # Assuming that files_to_move is a list (which it always should be), this will get "hit" after the
            # last file is moved from S3 -> Azure Blob
            self.log.info("All done, uploaded %s to Azure Blob.", len(files_to_move))

        else:
            # If there are no files to move, a message will be logged. May want to consider alternative
            # functionality (should an exception instead be raised?)
            self.log.info("There are no files to move!")

        # Return a list of the files that were moved
        return files_to_move

    def get_files_to_move(self) -> list[str]:
        """Determine the list of files that need to be moved, and return the name."""
        if self.s3_key:
            # Only pull the file name from the s3_key, drop the rest of the key
            files_to_move: list[str] = [self.s3_key.split("/")[-1]]
        else:
            # Pull the keys from the s3_bucket using the provided prefix. Remove the prefix from the file
            # name, and add to the list of files to move
            s3_keys: list[str] = self.s3_hook.list_keys(
                bucket_name=self.s3_bucket, prefix=self.s3_prefix
            )
            files_to_move = [
                s3_key.replace(f"{self.s3_prefix}/", "", 1) for s3_key in s3_keys
            ]

            # Now, make sure that there are not too many files to move to a single Azure blob
            if self.blob_name and len(files_to_move) > 1:
                raise TooManyFilesToMoveException(len(files_to_move))

        if not self.replace:
            # Only grab the files from S3 that are not in Azure Blob already. This will prevent any files that
            # exist in both S3 and Azure Blob from being overwritten. If a blob_name is provided, check to
            # see if that blob exists
            azure_blob_files: list[str] = []

            if self.blob_name:
                # If the singular blob (stored at self.blob_name) exists, add it to azure_blob_files so it
                # can be removed from the list of files to move
                if self.wasb_hook.check_for_blob(self.container_name, self.blob_name):
                    azure_blob_files.append(self.blob_name.split("/")[-1])

            elif self.blob_prefix:
                azure_blob_files += self.wasb_hook.get_blobs_list_recursive(
                    container_name=self.container_name, prefix=self.blob_prefix
                )
            else:
                raise InvalidAzureBlobParameters

            # This conditional block only does one thing - it alters the elements in the files_to_move list.
            # This list is being trimmed to remove the existing files in the Azure Blob (as mentioned above)
            existing_files = azure_blob_files if azure_blob_files else []
            files_to_move = list(set(files_to_move) - set(existing_files))

        return files_to_move

    def move_file(self, file_name: str) -> None:
        """Move file from S3 to Azure Blob storage."""
        with tempfile.NamedTemporaryFile("w") as temp_file:
            # If using an s3_key, this creates a scenario where the only file in the files_to_move
            # list is going to be the name pulled from the s3_key. It's not verbose, but provides
            # standard implementation across the operator
            source_s3_key: str = self._create_key(self.s3_key, self.s3_prefix, file_name)

            # Create retrieve the S3 client itself, rather than directly using the hook. Download the file to
            # the temp_file.name
            s3_client = self.s3_hook.get_conn()
            s3_client.download_file(self.s3_bucket, source_s3_key, temp_file.name)

            # Load the file to Azure Blob using either the key that has been passed in, or the key
            # from the list of files present in the s3_prefix, plus the blob_prefix. There may be
            # desire to only pass in an S3 key, in which case, the blob_name should be derived from
            # the S3 key
            destination_azure_blob_name: str = self._create_key(
                self.blob_name, self.blob_prefix, file_name
            )
            self.wasb_hook.load_file(
                file_path=temp_file.name,
                container_name=self.container_name,
                blob_name=destination_azure_blob_name,
                create_container=self.create_container,
                **self.wasb_extra_args,
            )

    @staticmethod
    def _create_key(full_path: str | None, prefix: str | None, file_name: str | None):
        """Return a file key using its components."""
        if full_path:
            return full_path
        elif prefix and file_name:
            return f"{prefix}/{file_name}"
        else:
            raise InvalidKeyComponents
