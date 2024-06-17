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

import os
import tempfile
from typing import TYPE_CHECKING, Sequence

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
        "s3_key",
        "blob_name"
    )

    def __init__(
        self,
        *,
        aws_conn_id: str = "aws_default",
        s3_bucket: str,
        s3_key: str,  # Maybe, s3_key_prefix
        wasb_conn_id: str = "wasb_default",
        blob_name: str,  # TODO: Think about making this optional
        container_name: str,
        create_container: str,
        replace: bool = True,
        s3_extra_args: dict | None = None,
        wasb_extra_args: dict | None = None,
        **kwargs
    ):
        # Call to super
        super().__init__(**kwargs)

        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.wasb_conn_id = wasb_conn_id
        self.blob_name = blob_name
        self.container_name = container_name
        self.create_container = create_container
        self.replace = replace
        self.s3_extra_args = s3_extra_args
        self.wasb_extra_args = wasb_extra_args

        # Placeholder for now, remove this before pushing
        self.dest_verify = None
        self.dest_s3_extra_args = None

    def execute(self, context: Context) -> None:
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
        wasb_hook = WasbHook(wasb_conn_id=self.wasb_conn_id, **self.wasb_extra_args)
        s3_hook = S3Hook(
            aws_conn_id=self.aws_conn_id,
            verify=self.dest_verify,  # TODO: How to handle this? Do we need to verify if it is a source?
            extra_args=self.dest_s3_extra_args,  # TODO: Should we use a different parameter for this?
            **self.s3_extra_args,
        )
        pass
