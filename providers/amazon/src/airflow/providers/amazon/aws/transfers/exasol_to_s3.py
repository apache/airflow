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
"""Transfers data from Exasol database into a S3 Bucket."""

from __future__ import annotations

import os
from collections.abc import Sequence
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Optional

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.exasol.hooks.exasol import ExasolHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ExasolExportOperator(BaseOperator):
    """
    Export data from Exasol database to a local temporary file.

    :param query_or_table: The SQL statement to execute or table name to export.
    :param export_params: Extra parameters for the underlying export_to_file method of Exasol.
    :param query_params: Query parameters for the underlying export_to_file method.
    :param exasol_conn_id: Reference to the Exasol connection.
    """

    template_fields: Sequence[str] = ("query_or_table", "export_params", "query_params")
    template_ext: Sequence[str] = (".sql",)
    ui_color = "#ededed"

    def __init__(
        self,
        *,
        query_or_table: str,
        export_params: Optional[dict] = None,
        query_params: Optional[dict] = None,
        exasol_conn_id: str = "exasol_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query_or_table = query_or_table
        self.export_params = export_params
        self.query_params = query_params
        self.exasol_conn_id = exasol_conn_id

    def execute(self, context: Context) -> str:
        exasol_hook = ExasolHook(exasol_conn_id=self.exasol_conn_id)
        file_path: Optional[str] = None

        try:
            with NamedTemporaryFile("w+", delete=False) as tmp_file:
                file_path = tmp_file.name
                exasol_hook.export_to_file(
                    filename=file_path,
                    query_or_table=self.query_or_table,
                    export_params=self.export_params,
                    query_params=self.query_params,
                )
                tmp_file.flush()
                self.log.info("Data successfully exported to temporary file: %s", file_path)
            return file_path
        except Exception as e:
            self.log.error("Error during export from Exasol: %s", e)
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    self.log.info("Temporary file %s removed after error.", file_path)
                except Exception as cleanup_error:
                    self.log.warning("Failed to remove temporary file %s: %s", file_path, cleanup_error)
            raise


class S3UploadOperator(BaseOperator):
    """
    Upload a local file to an AWS S3 bucket.

    :param file_path: Path to the file to upload.
    :param key: S3 key that will reference the uploaded file.
    :param bucket_name: Name of the S3 bucket.
    :param replace: Whether to overwrite the file if the key already exists.
    :param encrypt: If True, the file is encrypted on the server-side by S3.
    :param gzip: If True, the file is compressed prior to upload.
    :param acl_policy: Canned ACL policy for the uploaded file.
    :param aws_conn_id: Reference to the AWS connection.
    """

    template_fields: Sequence[str] = ("key", "bucket_name")
    ui_color = "#ededed"

    def __init__(
        self,
        *,
        file_path: str,
        key: str,
        bucket_name: Optional[str] = None,
        replace: bool = False,
        encrypt: bool = False,
        gzip: bool = False,
        acl_policy: Optional[str] = None,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.file_path = file_path
        self.key = key
        self.bucket_name = bucket_name
        self.replace = replace
        self.encrypt = encrypt
        self.gzip = gzip
        self.acl_policy = acl_policy
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Context) -> str:
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        try:
            self.log.info("Uploading file %s to S3 with key: %s", self.file_path, self.key)
            s3_hook.load_file(
                filename=self.file_path,
                key=self.key,
                bucket_name=self.bucket_name,
                replace=self.replace,
                encrypt=self.encrypt,
                gzip=self.gzip,
                acl_policy=self.acl_policy,
            )
            self.log.info("File successfully uploaded to S3 with key: %s", self.key)
            return self.key
        except Exception as e:
            self.log.error("Error uploading file to S3: %s", e)
            raise
        finally:
            if os.path.exists(self.file_path):
                try:
                    os.remove(self.file_path)
                    self.log.info("Temporary file %s removed", self.file_path)
                except Exception as cleanup_error:
                    self.log.warning("Failed to remove temporary file %s: %s", self.file_path, cleanup_error)




class ExasolToS3Operator(BaseOperator):
    """
    Wrapper operator that exports data from Exasol to a temporary file and then uploads it to an S3 bucket.
    This operator exposes the same interface as the original implementation.

    :param query_or_table: the SQL statement to be executed or table name to export.
    :param key: S3 key that will reference the uploaded file.
    :param bucket_name: Name of the bucket in which to store the file.
    :param replace: Whether to overwrite the file if the key already exists.
    :param encrypt: If True, the file will be encrypted on the server-side by S3.
    :param gzip: If True, the file will be compressed prior to upload.
    :param acl_policy: Canned ACL policy for the file being uploaded.
    :param query_params: Query parameters for the Exasol export.
    :param export_params: Extra parameters for the Exasol export.
    :param exasol_conn_id: Reference to the Exasol connection.
    :param aws_conn_id: Reference to the AWS connection.
    """

    template_fields: Sequence[str] = (
        "query_or_table",
        "key",
        "bucket_name",
        "query_params",
        "export_params",
    )
    template_fields_renderers = {
        "query_or_table": "sql",
        "query_params": "json",
        "export_params": "json",
    }
    template_ext: Sequence[str] = (".sql",)
    ui_color = "#ededed"

    def __init__(
        self,
        *,
        query_or_table: str,
        key: str,
        bucket_name: str | None = None,
        replace: bool = False,
        encrypt: bool = False,
        gzip: bool = False,
        acl_policy: str | None = None,
        query_params: dict | None = None,
        export_params: dict | None = None,
        exasol_conn_id: str = "exasol_default",
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query_or_table = query_or_table
        self.key = key
        self.bucket_name = bucket_name
        self.replace = replace
        self.encrypt = encrypt
        self.gzip = gzip
        self.acl_policy = acl_policy
        self.query_params = query_params
        self.export_params = export_params
        self.exasol_conn_id = exasol_conn_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Context) -> str:
        self.log.info("Starting Exasol export and S3 upload process.")
        export_operator = ExasolExportOperator(
            query_or_table=self.query_or_table,
            export_params=self.export_params,
            query_params=self.query_params,
            exasol_conn_id=self.exasol_conn_id,
            task_id=f"{self.task_id}_export",
        )
        file_path = export_operator.execute(context)

        upload_operator = S3UploadOperator(
            file_path=file_path,
            key=self.key,
            bucket_name=self.bucket_name,
            replace=self.replace,
            encrypt=self.encrypt,
            gzip=self.gzip,
            acl_policy=self.acl_policy,
            aws_conn_id=self.aws_conn_id,
            task_id=f"{self.task_id}_upload",
        )
        result_key = upload_operator.execute(context)
        self.log.info("ExasolToS3 process completed with S3 key: %s", result_key)
        return result_key