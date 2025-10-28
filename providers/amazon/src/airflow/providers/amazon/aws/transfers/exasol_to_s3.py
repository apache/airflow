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

from collections.abc import Sequence
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.exasol.hooks.exasol import ExasolHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ExasolToS3Operator(BaseOperator):
    """
    Export data from Exasol database to AWS S3 bucket.

    :param query_or_table: the sql statement to be executed or table name to export
    :param key: S3 key that will point to the file
    :param bucket_name: Name of the bucket in which to store the file
    :param replace: A flag to decide whether or not to overwrite the key
        if it already exists. If replace is False and the key exists, an
        error will be raised.
    :param encrypt: If True, the file will be encrypted on the server-side
        by S3 and will be stored in an encrypted form while at rest in S3.
    :param gzip: If True, the file will be compressed locally
    :param acl_policy: String specifying the canned ACL policy for the file being
        uploaded to the S3 bucket.
    :param query_params: Query parameters passed to underlying ``export_to_file``
        method of :class:`~pyexasol.connection.ExaConnection`.
    :param export_params: Extra parameters passed to underlying ``export_to_file``
        method of :class:`~pyexasol.connection.ExaConnection`.
    """

    template_fields: Sequence[str] = ("query_or_table", "key", "bucket_name", "query_params", "export_params")
    template_fields_renderers = {"query_or_table": "sql", "query_params": "json", "export_params": "json"}
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

    def execute(self, context: Context):
        exasol_hook = ExasolHook(exasol_conn_id=self.exasol_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        with NamedTemporaryFile("w+") as file:
            exasol_hook.export_to_file(
                filename=file.name,
                query_or_table=self.query_or_table,
                export_params=self.export_params,
                query_params=self.query_params,
            )
            file.flush()
            self.log.info("Uploading the data as %s", self.key)
            s3_hook.load_file(
                filename=file.name,
                key=self.key,
                bucket_name=self.bucket_name,
                replace=self.replace,
                encrypt=self.encrypt,
                gzip=self.gzip,
                acl_policy=self.acl_policy,
            )
        self.log.info("Data uploaded")
        return self.key
