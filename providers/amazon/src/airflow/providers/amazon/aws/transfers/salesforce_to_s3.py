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
from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.salesforce.hooks.salesforce import SalesforceHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SalesforceToS3Operator(BaseOperator):
    """
    Submits a Salesforce query and uploads the results to AWS S3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SalesforceToS3Operator`

    :param salesforce_query: The query to send to Salesforce.
    :param s3_bucket_name: The bucket name to upload to.
    :param s3_key: The object name to set when uploading the file.
    :param salesforce_conn_id: The name of the connection that has the parameters needed
        to connect to Salesforce.
    :param export_format: Desired format of files to be exported.
    :param query_params: Additional optional arguments to be passed to the HTTP request querying Salesforce.
    :param include_deleted: True if the query should include deleted records.
    :param coerce_to_timestamp: True if you want all datetime fields to be converted into Unix timestamps.
        False if you want them to be left in the same format as they were in Salesforce.
        Leaving the value as False will result in datetimes being strings. Default: False
    :param record_time_added: True if you want to add a Unix timestamp field
        to the resulting data that marks when the data was fetched from Salesforce. Default: False
    :param aws_conn_id: The name of the connection that has the parameters we need to connect to S3.
    :param replace: A flag to decide whether or not to overwrite the S3 key if it already exists. If set to
        False and the key exists an error will be raised.
    :param encrypt: If True, the file will be encrypted on the server-side by S3 and will
        be stored in an encrypted form while at rest in S3.
    :param gzip: If True, the file will be compressed locally.
    :param acl_policy: String specifying the canned ACL policy for the file being uploaded
        to the S3 bucket.
    """

    template_fields: Sequence[str] = ("salesforce_query", "s3_bucket_name", "s3_key")
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"salesforce_query": "sql"}

    def __init__(
        self,
        *,
        salesforce_query: str,
        s3_bucket_name: str,
        s3_key: str,
        salesforce_conn_id: str,
        export_format: str = "csv",
        query_params: dict | None = None,
        include_deleted: bool = False,
        coerce_to_timestamp: bool = False,
        record_time_added: bool = False,
        aws_conn_id: str | None = "aws_default",
        replace: bool = False,
        encrypt: bool = False,
        gzip: bool = False,
        acl_policy: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.salesforce_query = salesforce_query
        self.s3_bucket_name = s3_bucket_name
        self.s3_key = s3_key
        self.salesforce_conn_id = salesforce_conn_id
        self.export_format = export_format
        self.query_params = query_params
        self.include_deleted = include_deleted
        self.coerce_to_timestamp = coerce_to_timestamp
        self.record_time_added = record_time_added
        self.aws_conn_id = aws_conn_id
        self.replace = replace
        self.encrypt = encrypt
        self.gzip = gzip
        self.acl_policy = acl_policy

    def execute(self, context: Context) -> str:
        salesforce_hook = SalesforceHook(salesforce_conn_id=self.salesforce_conn_id)
        response = salesforce_hook.make_query(
            query=self.salesforce_query,
            include_deleted=self.include_deleted,
            query_params=self.query_params,
        )

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "salesforce_temp_file")
            salesforce_hook.write_object_to_file(
                query_results=response["records"],
                filename=path,
                fmt=self.export_format,
                coerce_to_timestamp=self.coerce_to_timestamp,
                record_time_added=self.record_time_added,
            )

            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
            s3_hook.load_file(
                filename=path,
                key=self.s3_key,
                bucket_name=self.s3_bucket_name,
                replace=self.replace,
                encrypt=self.encrypt,
                gzip=self.gzip,
                acl_policy=self.acl_policy,
            )

            s3_uri = f"s3://{self.s3_bucket_name}/{self.s3_key}"
            self.log.info("Salesforce data uploaded to S3 at %s.", s3_uri)

            return s3_uri
