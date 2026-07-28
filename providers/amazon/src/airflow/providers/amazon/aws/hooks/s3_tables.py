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

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class S3TablesHook(AwsBaseHook):
    """
    Interact with Amazon S3 Tables.

    Provide thin wrapper around
    :external+boto3:py:class:`boto3.client("s3tables") <S3Tables.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "s3tables"
        super().__init__(*args, **kwargs)

    def get_table_bucket_arn_by_name(self, table_bucket_name: str) -> str | None:
        """Get the table bucket ARN by name, or None if not found."""
        paginator = self.conn.get_paginator("list_table_buckets")
        for page in paginator.paginate():
            for tb in page.get("tableBuckets", []):
                if tb["name"] == table_bucket_name:
                    return tb["arn"]
        return None
