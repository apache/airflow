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
"""Amazon S3 Tables operators."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    from airflow.sdk import Context


class S3TablesCreateTableOperator(AwsBaseOperator[AwsBaseHook]):
    """
    Create a new table in an Amazon S3 Tables namespace.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3TablesCreateTableOperator`

    :param table_bucket_arn: The ARN of the table bucket to create the table in. (templated)
    :param namespace: The namespace to associate with the table. (templated)
    :param table_name: The name of the table. (templated)
    :param format: The table format. (templated) Currently only ``ICEBERG`` is supported.
    :param metadata: Optional Iceberg schema metadata. (templated)
        Example: ``{"iceberg": {"schema": {"fields": [{"name": "id", "type": "int", "required": True}]}}}``
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    template_fields: Sequence[str] = aws_template_fields(
        "table_bucket_arn", "namespace", "table_name", "format", "metadata"
    )
    template_fields_renderers = {"metadata": "json"}
    aws_hook_class = AwsBaseHook

    def __init__(
        self,
        *,
        table_bucket_arn: str,
        namespace: str,
        table_name: str,
        format: str = "ICEBERG",
        metadata: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_bucket_arn = table_bucket_arn
        self.namespace = namespace
        self.table_name = table_name
        self.format = format
        self.metadata = metadata

    @property
    def _hook_parameters(self):
        return {**super()._hook_parameters, "client_type": "s3tables"}

    def execute(self, context: Context) -> str:
        self.log.info(
            "Creating S3 table %s in namespace %s (bucket %s)",
            self.table_name,
            self.namespace,
            self.table_bucket_arn,
        )
        kwargs: dict[str, Any] = {
            "tableBucketARN": self.table_bucket_arn,
            "namespace": self.namespace,
            "name": self.table_name,
            "format": self.format,
        }
        if self.metadata:
            kwargs["metadata"] = self.metadata
        response = self.hook.conn.create_table(**kwargs)
        table_arn = response["tableARN"]
        self.log.info("Created table: %s", table_arn)
        return table_arn


class S3TablesDeleteTableOperator(AwsBaseOperator[AwsBaseHook]):
    """
    Delete a table from an Amazon S3 Tables namespace.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3TablesDeleteTableOperator`

    :param table_bucket_arn: The ARN of the table bucket containing the table. (templated)
    :param namespace: The namespace of the table. (templated)
    :param table_name: The name of the table to delete. (templated)
    :param version_token: Optional version token for optimistic concurrency. (templated)
    """

    template_fields: Sequence[str] = aws_template_fields(
        "table_bucket_arn", "namespace", "table_name", "version_token"
    )
    aws_hook_class = AwsBaseHook

    def __init__(
        self,
        *,
        table_bucket_arn: str,
        namespace: str,
        table_name: str,
        version_token: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_bucket_arn = table_bucket_arn
        self.namespace = namespace
        self.table_name = table_name
        self.version_token = version_token

    @property
    def _hook_parameters(self):
        return {**super()._hook_parameters, "client_type": "s3tables"}

    def execute(self, context: Context) -> None:
        self.log.info(
            "Deleting S3 table %s from namespace %s (bucket %s)",
            self.table_name,
            self.namespace,
            self.table_bucket_arn,
        )
        kwargs: dict[str, Any] = prune_dict(
            {
                "tableBucketARN": self.table_bucket_arn,
                "namespace": self.namespace,
                "name": self.table_name,
                "versionToken": self.version_token,
            }
        )
        self.hook.conn.delete_table(**kwargs)
        self.log.info("Deleted table %s", self.table_name)
