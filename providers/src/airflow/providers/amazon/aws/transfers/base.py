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
"""This module contains base AWS to AWS transfer operator."""

from __future__ import annotations

from typing import Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.types import NOTSET, ArgNotSet


class AwsToAwsBaseOperator(BaseOperator):
    """
    Base class for AWS to AWS transfer operators.

    :param source_aws_conn_id: The Airflow connection used for AWS credentials
        to access DynamoDB. If this is None or empty then the default boto3
        behaviour is used. If running Airflow in a distributed manner and
        source_aws_conn_id is None or empty, then default boto3 configuration
        would be used (and must be maintained on each worker node).
    :param dest_aws_conn_id: The Airflow connection used for AWS credentials
        to access S3. If this is not set then the source_aws_conn_id connection is used.
    """

    template_fields: Sequence[str] = (
        "source_aws_conn_id",
        "dest_aws_conn_id",
    )

    def __init__(
        self,
        *,
        source_aws_conn_id: str | None = AwsBaseHook.default_conn_name,
        dest_aws_conn_id: str | None | ArgNotSet = NOTSET,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_aws_conn_id = source_aws_conn_id
        self.dest_aws_conn_id = dest_aws_conn_id
        self.source_aws_conn_id = source_aws_conn_id
        if isinstance(dest_aws_conn_id, ArgNotSet):
            self.dest_aws_conn_id = self.source_aws_conn_id
        else:
            self.dest_aws_conn_id = dest_aws_conn_id
