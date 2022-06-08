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

import sys
import warnings
from typing import Generic, Optional, Set, Type, TypeVar

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property

from botocore.config import Config

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

AwsHookClass = TypeVar("AwsHookClass", bound=AwsBaseHook)


class AwsBaseOperator(BaseOperator, Generic[AwsHookClass]):
    """Base implementations for amazon-provider operators.

    :param aws_conn_id: aws connection to use
    :param region_name: (optional) region name to use in AWS Hook.
        Override the region_name in connection (if provided)
    :param botocore_config: Configuration for botocore client.
        (https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html)
    """

    aws_hook_class: Type[AwsHookClass]
    aws_hook_class_kwargs: Optional[Set[str]] = None

    def __init__(
        self,
        *,
        aws_conn_id: Optional[str] = "aws_default",
        region_name: Optional[str] = None,
        botocore_config: Optional[Config] = None,
        **kwargs,
    ) -> None:
        self.aws_conn_id = aws_conn_id

        region = kwargs.pop("region", None)
        if region:
            warnings.warn(
                'Parameter `region` is deprecated. Please use `region_name` instead.',
                DeprecationWarning,
                stacklevel=2,
            )
            if region_name:
                raise AirflowException("Either `region_name` or `region` can be provided, not both.")
            region_name = region

        self.region_name = region_name
        self.botocore_config = botocore_config

        self.hooks_class_args = {}

        # Check if Hook uses non `aws_conn_id` for connection
        conn_name_attr = self.aws_hook_class.conn_name_attr
        if conn_name_attr != "aws_conn_id":
            self.hooks_class_args[conn_name_attr] = kwargs.pop(
                conn_name_attr, self.aws_hook_class.default_conn_name
            )

        # Add additional hook args and remove from keywords arguments
        for arg in self.aws_hook_class_kwargs or {}:
            if arg in kwargs:
                self.hooks_class_args[arg] = kwargs.pop(arg, None)

        super().__init__(**kwargs)

    @cached_property
    def hook(self) -> AwsHookClass:
        """Create and return an AWS Hook."""
        return self.aws_hook_class(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            config=self.botocore_config,
            **self.hooks_class_args,
        )
