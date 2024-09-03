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
"""This module contains AWS CloudFormation create/delete stack operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.providers.amazon.aws.hooks.cloud_formation import CloudFormationHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CloudFormationCreateStackOperator(AwsBaseOperator[CloudFormationHook]):
    """
    An operator that creates a AWS CloudFormation stack.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudFormationCreateStackOperator`

    :param stack_name: stack name (templated)
    :param cloudformation_parameters: parameters to be passed to AWS CloudFormation.
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

    aws_hook_class = CloudFormationHook
    template_fields: Sequence[str] = aws_template_fields("stack_name", "cloudformation_parameters")
    ui_color = "#6b9659"

    def __init__(self, *, stack_name: str, cloudformation_parameters: dict, **kwargs):
        super().__init__(**kwargs)
        self.stack_name = stack_name
        self.cloudformation_parameters = cloudformation_parameters

    def execute(self, context: Context):
        self.log.info("CloudFormation parameters: %s", self.cloudformation_parameters)
        self.hook.create_stack(self.stack_name, self.cloudformation_parameters)


class CloudFormationDeleteStackOperator(AwsBaseOperator[CloudFormationHook]):
    """
    An operator that deletes a AWS CloudFormation stack.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudFormationDeleteStackOperator`

    :param stack_name: stack name (templated)
    :param cloudformation_parameters: parameters to be passed to CloudFormation.
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

    aws_hook_class = CloudFormationHook
    template_fields: Sequence[str] = aws_template_fields("stack_name")
    ui_color = "#1d472b"
    ui_fgcolor = "#FFF"

    def __init__(
        self,
        *,
        stack_name: str,
        cloudformation_parameters: dict | None = None,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cloudformation_parameters = cloudformation_parameters or {}
        self.stack_name = stack_name
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Context):
        self.log.info("CloudFormation Parameters: %s", self.cloudformation_parameters)
        self.hook.delete_stack(self.stack_name, self.cloudformation_parameters)
