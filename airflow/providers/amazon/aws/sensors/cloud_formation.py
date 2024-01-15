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
"""This module contains sensors for AWS CloudFormation."""
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.cloud_formation import CloudFormationHook


class CloudFormationCreateStackSensor(AwsBaseSensor[CloudFormationHook]):
    """
    Waits for a stack to be created successfully on AWS CloudFormation.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:CloudFormationCreateStackSensor`

    :param stack_name: The name of the stack to wait for (templated)
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
    ui_color = "#C5CAE9"

    def __init__(self, *, stack_name, **kwargs):
        super().__init__(**kwargs)
        self.stack_name = stack_name

    def poke(self, context: Context):
        stack_status = self.hook.get_stack_status(self.stack_name)
        if stack_status == "CREATE_COMPLETE":
            return True
        if stack_status in ("CREATE_IN_PROGRESS", None):
            return False

        # TODO: remove this if check when min_airflow_version is set to higher than 2.7.1
        message = f"Stack {self.stack_name} in bad state: {stack_status}"
        if self.soft_fail:
            raise AirflowSkipException(message)
        raise ValueError(message)


class CloudFormationDeleteStackSensor(AwsBaseSensor[CloudFormationHook]):
    """
    Waits for a stack to be deleted successfully on AWS CloudFormation.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:CloudFormationDeleteStackSensor`

    :param stack_name: The name of the stack to wait for (templated)
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
    ui_color = "#C5CAE9"

    def __init__(
        self,
        *,
        stack_name: str,
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.stack_name = stack_name

    def poke(self, context: Context):
        stack_status = self.hook.get_stack_status(self.stack_name)
        if stack_status in ("DELETE_COMPLETE", None):
            return True
        if stack_status == "DELETE_IN_PROGRESS":
            return False

        # TODO: remove this if check when min_airflow_version is set to higher than 2.7.1
        message = f"Stack {self.stack_name} in bad state: {stack_status}"
        if self.soft_fail:
            raise AirflowSkipException(message)
        raise ValueError(message)
