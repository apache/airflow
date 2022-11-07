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
"""This module contains CloudFormation create/delete stack operators."""
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.cloud_formation import CloudFormationHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CloudFormationCreateStackOperator(BaseOperator):
    """
    An operator that creates a CloudFormation stack.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudFormationCreateStackOperator`


    :param stack_name: stack name (templated)
    :param cloudformation_parameters: parameters to be passed to CloudFormation.
    :param aws_conn_id: aws connection to uses
    """

    template_fields: Sequence[str] = ("stack_name", "cloudformation_parameters")
    template_ext: Sequence[str] = ()
    ui_color = "#6b9659"

    def __init__(
        self, *, stack_name: str, cloudformation_parameters: dict, aws_conn_id: str = "aws_default", **kwargs
    ):
        super().__init__(**kwargs)
        self.stack_name = stack_name
        self.cloudformation_parameters = cloudformation_parameters
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Context):
        self.log.info("CloudFormation parameters: %s", self.cloudformation_parameters)

        cloudformation_hook = CloudFormationHook(aws_conn_id=self.aws_conn_id)
        cloudformation_hook.create_stack(self.stack_name, self.cloudformation_parameters)


class CloudFormationDeleteStackOperator(BaseOperator):
    """
    An operator that deletes a CloudFormation stack.

    :param stack_name: stack name (templated)
    :param cloudformation_parameters: parameters to be passed to CloudFormation.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudFormationDeleteStackOperator`

    :param aws_conn_id: aws connection to uses
    """

    template_fields: Sequence[str] = ("stack_name",)
    template_ext: Sequence[str] = ()
    ui_color = "#1d472b"
    ui_fgcolor = "#FFF"

    def __init__(
        self,
        *,
        stack_name: str,
        cloudformation_parameters: dict | None = None,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cloudformation_parameters = cloudformation_parameters or {}
        self.stack_name = stack_name
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Context):
        self.log.info("CloudFormation Parameters: %s", self.cloudformation_parameters)

        cloudformation_hook = CloudFormationHook(aws_conn_id=self.aws_conn_id)
        cloudformation_hook.delete_stack(self.stack_name, self.cloudformation_parameters)
