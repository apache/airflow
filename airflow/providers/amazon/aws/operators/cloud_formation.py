# -*- coding: utf-8 -*-
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
"""
This module contains CloudFormation create/delete stack operators.
"""
from typing import List

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.cloud_formation import AWSCloudFormationHook
from airflow.utils.decorators import apply_defaults


class BaseCloudFormationOperator(BaseOperator):
    """
    Base operator for CloudFormation operations.

    :param params: parameters to be passed to CloudFormation.
    :type params: dict
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    """
    template_fields: List[str] = []
    template_ext = ()
    ui_color = '#1d472b'
    ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(
            self,
            params,
            aws_conn_id='aws_default',
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params = params
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        self.log.info('Parameters: %s', self.params)

        self.cloudformation_op(AWSCloudFormationHook(aws_conn_id=self.aws_conn_id).get_conn())

    def cloudformation_op(self, cloudformation):
        """
        This is the main method to run CloudFormation operation.
        """
        raise NotImplementedError()


class CloudFormationCreateStackOperator(BaseCloudFormationOperator):
    """
    An operator that creates a CloudFormation stack.

    :param params: parameters to be passed to CloudFormation. For possible arguments see:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudformation.html#CloudFormation.Client.create_stack
    :type params: dict
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    """
    template_fields: List[str] = []
    template_ext = ()
    ui_color = '#6b9659'

    @apply_defaults
    def __init__(
            self,
            params,
            aws_conn_id='aws_default',
            *args, **kwargs):
        super().__init__(params=params, aws_conn_id=aws_conn_id, *args, **kwargs)

    def cloudformation_op(self, cloudformation):
        cloudformation.create_stack(**self.params)


class CloudFormationDeleteStackOperator(BaseCloudFormationOperator):
    """
    An operator that deletes a CloudFormation stack.

    :param params: parameters to be passed to CloudFormation. For possible arguments see:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudformation.html#CloudFormation.Client.delete_stack
    :type params: dict
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    """
    template_fields: List[str] = []
    template_ext = ()
    ui_color = '#1d472b'
    ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(
            self,
            params,
            aws_conn_id='aws_default',
            *args, **kwargs):
        super().__init__(params=params, aws_conn_id=aws_conn_id, *args, **kwargs)

    def cloudformation_op(self, cloudformation):
        cloudformation.delete_stack(**self.params)
