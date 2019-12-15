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
This module contains CloudFormationCreateStackOperator.
"""
import inspect
from typing import List

from airflow.contrib.hooks.aws_cloudformation_hook import CloudFormationHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CloudFormationCreateStackOperator(BaseOperator):
    """
    An operator that creates a CloudFormation stack.

    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    :param kwargs: Additional arguments to be passed to CloudFormation. For possible arguments see:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudformation.html#CloudFormation.Client.create_stack
    """
    template_fields: List[str] = []
    template_ext = ()
    ui_color = '#6b9659'

    @apply_defaults
    def __init__(
            self,
            aws_conn_id='aws_default',
            *args, **kwargs):
        # pylint: disable=no-member
        self.operator_arguments = inspect.getfullargspec(super().__init__.__wrapped__)[0]
        # pylint: enable=no-member
        super_kwargs = {k: v for (k, v) in kwargs.items() if k in self.operator_arguments}
        super().__init__(*args, **super_kwargs)
        self.aws_conn_id = aws_conn_id
        self.kwargs = kwargs

    def execute(self, context):
        args = {k: v for k, v in self.kwargs.items() if k not in self.operator_arguments}

        self.log.info('Creating CloudFormation stack: %s', args)

        cloudformation = CloudFormationHook(aws_conn_id=self.aws_conn_id).get_conn()

        cloudformation.create_stack(**args)

        waiter = cloudformation.get_waiter('stack_create_complete')
        waiter.wait(StackName=args['StackName'])
