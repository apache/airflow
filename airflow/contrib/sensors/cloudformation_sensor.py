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
This module contains sensors for AWS CloudFormation.
"""
from botocore.exceptions import ClientError

from airflow.contrib.hooks.aws_cloudformation_hook import CloudFormationHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class BaseCloudFormationSensor(BaseSensorOperator):
    """
    Waits for a stack operation to complete on AWS CloudFormation.

    :param stack_name: The name of the stack to wait for (templated)
    :type stack_name: str
    :param aws_conn_id: ID of the Airflow connection where credentials and extra configuration are
        stored
    :type aws_conn_id: str
    :param poke_interval: Time in seconds that the job should wait between each try
    :type poke_interval: int
    """

    @apply_defaults
    def __init__(self,
                 stack_name,
                 complete_status,
                 in_progress_status,
                 aws_conn_id='aws_default',
                 poke_interval=60 * 3,
                 *args,
                 **kwargs):
        super().__init__(poke_interval=poke_interval, *args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.stack_name = stack_name
        self.complete_status = complete_status
        self.in_progress_status = in_progress_status
        self.hook = None

    def poke(self, context):
        """
        Checks for existence of the stack in AWS CloudFormation.
        """
        cloudformation = self.get_hook().get_conn()

        self.log.info('Poking for stack %s', self.stack_name)

        try:
            stacks = cloudformation.describe_stacks(StackName=self.stack_name)['Stacks']
            stack_status = stacks[0]['StackStatus']
            if stack_status == self.complete_status:
                return True
            elif stack_status == self.in_progress_status:
                return False
            else:
                raise ValueError(f'Stack {self.stack_name} in bad state: {stack_status}')
        except ClientError as e:
            if 'does not exist' in str(e):
                if not self.allow_non_existing_stack_status():
                    raise ValueError(f'Stack {self.stack_name} does not exist')
                else:
                    return True
            else:
                raise e

    def get_hook(self):
        """
        Gets the AwsGlueCatalogHook
        """
        if not self.hook:
            self.hook = CloudFormationHook(aws_conn_id=self.aws_conn_id)

        return self.hook

    def allow_non_existing_stack_status(self):
        """
        Boolean value whether or not sensor should allow non existing stack responses.
        """
        return False


class CloudFormationCreateStackSensor(BaseCloudFormationSensor):
    """
    Waits for a stack to be created successfully on AWS CloudFormation.

    :param stack_name: The name of the stack to wait for (templated)
    :type stack_name: str
    :param aws_conn_id: ID of the Airflow connection where credentials and extra configuration are
        stored
    :type aws_conn_id: str
    :param poke_interval: Time in seconds that the job should wait between each try
    :type poke_interval: int
    """

    template_fields = ['stack_name']
    ui_color = '#C5CAE9'

    @apply_defaults
    def __init__(self,
                 stack_name,
                 aws_conn_id='aws_default',
                 poke_interval=60 * 3,
                 *args,
                 **kwargs):
        super().__init__(stack_name=stack_name,
                         complete_status='CREATE_COMPLETE',
                         in_progress_status='CREATE_IN_PROGRESS',
                         aws_conn_id=aws_conn_id,
                         poke_interval=poke_interval,
                         *args,
                         **kwargs)


class CloudFormationDeleteStackSensor(BaseCloudFormationSensor):
    """
    Waits for a stack to be deleted successfully on AWS CloudFormation.

    :param stack_name: The name of the stack to wait for (templated)
    :type stack_name: str
    :param aws_conn_id: ID of the Airflow connection where credentials and extra configuration are
        stored
    :type aws_conn_id: str
    :param poke_interval: Time in seconds that the job should wait between each try
    :type poke_interval: int
    """

    template_fields = ['stack_name']
    ui_color = '#C5CAE9'

    @apply_defaults
    def __init__(self,
                 stack_name,
                 aws_conn_id='aws_default',
                 poke_interval=60 * 3,
                 *args,
                 **kwargs):
        super().__init__(stack_name=stack_name,
                         complete_status='DELETE_COMPLETE',
                         in_progress_status='DELETE_IN_PROGRESS',
                         aws_conn_id=aws_conn_id,
                         poke_interval=poke_interval, *args, **kwargs)

    def allow_non_existing_stack_status(self):
        return True
